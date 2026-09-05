"""Read-only views over history for body/guard callables.

A body or guard receives a :class:`DictView` that exposes, for the *current*
node, each declared producer's output as resolved by that node's
:class:`InputPolicy`. Access patterns::

    view.A            # producer A's resolved value (latest_before or A[k])
    view["A"]         # same, dict-style
    view.A.value      # the resolved value
    view.A.k          # the k used, or None if latest
    view.inputs()     # dict[str, value] of all resolved inputs

Resolution semantics
--------------------
- ``latest`` (default): the producer's most recent fire with ``tick < t``.
  This is the marking-consistent read -- a node firing at tick ``t`` cannot
  see another node's same-tick write, only the prior marking's content. Loops
  thus read the previous iteration's output, not their own.
- ``index`` (``A[k]``): the producer's ``k``-th fire overall (1-based),
  independent of tick. Used for cross-iteration pinning and audit replays.

A producer that has no qualifying fire yet yields a sentinel
:class:`Missing`; bodies are expected to handle it (e.g. start nodes whose
inputs haven't fired). The view does not raise so that a guard may simply
return False on missing data.

Bind-declared consumption
-------------------------
The modern per-fire context is :class:`NodeView` (bodies) and
:class:`GuardView` (guards): consume bind-declared inputs via
``.input()``/``.args``/``.named``/``.field()`` instead of by-name access.
Name-based access (``view.A``, ``view["A"]``, ``inputs()``, ...) is
deprecated and emits :class:`DeprecationWarning`; :class:`DictView` survives
only as a deprecated construction shim over :class:`NodeView`.
"""

from __future__ import annotations

import warnings
from dataclasses import dataclass
from typing import Any


class _MissingType:
    """Sentinel for "no fire satisfies the policy yet"."""

    _instance = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    def __bool__(self) -> bool:  # falsy: guards can ``if view.A`` naturally
        return False

    def __repr__(self) -> str:
        return "Missing"


Missing = _MissingType()


@dataclass
class Resolved:
    """One resolved input binding."""

    value: Any
    k: int | None  # the 1-based fire index used, or None for latest_before


class _ResolvedAttr:
    """Wrapper exposed via attribute access (``view.A``). Returns ``value``
    on plain read, has ``.value`` / ``.k`` for inspection."""

    __slots__ = ("_resolved",)

    def __init__(self, resolved: Resolved) -> None:
        object.__setattr__(self, "_resolved", resolved)

    @property
    def value(self) -> Any:
        return self._resolved.value

    @property
    def k(self) -> int | None:
        return self._resolved.k

    def __bool__(self) -> bool:
        v = self._resolved.value
        return bool(v) if v is not Missing else False

    def __repr__(self) -> str:
        return f"Resolved(value={self.value!r}, k={self._resolved.k!r})"


def _warn_name_access(view_obj: Any, what: str) -> None:
    warnings.warn(
        f"name-based input access {what!r} on {type(view_obj).__name__} for node "
        f"{view_obj.__dict__.get('_node', '')!r} is deprecated; consume bind-declared "
        "inputs via .input()/.args/.named/.field() (or switch the callable to value mode)",
        DeprecationWarning,
        stacklevel=3,
    )


class _BindAccess:
    """Shared positional/named consumption over ``_fields``/``_values``."""

    @property
    def args(self) -> tuple:
        """The bound input values, always a tuple. Elements are the resolved
        values: ``Missing`` is preserved (never collapsed to None)."""
        return self.__dict__["_values"]

    @property
    def named(self) -> dict[str, Any]:
        """{field: value} for named binds ({} for positional binds)."""
        pairs = self.__dict__.get("_fields") or ()
        return {f: v for (f, _), v in zip(pairs, self.__dict__["_values"])
                if f is not None}

    def field(self, name: str) -> Any:
        """Named-bind lookup by field name."""
        pairs = self.__dict__.get("_fields") or ()
        if not any(f is not None for f, _ in pairs):
            raise TypeError(
                f"field({name!r}) requires a named bind; this bind is positional"
            )
        for (f, _), v in zip(pairs, self.__dict__["_values"]):
            if f == name:
                return v
        raise KeyError(name)


class _LegacyNameAccess:
    """Deprecated by-name input access shared by NodeView and GuardView.
    Key space: field names plus every resolved producer key (fields win when
    both exist). Missing names raise AttributeError/KeyError without warning."""

    def __getattr__(self, name: str) -> Any:
        if name.startswith("_"):
            raise AttributeError(name)
        r = self.__dict__.get("_resolved", {}).get(name)
        if r is None:
            raise AttributeError(name)
        _warn_name_access(self, f".{name}")
        return _ResolvedAttr(r)

    def __getitem__(self, name: str) -> Any:
        r = self.__dict__.get("_resolved", {}).get(name)
        if r is None:
            raise KeyError(name)
        _warn_name_access(self, f"[{name!r}]")
        return _ResolvedAttr(r)

    def __contains__(self, name: str) -> bool:
        _warn_name_access(self, f"{name!r} in view")
        return name in self.__dict__.get("_resolved", {})

    def inputs(self) -> dict[str, Any]:
        _warn_name_access(self, ".inputs()")
        return {k: v.value for k, v in self.__dict__.get("_resolved", {}).items()}

    def items(self):
        _warn_name_access(self, ".items()")
        return ((k, v.value) for k, v in self.__dict__.get("_resolved", {}).items())


class NodeView(_BindAccess, _LegacyNameAccess):
    """Per-fire context handed to view-mode bodies (successor of DictView).

    Prefer value mode (plain positional/keyword parameters) for hand-written
    bodies; view mode exists for engine hosts that need ``state``/``node``
    alongside the resolved inputs.

    Consumption:
      v.input()   -> None | value | tuple  (arity-polymorphic; elements
                     preserve ``Missing`` — None is returned only when the
                     node has NO bound inputs at all, so "no inputs",
                     "not fired yet (Missing)" and "fired, output None"
                     are three distinguishable situations)
      v.args      -> tuple (always; Missing preserved)
      v.named     -> {field: value} for named binds ({} positional)
      v.field(f)  -> named lookup (TypeError on positional binds)
      v.state     -> mutable state proxy (unchanged semantics)
      v.node      -> this node's name
    """

    def __init__(self, *, node: str = "", fields=None, values: tuple = (),
                 state: Any = None, resolved: dict[str, Resolved] | None = None) -> None:
        self._node = node
        self._fields = fields  # tuple[(field|None, producer), ...] | None
        self._values = tuple(values)
        self._state = state
        self._resolved = dict(resolved) if resolved else {}

    @property
    def node(self) -> str:
        return self._node

    @property
    def state(self) -> Any:
        return self._state

    def input(self) -> Any:
        v = self._values
        if len(v) == 0:
            return None
        if len(v) == 1:
            return v[0]
        return v


class GuardView(_BindAccess, _LegacyNameAccess):
    """Adjudication context for the guard on edge ``src--|g|-->dst``.

    A guard is not an input consumer — it rules on the output its source node
    just produced this tick. Deliberately NO ``input()``.

    - ``output``: src's current-tick output (the thing being adjudicated).
    - ``args``/``named``/``field()``: src's bind-declared inputs, resolved
      with the same policies (and values) the src body just consumed.
    - ``state``: src's post-body state, read-only.
    - ``node``/``src``: the firing node's name.

    Legacy name access: the src name maps to the adjudicated output, then
    field/producer names map to declared inputs. Anything else raises
    KeyError — guards cannot read undeclared nodes.
    """

    def __init__(self, *, src: str, output: Any, fields=None, values: tuple = (),
                 state: Any = None, resolved: dict[str, Resolved] | None = None) -> None:
        self._node = src
        self._output = output
        self._fields = fields
        self._values = tuple(values)
        self._state = state
        self._resolved = dict(resolved) if resolved else {}

    @property
    def src(self) -> str:
        return self._node

    @property
    def node(self) -> str:
        return self._node

    @property
    def output(self) -> Any:
        return self._output

    @property
    def state(self) -> Any:
        return self._state


class _ReadOnlyStateView:
    """Read-only state proxy handed to guards (they adjudicate; they don't write)."""

    def __init__(self, d: dict[str, Any]) -> None:
        object.__setattr__(self, "_d", d)

    def __setattr__(self, k, v):
        raise TypeError("guard state is read-only")

    def __getitem__(self, k: str) -> Any:
        return self._d[k]

    def __setitem__(self, k: str, v: Any) -> None:
        raise TypeError("guard state is read-only")

    def __contains__(self, k: str) -> bool:
        return k in self._d

    def get(self, k: str, default: Any = None) -> Any:
        return self._d.get(k, default)

    def keys(self):
        return self._d.keys()

    def items(self):
        return self._d.items()

    def values(self):
        return self._d.values()

    def __repr__(self) -> str:
        return f"ReadOnlyState({self._d!r})"


class DictView(NodeView):
    """Deprecated legacy constructor shim: ``DictView(inputs, state, node)``
    keeps existing construction sites (tests, SpecModule's translator) working
    on top of NodeView. Prefer NodeView / value mode."""

    def __init__(self, inputs: dict[str, Resolved] | None, state: Any = None,
                 node: str = "") -> None:
        warnings.warn(
            "DictView is deprecated; construct NodeView or use value-mode bodies",
            DeprecationWarning,
            stacklevel=2,
        )
        resolved = dict(inputs) if inputs else {}
        values = tuple(
            r.value if isinstance(r, Resolved) else r for r in resolved.values()
        )
        super().__init__(node=node, fields=None, values=values, state=state,
                         resolved=resolved)
        # Compat alias: the pre-shim DictView stored inputs as ``_inputs`` and
        # registry._identity_body still reads it; keep echo semantics intact.
        self._inputs = self._resolved
