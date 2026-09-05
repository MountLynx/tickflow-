"""Read-only views handed to body/guard callables.

Two calling conventions exist. *Value mode* (preferred for hand-written
bodies/guards): plain parameters, filled from the node's bind declaration by
the engine — no view involved. *View mode*: the callable takes a single
``v``/``view`` parameter and consumes one of the classes below.

NodeView (view-mode bodies)
---------------------------
Per-fire context for a node. Consume bind-declared inputs; do not read by
name::

    v.input()   -> None | value | tuple  (arity-polymorphic)
    v.args      -> tuple of bound values (always; ``Missing`` preserved)
    v.named     -> {field: value} for named binds ({} for positional)
    v.field(f)  -> named-bind lookup (TypeError on positional binds)
    v.state     -> mutable state proxy (writes persist per firing)
    v.node      -> this node's name

GuardView (view-mode guards)
----------------------------
Adjudication context for the guard on edge ``src--|g|-->dst``. A guard rules
on the output its source node just produced — it is not an input consumer, so
there is deliberately NO ``input()``::

    v.output    -> src's current-tick output (the thing being adjudicated)
    v.args / v.named / v.field(f)
                -> src's bind-declared inputs, resolved with the same
                   policies (and values) the src body just consumed
    v.state     -> src's post-body state, READ-ONLY
    v.node / v.src -> the firing node's name

The ``Missing`` three-state table
---------------------------------
``input()``/``args`` distinguish three situations that a bare ``None`` would
collapse:

- node has NO bound inputs at all  -> ``input()`` returns ``None``
- input declared but producer has not fired yet -> ``Missing`` (falsy)
- producer genuinely fired ``None`` -> ``None``

Deprecated name-based access
----------------------------
Name-based access on a view (``view.A``, ``view["A"]``, ``view.inputs()``,
``view.items()``, ``k in view``) still works but emits
:class:`DeprecationWarning` and is scheduled for removal;
:class:`DictView` survives only as a deprecated construction shim over
:class:`NodeView`.

Resolution semantics
--------------------
- ``latest`` (default): the producer's most recent fire with ``tick < t``.
  This is the marking-consistent read -- a node firing at tick ``t`` cannot
  see another node's same-tick write, only the prior marking's content. Loops
  thus read the previous iteration's output, not their own.
- ``index`` (``A[k]``): the producer's ``k``-th fire overall (1-based),
  independent of tick. Used for cross-iteration pinning and audit replays.
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
    both exist). Missing names raise AttributeError/KeyError without warning.
    Reserved names (``args``/``named``/``field`` and the subclasses' own
    properties) are not dispatched through here — producers with those names
    remain reachable via ``view["name"]``."""

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
        present = name in self.__dict__.get("_resolved", {})
        if present:
            _warn_name_access(self, f"{name!r} in view")
        return present

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

    def __repr__(self) -> str:
        return f"{type(self).__name__}(node={self._node!r}, inputs={list(self._resolved)})"


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

    def __repr__(self) -> str:
        return f"GuardView(src={self._node!r}, inputs={list(self._resolved)})"


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
    keeps legacy construction sites (tests, pre-bind engine hosts) working
    on top of NodeView. Prefer NodeView / value mode."""

    def __init__(self, inputs: dict[str, Resolved] | None, state: Any = None,
                 node: str = "") -> None:
        warnings.warn(
            "DictView is deprecated; construct NodeView or use value-mode bodies",
            DeprecationWarning,
            stacklevel=2,
        )
        resolved = {
            k: (r if isinstance(r, Resolved) else Resolved(r, None))
            for k, r in (inputs or {}).items()
        }
        values = tuple(r.value for r in resolved.values())
        super().__init__(node=node, fields=None, values=values, state=state,
                         resolved=resolved)
        # Compat alias: the pre-shim DictView stored inputs as ``_inputs`` and
        # registry._identity_body still reads it; keep echo semantics intact.
        self._inputs = self._resolved
