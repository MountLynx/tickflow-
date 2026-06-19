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
"""

from __future__ import annotations

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


class DictView:
    """Read-only view of a node's resolved inputs at a given tick, plus an
    optional :class:`_NodeStateView` exposing the node's mutable state.

    Constructed by the engine each tick, per firing node, after resolving
    each declared producer per its :class:`InputPolicy`. Inputs are immutable;
    ``view.state`` is a read/write proxy for the node's own state slot (writes
    land in the next marking). Callables should treat inputs as immutable.
    """

    def __init__(self, inputs: dict[str, Resolved], state: Any = None, node: str = "") -> None:
        # ``inputs`` maps producer name -> Resolved.
        object.__setattr__(self, "_inputs", inputs)
        object.__setattr__(self, "_state", state)
        object.__setattr__(self, "_node", node)

    # -- access ------------------------------------------------------------

    @property
    def state(self) -> Any:
        """This node's mutable state slot. Bodies may read/write it
        (``view.state["attempts"] += 1``); guards receive a read-only view.
        ``None`` if no state was attached (e.g. bare DictView in tests)."""
        return self._state

    @property
    def node(self) -> str:
        """The name of the node this view belongs to."""
        return self._node

    def __getattr__(self, name: str) -> Any:
        inputs = self.__dict__.get("_inputs")
        if inputs is None or name not in inputs:
            raise AttributeError(name)
        return _ResolvedAttr(inputs[name])

    def __getitem__(self, name: str) -> Any:
        if name not in self._inputs:
            raise KeyError(name)
        return _ResolvedAttr(self._inputs[name])

    def inputs(self) -> dict[str, Any]:
        """Flat dict of producer -> resolved value (for bodies that want the
        whole bundle, e.g. the identity body)."""
        return {k: v.value for k, v in self._inputs.items()}

    def items(self):
        return ((k, v.value) for k, v in self._inputs.items())

    def __contains__(self, name: str) -> bool:
        return name in self._inputs

    def __repr__(self) -> str:
        return f"DictView({list(self._inputs)})"
