"""Function registry for node bodies and edge guards.

The graph text declares *structure only* (``C.body: compute_c``,
``B--|go_a|-->A``); the actual Python callables live in a :class:`Registry`.
This keeps snapshots structural (JSON-serializable, no code) while letting
users attach behaviour programmatically.

A default module-level :data:`registry` is provided for the common case where
a single registry is convenient (CLI, examples). :class:`Runner` accepts an
explicit registry so multiple graphs with overlapping keys can coexist.

Callables
---------
- ``Body``: ``(DictView) -> Any`` -- receives a read-only view over history
  (``view.A`` / ``view["A"]`` / ``view.A[2]``) and returns the node's output,
  stored in history under ``(node, tick)``.
- ``Guard``: ``(DictView) -> bool`` -- evaluated against the same view; its
  return decides whether the guarded edge produces True into the downstream
  slot this tick. A failing guard writes **False** (explicit clobber, not
  "leave untouched") so a stale True from a prior iteration cannot leak in.

Identity body
-------------
A node with ``body is None`` echoes its single declared input (or ``None`` if
it has no declared inputs) -- handy for pure-routing nodes like ``Merge``.
"""

from __future__ import annotations

from typing import Callable, Any


Body = Callable[[Any], Any]   # Callable[[DictView], Any]
Guard = Callable[[Any], bool]  # Callable[[DictView], bool]


class Registry:
    """A bag of named body/guard callables, looked up by the graph."""

    def __init__(self) -> None:
        self._bodies: dict[str, Body] = {}
        self._guards: dict[str, Guard] = {}

    # -- registration ------------------------------------------------------

    def body(self, name: str, fn: Body | None = None) -> Any:
        """Decorator or direct call: ``r.body("compute_c", fn)`` or
        ``@r.body("compute_c")``."""
        if fn is None:
            def deco(f: Body) -> Body:
                self._bodies[name] = f
                return f
            return deco
        self._bodies[name] = fn
        return fn

    def guard(self, name: str, fn: Guard | None = None) -> Any:
        if fn is None:
            def deco(f: Guard) -> Guard:
                self._guards[name] = f
                return f
            return deco
        self._guards[name] = fn
        return fn

    # -- lookup ------------------------------------------------------------

    def get_body(self, name: str | None) -> Body:
        if name is None:
            return _identity_body
        try:
            return self._bodies[name]
        except KeyError:
            raise KeyError(f"body '{name}' not registered")

    def get_guard(self, name: str) -> Guard:
        try:
            return self._guards[name]
        except KeyError:
            raise KeyError(f"guard '{name}' not registered")

    def has_body(self, name: str | None) -> bool:
        return name is None or name in self._bodies

    def has_guard(self, name: str) -> bool:
        return name in self._guards

    def __contains__(self, name: str) -> bool:
        return name in self._bodies or name in self._guards


def _identity_body(view: Any) -> Any:
    """Default body: echo the first declared input's value, or None."""
    inputs = getattr(view, "_inputs", None)
    if not inputs:
        return None
    first = next(iter(inputs.values()))
    # ``inputs`` values are Resolved wrappers; return the bare value.
    return first.value if hasattr(first, "value") else first


# Module-level default registry for convenience (CLI / examples).
registry = Registry()
