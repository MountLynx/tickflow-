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
Bodies and guards come in two calling conventions, decided once at
registration time (:func:`classify`) and recorded as a :class:`Sig`.

- *Value mode*: plain positional parameters; the engine calls the callable
  with keyword arguments bound per the node's bind declaration, plus an
  optional keyword-only ``state`` tail. A body returns the node's output
  (stored in history under ``(node, tick)``); a guard receives the node's
  output and returns truthiness.
- *View mode*: a single parameter named ``v``/``view`` (or annotated
  ``NodeView``/``DictView``); the callable receives the node's view object
  over history (``view.A`` / ``view["A"]`` / ``view.A[2]``) and, for a guard,
  its return decides whether the guarded edge produces True into the
  downstream slot this tick. A failing guard writes **False** (explicit
  clobber, not "leave untouched") so a stale True from a prior iteration
  cannot leak in.

Identity body
-------------
A node with ``body is None`` echoes its single declared input (or ``None`` if
it has no declared inputs) -- handy for pure-routing nodes like ``Merge``.
"""

from __future__ import annotations

import inspect
from dataclasses import dataclass
from typing import Callable, Any, Literal


Body = Callable[..., Any]    # value mode: plain params; view mode: one NodeView
Guard = Callable[..., Any]   # value mode: (output,); view mode: one GuardView


@dataclass(frozen=True)
class Sig:
    """Registration-time classification of a body/guard callable: how the
    engine should call it."""

    mode: Literal["value", "view"]
    arity: int | None              # value mode: positional parameter count
    param_names: tuple[str, ...]   # value mode: positional parameter names
    wants_state: bool              # value mode: keyword-only ``state`` tail
    is_async: bool


def _annotation_name(ann: Any) -> str | None:
    if ann is inspect.Parameter.empty:
        return None
    if isinstance(ann, str):  # `from __future__ import annotations` stores strings
        return ann.rsplit(".", 1)[-1].strip("'\"")
    return getattr(ann, "__name__", None)


def classify(fn: Callable) -> Sig:
    """A single positional parameter named ``v``/``view`` (or annotated
    ``NodeView``/``DictView``) is view mode; everything else is value mode.

    Non-introspectable callables (some builtins) and ``*args``-accepting
    callables classify as value mode with ``arity=None`` — the engine calls
    them with the bound values, and build-time arity validation skips them."""
    try:
        sig = inspect.signature(fn)
    except ValueError:  # some C builtins expose no signature
        return Sig(mode="value", arity=None, param_names=(), wants_state=False,
                   is_async=inspect.iscoroutinefunction(fn))
    params = list(sig.parameters.values())
    positional = [
        p for p in params
        if p.kind in (inspect.Parameter.POSITIONAL_ONLY,
                      inspect.Parameter.POSITIONAL_OR_KEYWORD)
    ]
    kwonly = [p for p in params if p.kind == inspect.Parameter.KEYWORD_ONLY]
    has_var_positional = any(
        p.kind == inspect.Parameter.VAR_POSITIONAL for p in params
    )
    is_async = inspect.iscoroutinefunction(fn)
    if (
        len(positional) == 1 and not kwonly
        and positional[0].kind == inspect.Parameter.POSITIONAL_OR_KEYWORD
        and (positional[0].name in ("v", "view")
             or _annotation_name(positional[0].annotation) in ("NodeView", "DictView"))
    ):
        return Sig(mode="view", arity=None, param_names=(), wants_state=False,
                   is_async=is_async)
    return Sig(
        mode="value",
        arity=None if has_var_positional else len(positional),
        param_names=tuple(p.name for p in positional),
        wants_state=any(p.name == "state" for p in kwonly),
        is_async=is_async,
    )


class Registry:
    """A bag of named body/guard callables, looked up by the graph."""

    def __init__(self) -> None:
        self._bodies: dict[str, Body] = {}
        self._guards: dict[str, Guard] = {}
        self._body_sigs: dict[str, Sig] = {}
        self._guard_sigs: dict[str, Sig] = {}

    # -- registration ------------------------------------------------------

    def body(self, name: str, fn: Body | None = None) -> Any:
        """Decorator or direct call: ``r.body("compute_c", fn)`` or
        ``@r.body("compute_c")``."""
        if fn is None:
            def deco(f: Body) -> Body:
                self._bodies[name] = f
                self._body_sigs[name] = classify(f)
                return f
            return deco
        self._bodies[name] = fn
        self._body_sigs[name] = classify(fn)
        return fn

    def guard(self, name: str, fn: Guard | None = None) -> Any:
        if fn is None:
            def deco(f: Guard) -> Guard:
                self._guards[name] = f
                self._guard_sigs[name] = classify(f)
                return f
            return deco
        self._guards[name] = fn
        self._guard_sigs[name] = classify(fn)
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

    def body_sig(self, name: str) -> Sig:
        try:
            return self._body_sigs[name]
        except KeyError:
            raise KeyError(f"body '{name}' not registered")

    def guard_sig(self, name: str) -> Sig:
        try:
            return self._guard_sigs[name]
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
