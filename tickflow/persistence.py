"""Pluggable persistence backends for snapshots, firing logs, and checkpoints.

The :class:`Backend` protocol is the storage abstraction flow's :class:`Runner`
talks to. A Runner constructed with ``backend=...`` and ``session_id=...``
persists, at the end of every tick:

- each :class:`Firing` (the "process record" -- 全过程记录), and
- a full :meth:`Runner.snapshot` at the new tick index (the "tick-level
  checkpoint" -- 快照粒度一个 tick, per 重构.md).

Checkpoints are *named* snapshots (label -> snapshot) layered on top, backing
:meth:`Runner.checkpoint` / :meth:`Runner.rollback_to`.

Two reference implementations ship:

- :class:`JsonBackend` -- one directory per session, ``tick_N.json`` +
  ``firings.jsonl`` + ``checkpoints.json``. Default; good for small/medium
  graphs and human inspection.
- :class:`SqliteBackend` -- (planned, not in v1) a single DB with
  ``snapshots`` / ``firings`` / ``checkpoints`` tables; better for high
  tick-throughput and concurrent sessions.

A :class:`NullBackend` (in-memory) is provided for tests that want to exercise
the persistence wiring without touching disk.

Design notes
------------
- Backends are **synchronous** from the Runner's view. The AsyncRunner calls
  them directly (off the event loop) -- if a backend does blocking IO, prefer
  ``asyncio.to_thread`` inside an async-flavoured backend wrapper, or accept
  the brief block. JsonBackend writes are small and fast enough to inline.
- Snapshots are full-state (marking + history + tick + status). This is O(graph
  + history) per tick; for very long runs the SqliteBackend's incremental
  approach will be preferable, but v1 favours simplicity and exact restore.
- ``save_firing`` is append-only and never rewritten; it is the audit trail.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Protocol, runtime_checkable


@runtime_checkable
class Backend(Protocol):
    """Storage protocol for Runner persistence.

    All methods are synchronous. ``session_id`` scopes every call: a backend
    may host many concurrent sessions under one storage root.
    """

    def save_snapshot(self, session_id: str, tick: int, snap: dict) -> None:
        """Persist ``snap`` as the state *after* ``tick`` (i.e. ticks
        ``0..tick-1`` have fired; ``tick`` is the next tick to fire).
        Overwrites any prior snapshot at the same tick for this session."""
        ...

    def load_snapshot(self, session_id: str, tick: int) -> dict | None:
        """Load the snapshot saved at ``tick``, or None if absent."""
        ...

    def latest_tick(self, session_id: str) -> int | None:
        """The highest tick for which a snapshot exists, or None if the
        session has no snapshots."""
        ...

    def list_snapshots(self, session_id: str) -> list[int]:
        """All saved tick indices for the session, ascending."""
        ...

    def save_firing(self, session_id: str, firing: Any) -> None:
        """Append a :class:`Firing` (or its JSON form) to the process log.
        Append-only; never rewritten."""
        ...

    def list_firings(self, session_id: str, since_tick: int = 0) -> list[dict]:
        """All firings with ``tick >= since_tick``, in append order."""
        ...

    def save_checkpoint(self, session_id: str, label: str, snap: dict) -> None:
        """Save ``snap`` under a human-readable ``label``. Overwrites a
        checkpoint with the same label."""
        ...

    def list_checkpoints(self, session_id: str) -> list[tuple[str, int]]:
        """ ``(label, tick)`` pairs for all named checkpoints."""
        ...

    def load_checkpoint(self, session_id: str, label: str) -> dict | None:
        """Load the snapshot saved under ``label``, or None."""
        ...


class NullBackend:
    """In-memory backend for tests. Implements the full Backend protocol."""

    def __init__(self) -> None:
        self._snapshots: dict[str, dict[int, dict]] = {}
        self._firings: dict[str, list[dict]] = {}
        self._checkpoints: dict[str, dict[str, dict]] = {}

    def save_snapshot(self, session_id: str, tick: int, snap: dict) -> None:
        self._snapshots.setdefault(session_id, {})[tick] = snap

    def load_snapshot(self, session_id: str, tick: int) -> dict | None:
        return self._snapshots.get(session_id, {}).get(tick)

    def latest_tick(self, session_id: str) -> int | None:
        ticks = self._snapshots.get(session_id, {})
        return max(ticks) if ticks else None

    def list_snapshots(self, session_id: str) -> list[int]:
        return sorted(self._snapshots.get(session_id, {}))

    def save_firing(self, session_id: str, firing: Any) -> None:
        d = firing.to_json() if hasattr(firing, "to_json") else dict(firing)
        self._firings.setdefault(session_id, []).append(d)

    def list_firings(self, session_id: str, since_tick: int = 0) -> list[dict]:
        return [f for f in self._firings.get(session_id, []) if f.get("tick", 0) >= since_tick]

    def save_checkpoint(self, session_id: str, label: str, snap: dict) -> None:
        self._checkpoints.setdefault(session_id, {})[label] = snap

    def list_checkpoints(self, session_id: str) -> list[tuple[str, int]]:
        items = self._checkpoints.get(session_id, {})
        return sorted([(label, s["tick"]) for label, s in items.items()])

    def load_checkpoint(self, session_id: str, label: str) -> dict | None:
        return self._checkpoints.get(session_id, {}).get(label)


class JsonBackend:
    """Filesystem backend: one directory per session.

    Layout (under ``storage_dir/<session_id>/``)::

        tick_<N>.json     # snapshot after tick N (N = next tick to fire)
        firings.jsonl     # append-only process log, one Firing per line
        checkpoints.json  # {label: snapshot_dict}

    Snapshots are full-state and overwrite-in-place. ``firings.jsonl`` is
    append-only and never truncated by the backend (rollback rewinds the
    Runner's in-memory state; the on-disk log is retained as audit history).
    """

    def __init__(self, storage_dir: str | Path) -> None:
        self._root = Path(storage_dir)

    # -- paths -------------------------------------------------------------

    def _session_dir(self, session_id: str) -> Path:
        return self._root / session_id

    def _tick_path(self, session_id: str, tick: int) -> Path:
        return self._session_dir(session_id) / f"tick_{tick}.json"

    def _firings_path(self, session_id: str) -> Path:
        return self._session_dir(session_id) / "firings.jsonl"

    def _checkpoints_path(self, session_id: str) -> Path:
        return self._session_dir(session_id) / "checkpoints.json"

    # -- snapshots ---------------------------------------------------------

    def save_snapshot(self, session_id: str, tick: int, snap: dict) -> None:
        d = self._session_dir(session_id)
        d.mkdir(parents=True, exist_ok=True)
        self._tick_path(session_id, tick).write_text(
            json.dumps(snap, default=_default), encoding="utf-8"
        )

    def load_snapshot(self, session_id: str, tick: int) -> dict | None:
        p = self._tick_path(session_id, tick)
        if not p.exists():
            return None
        return json.loads(p.read_text(encoding="utf-8"))

    def latest_tick(self, session_id: str) -> int | None:
        ticks = self.list_snapshots(session_id)
        return ticks[-1] if ticks else None

    def list_snapshots(self, session_id: str) -> list[int]:
        d = self._session_dir(session_id)
        if not d.exists():
            return []
        out: list[int] = []
        for p in d.glob("tick_*.json"):
            try:
                out.append(int(p.stem.split("_", 1)[1]))
            except (ValueError, IndexError):
                continue
        return sorted(out)

    # -- firings -----------------------------------------------------------

    def save_firing(self, session_id: str, firing: Any) -> None:
        d = self._session_dir(session_id)
        d.mkdir(parents=True, exist_ok=True)
        payload = firing.to_json() if hasattr(firing, "to_json") else dict(firing)
        with self._firings_path(session_id).open("a", encoding="utf-8") as f:
            f.write(json.dumps(payload, default=_default) + "\n")

    def list_firings(self, session_id: str, since_tick: int = 0) -> list[dict]:
        p = self._firings_path(session_id)
        if not p.exists():
            return []
        out: list[dict] = []
        for line in p.read_text(encoding="utf-8").splitlines():
            line = line.strip()
            if not line:
                continue
            try:
                d = json.loads(line)
            except json.JSONDecodeError:
                continue
            if d.get("tick", 0) >= since_tick:
                out.append(d)
        return out

    # -- checkpoints -------------------------------------------------------

    def save_checkpoint(self, session_id: str, label: str, snap: dict) -> None:
        d = self._session_dir(session_id)
        d.mkdir(parents=True, exist_ok=True)
        cp = self._load_checkpoints(session_id)
        cp[label] = snap
        self._checkpoints_path(session_id).write_text(
            json.dumps(cp, default=_default), encoding="utf-8"
        )

    def list_checkpoints(self, session_id: str) -> list[tuple[str, int]]:
        cp = self._load_checkpoints(session_id)
        return sorted([(label, s["tick"]) for label, s in cp.items()])

    def load_checkpoint(self, session_id: str, label: str) -> dict | None:
        return self._load_checkpoints(session_id).get(label)

    def _load_checkpoints(self, session_id: str) -> dict[str, dict]:
        p = self._checkpoints_path(session_id)
        if not p.exists():
            return {}
        try:
            return json.loads(p.read_text(encoding="utf-8"))
        except json.JSONDecodeError:
            return {}


def _default(o: Any) -> Any:
    """json default for objects that aren't natively serialisable. Mirrors
    runner._jsonable's fallback policy (repr) so snapshots stay stable."""
    if hasattr(o, "isoformat"):
        return o.isoformat()
    return repr(o)
