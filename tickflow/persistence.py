"""Pluggable persistence backends for snapshots, firing logs, and checkpoints.

The :class:`Backend` protocol is the storage abstraction flow's :class:`Runner`
talks to. A Runner constructed with ``backend=...`` and ``session_id=...``
persists, at the end of every tick:

- each :class:`~tickflow.state.NodeState` (the "process record" -- 全过程记录), and
- a full :meth:`Runner.snapshot` at the new tick index (the "tick-level
  checkpoint" -- 快照粒度一个 tick, per 重构.md).

Checkpoints are *named* snapshots (label -> snapshot) layered on top, backing
:meth:`Runner.checkpoint` / :meth:`Runner.rollback_to`.

Two reference implementations ship:

- :class:`JsonBackend` -- one directory per session, ``tick_N.json`` +
  ``firings.jsonl`` + ``checkpoints.json``. Default; good for small/medium
  graphs and human inspection.
- :class:`SqliteBackend` -- a single DB file (SQLite) with ``snapshots`` /
  ``firings`` / ``checkpoints`` tables; better for high tick-throughput
  and concurrent sessions. Optional; use by passing
  ``backend=SqliteBackend("path/to.db")`` to the Runner.

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
import sqlite3
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
        """Append a single :class:`~tickflow.state.NodeState` (or its JSON form)
        to the process log. Append-only; never rewritten. Prefer
        :meth:`save_firings` (batch) when persisting a whole tick."""
        ...

    def save_firings(self, session_id: str, firings: list) -> None:
        """Append multiple firings in one batch (one transaction/fsync where
        the backend supports it). Implementations should be equivalent to
        calling :meth:`save_firing` for each element but cheaper. ``firings``
        may be empty (no-op)."""
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

    def save_firings(self, session_id: str, firings: list) -> None:
        bucket = self._firings.setdefault(session_id, [])
        for f in firings:
            d = f.to_json() if hasattr(f, "to_json") else dict(f)
            bucket.append(d)

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
        firings.jsonl     # append-only process log, one NodeState per line
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

    def save_firings(self, session_id: str, firings: list) -> None:
        if not firings:
            return
        d = self._session_dir(session_id)
        d.mkdir(parents=True, exist_ok=True)
        # One open/flush for the whole batch -- avoids per-firing open fsync.
        with self._firings_path(session_id).open("a", encoding="utf-8") as f:
            for firing in firings:
                payload = firing.to_json() if hasattr(firing, "to_json") else dict(firing)
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


class SqliteBackend:
    """SQLite backend: single DB file for snapshots, firings, and checkpoints.

    Uses WAL journal mode for better concurrent read performance. A single
    connection is opened at construction time with ``check_same_thread=False``
    so the same backend can be shared by sync and async runners.

    Thread safety
    -------------
    A single ``sqlite3.Connection`` is **not** safe for concurrent use from
    multiple threads. This backend guards every write with an internal
    :class:`threading.Lock`, so multiple threads sharing one ``SqliteBackend``
    instance won't corrupt the database -- but writes serialise, and under
    heavy contention you may see ``database is locked`` retries (caught and
    re-raised here after the lock is released). For high-concurrency servers,
    prefer one backend instance per thread/worker, or wrap calls in
    ``asyncio.to_thread`` from a single async runner.

    The parent directory of ``db_path`` is created automatically if it does
    not exist (matching :class:`JsonBackend`). Pass ``":memory:"`` for an
    in-memory database (useful for testing); note that ``:memory:`` databases
    are per-connection, so two ``SqliteBackend(":memory:")`` instances do NOT
    share data -- use ``"file::memory:?cache=shared"`` for that.

    Parameters
    ----------
    db_path : str | Path
        Path to the SQLite database file.
    """

    def __init__(self, db_path: str | Path) -> None:
        import threading
        self._db_path = Path(db_path)
        # Don't create parent directories for :memory: or file: URIs.
        _raw = str(db_path)
        if _raw not in (":memory:",) and not _raw.startswith("file:"):
            self._db_path.parent.mkdir(parents=True, exist_ok=True)
        self._conn = sqlite3.connect(str(self._db_path), check_same_thread=False)
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._lock = threading.Lock()
        self._init_tables()

    # -- schema -------------------------------------------------------------

    def _init_tables(self) -> None:
        """Create tables and indexes if they do not yet exist."""
        self._conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS snapshots (
                session_id TEXT NOT NULL,
                tick       INTEGER NOT NULL,
                data       TEXT    NOT NULL,
                PRIMARY KEY (session_id, tick)
            );
            CREATE TABLE IF NOT EXISTS firings (
                id         INTEGER PRIMARY KEY AUTOINCREMENT,
                session_id TEXT    NOT NULL,
                tick       INTEGER NOT NULL,
                data       TEXT    NOT NULL
            );
            CREATE TABLE IF NOT EXISTS checkpoints (
                session_id TEXT    NOT NULL,
                label      TEXT    NOT NULL,
                tick       INTEGER NOT NULL,
                data       TEXT    NOT NULL,
                PRIMARY KEY (session_id, label)
            );
            CREATE INDEX IF NOT EXISTS idx_firings_lookup
                ON firings (session_id, tick, id);
            """
        )

    # -- snapshots ----------------------------------------------------------

    def save_snapshot(self, session_id: str, tick: int, snap: dict) -> None:
        with self._lock:
            self._conn.execute(
                "INSERT OR REPLACE INTO snapshots (session_id, tick, data) VALUES (?, ?, ?)",
                (session_id, tick, json.dumps(snap, default=_default)),
            )
            self._conn.commit()

    def load_snapshot(self, session_id: str, tick: int) -> dict | None:
        with self._lock:
            row = self._conn.execute(
                "SELECT data FROM snapshots WHERE session_id = ? AND tick = ?",
                (session_id, tick),
            ).fetchone()
        return json.loads(row[0]) if row else None

    def latest_tick(self, session_id: str) -> int | None:
        with self._lock:
            row = self._conn.execute(
                "SELECT MAX(tick) FROM snapshots WHERE session_id = ?",
                (session_id,),
            ).fetchone()
        return row[0] if row and row[0] is not None else None

    def list_snapshots(self, session_id: str) -> list[int]:
        with self._lock:
            rows = self._conn.execute(
                "SELECT tick FROM snapshots WHERE session_id = ? ORDER BY tick",
                (session_id,),
            ).fetchall()
        return [r[0] for r in rows]

    # -- firings ------------------------------------------------------------

    def save_firing(self, session_id: str, firing: Any) -> None:
        d = firing.to_json() if hasattr(firing, "to_json") else dict(firing)
        tick = d.get("tick", 0)
        with self._lock:
            self._conn.execute(
                "INSERT INTO firings (session_id, tick, data) VALUES (?, ?, ?)",
                (session_id, tick, json.dumps(d, default=_default)),
            )
            self._conn.commit()

    def save_firings(self, session_id: str, firings: list) -> None:
        """Batch-insert all firings in a single transaction (one commit).
        Cheaper than per-firing :meth:`save_firing` for a tick with N fires."""
        if not firings:
            return
        rows = []
        for firing in firings:
            d = firing.to_json() if hasattr(firing, "to_json") else dict(firing)
            rows.append((session_id, d.get("tick", 0), json.dumps(d, default=_default)))
        with self._lock:
            self._conn.executemany(
                "INSERT INTO firings (session_id, tick, data) VALUES (?, ?, ?)",
                rows,
            )
            self._conn.commit()

    def list_firings(self, session_id: str, since_tick: int = 0) -> list[dict]:
        with self._lock:
            rows = self._conn.execute(
                "SELECT data FROM firings WHERE session_id = ? AND tick >= ? ORDER BY id",
                (session_id, since_tick),
            ).fetchall()
        return [json.loads(r[0]) for r in rows]

    # -- checkpoints --------------------------------------------------------

    def save_checkpoint(self, session_id: str, label: str, snap: dict) -> None:
        with self._lock:
            self._conn.execute(
                "INSERT OR REPLACE INTO checkpoints (session_id, label, tick, data) "
                "VALUES (?, ?, ?, ?)",
                (
                    session_id,
                    label,
                    snap.get("tick", 0),
                    json.dumps(snap, default=_default),
                ),
            )
            self._conn.commit()

    def list_checkpoints(self, session_id: str) -> list[tuple[str, int]]:
        with self._lock:
            rows = self._conn.execute(
                "SELECT label, tick FROM checkpoints WHERE session_id = ? ORDER BY label",
                (session_id,),
            ).fetchall()
        return [(r[0], r[1]) for r in rows]

    def load_checkpoint(self, session_id: str, label: str) -> dict | None:
        with self._lock:
            row = self._conn.execute(
                "SELECT data FROM checkpoints WHERE session_id = ? AND label = ?",
                (session_id, label),
            ).fetchone()
        return json.loads(row[0]) if row else None

    # -- cleanup ------------------------------------------------------------

    def close(self) -> None:
        with self._lock:
            self._conn.close()
        """Close the underlying SQLite connection."""
        self._conn.close()


def _default(o: Any) -> Any:
    """json default for objects that aren't natively serialisable. Mirrors
    runner._jsonable's fallback policy (repr) so snapshots stay stable."""
    if hasattr(o, "isoformat"):
        return o.isoformat()
    return repr(o)
