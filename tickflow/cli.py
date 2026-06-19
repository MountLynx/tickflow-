"""Command-line interface.

Usage
-----
The graph text declares structure; behaviour (bodies/guards) lives in a Python
module whose callables you register on the default :data:`tickflow.registry`.
Point the CLI at that module with ``--behaviours`` (or ``-b``); the module is
imported and expected to register bodies/guards at import time.

    python -m tickflow run graph.txt -b my_behaviours.py --max-ticks 100
    python -m tickflow run graph.txt -b b.py --pause-at 5        # stop at tick 5
    python -m tickflow step graph.txt -b b.py --from-snapshot snap.json --ticks 3
    python -m tickflow snapshot graph.txt -b b.py --out snap.json
    python -m tickflow audit run.json                            # print audit log

Deadlock suggestions are presented interactively on ``run``/``step``: each is
shown and you confirm whether to promote the AND-join to OR-join. Use
``--auto-promote`` to accept all, or ``--no-promote`` to reject (and error out).
"""
from __future__ import annotations

import argparse
import importlib.util
import sys
from pathlib import Path

from . import parse, check, promote, DeadlockError, Registry, registry as _default_registry
from .runner import Runner


def _load_behaviours(path: str) -> None:
    """Import a Python file so it registers bodies/guards on the default
    registry (the module is expected to do ``from tickflow import registry`` and
    decorate callables)."""
    p = Path(path)
    spec = importlib.util.spec_from_file_location(p.stem, p)
    if spec is None or spec.loader is None:
        raise SystemExit(f"cannot load behaviours module {path!r}")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)


def _resolve_deadlocks(graph, args) -> None:
    sugs = check(graph)
    if not sugs:
        return
    if args.no_promote:
        raise DeadlockError(sugs)
    for s in sugs:
        print(s.msg, file=sys.stderr)
        if args.auto_promote:
            print("  (auto-promoting to OR-join)", file=sys.stderr)
            promote(s, graph)
            continue
        ans = input("  promote to OR-join? [y/N] ").strip().lower()
        if ans == "y":
            promote(s, graph)
        else:
            raise DeadlockError([s])


def _cmd_run(args) -> int:
    _load_behaviours(args.behaviours)
    graph = parse(Path(args.graph).read_text(encoding="utf-8"), registry=_default_registry)
    _resolve_deadlocks(graph, args)
    rn = Runner(graph, _default_registry)
    rn.run_until_idle(max_ticks=args.max_ticks, pause_at=set(args.pause_at or ()))
    print(rn.to_json())
    return 0


def _cmd_step(args) -> int:
    _load_behaviours(args.behaviours)
    graph = parse(Path(args.graph).read_text(encoding="utf-8"), registry=_default_registry)
    if args.from_snapshot:
        # Snapshot files are full Runner.to_json() dumps (snapshot + audit).
        raw = Path(args.from_snapshot).read_text(encoding="utf-8")
        rn = Runner.from_json(raw, graph, _default_registry)
    else:
        _resolve_deadlocks(graph, args)
        rn = Runner(graph, _default_registry)
    rn.run_until_idle(max_ticks=args.ticks)
    print(rn.to_json())
    return 0


def _cmd_snapshot(args) -> int:
    _load_behaviours(args.behaviours)
    graph = parse(Path(args.graph).read_text(encoding="utf-8"), registry=_default_registry)
    _resolve_deadlocks(graph, args)
    rn = Runner(graph, _default_registry)
    rn.run_until_idle(max_ticks=args.max_ticks)
    Path(args.out).write_text(rn.to_json(), encoding="utf-8")
    print(f"snapshot written to {args.out} (tick={rn.tick_count}, idle={rn.is_idle()})")
    return 0


def _cmd_audit(args) -> int:
    import json
    data = json.loads(Path(args.run).read_text(encoding="utf-8"))
    for e in data.get("audit", []):
        edges = ", ".join(f"{e2[0]}({e2[1] or '-'})={'T' if e2[2] else 'F'}" for e2 in e["edges_fired"])
        print(f"t{e['tick']:>3} {e['node']:<10} out={e['output']!r:<20} [{edges}]")
    return 0


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="python -m tickflow", description=__doc__)
    sub = p.add_subparsers(dest="cmd", required=True)

    common = argparse.ArgumentParser(add_help=False)
    common.add_argument("-b", "--behaviours", required=True, help="Python file registering bodies/guards")
    common.add_argument("--auto-promote", action="store_true", help="accept all OR-join suggestions")
    common.add_argument("--no-promote", action="store_true", help="reject all suggestions (error on any)")

    pr = sub.add_parser("run", parents=[common], help="run a graph to idle")
    pr.add_argument("graph")
    pr.add_argument("--max-ticks", type=int, default=1000)
    pr.add_argument("--pause-at", type=int, nargs="*", default=None)
    pr.set_defaults(func=_cmd_run)

    ps = sub.add_parser("step", parents=[common], help="step a graph N ticks (optionally from a snapshot)")
    ps.add_argument("graph")
    ps.add_argument("--ticks", type=int, default=1)
    ps.add_argument("--from-snapshot", default=None, help="JSON snapshot to resume from")
    ps.set_defaults(func=_cmd_step)

    pss = sub.add_parser("snapshot", parents=[common], help="run to idle and write a snapshot file")
    pss.add_argument("graph")
    pss.add_argument("--out", required=True)
    pss.add_argument("--max-ticks", type=int, default=1000)
    pss.set_defaults(func=_cmd_snapshot)

    pa = sub.add_parser("audit", help="print an audit log from a run dump")
    pa.add_argument("run")
    pa.set_defaults(func=_cmd_audit)

    return p


def main(argv=None) -> int:
    args = build_parser().parse_args(argv)
    return args.func(args)


if __name__ == "__main__":
    sys.exit(main())
