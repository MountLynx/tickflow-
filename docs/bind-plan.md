# bind 迁移实施计划（tickflow）

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 落地 [bind-design.md](bind-design.md)：IR 级 `bind` 声明（位置式 + 具名式）、值模式/视图模式双分发、GuardView 裁决语义、Missing 保真、Runner 构建期 E1–E3/W1 校验。

**Architecture:** bind 声明进 IR，引擎单点解析（`_prepare_fire`）后按注册期签名（`Sig`）分发给值模式或视图模式 body/guard；sync 与 async 引擎共享全部解析与调用装配 helper；legacy 按名访问（`v.A`/`v["A"]`/`v.inputs()`/`v.items()`/`in`）经 `DictView` 兼容垫片保留并告警。

**Tech Stack:** Python 3.10+（仓库要求 `>=3.10`，本机 3.13.7）、pytest（无 asyncio 插件——异步测试用 `asyncio.run` 包装，参照 `tests/test_async.py:40-41`）、dataclasses。

**规约：**
- 每个任务：写失败测试 → 跑失败 → 最小实现 → 跑通过 → **跑全量测试**（`python -m pytest tests -q`，基线 186 passed）→ 提交。
- 全量回归是硬性步骤：本迁移必须保证存量 `def f(v)` 图全程绿（legacy 路径允许 DeprecationWarning，不允许失败）。
- 工作目录：`C:\Users\xingy\Desktop\开发\Graph`。测试命令统一 `python -m pytest <file> -q`。
- 不要动 `README.md` / `README.zh.md`（工作区有用户未提交的改动）。

---

### Task 0: 基线确认

- [ ] **Step 1: 跑全量测试确认基线**

Run: `python -m pytest tests -q`
Expected: `186 passed`（若数量不同，记录实际数字作为后续对照；有失败则先停止排查环境）

---

### Task 1: IR — `Bind` 数据类与 `Node.bind`

**Files:**
- Modify: `tickflow/ir.py`
- Create: `tests/test_bind.py`

- [ ] **Step 1: 写失败测试**

```python
# tests/test_bind.py
"""Bind declaration IR: construction, validation, Node/Graph integration."""
import json

import pytest

from tickflow import parse
from tickflow.ir import Bind


def test_bind_positional():
    b = Bind.positional(["A", "C"])
    assert b.entries == ((None, "A"), (None, "C"))
    assert b.producers == ("A", "C")
    assert b.fields == ()
    assert b.is_named is False


def test_bind_named():
    b = Bind.named({"outline": "A", "draft": "C"})
    assert b.fields == ("outline", "draft")
    assert b.producers == ("A", "C")
    assert b.is_named is True


def test_bind_named_accepts_pair_list():
    b = Bind.named([("x", "A"), ("y", "B")])
    assert b.fields == ("x", "y")


def test_bind_rejects_mixed():
    with pytest.raises(ValueError, match="mix"):
        Bind(entries=(("x", "A"), (None, "C")))


def test_bind_rejects_duplicate_field():
    with pytest.raises(ValueError, match="[Dd]uplicate"):
        Bind(entries=(("x", "A"), ("x", "C")))


def test_bind_rejects_duplicate_producer():
    with pytest.raises(ValueError, match="[Dd]uplicate"):
        Bind.positional(["A", "A"])


def test_bind_rejects_empty():
    with pytest.raises(ValueError, match="at least one"):
        Bind(entries=())


def test_node_bind_default_none_copy_and_to_dict():
    g = parse("[A]-->B\nB.bind: {x: A}", registry=None)
    assert g.nodes["A"].bind is None
    assert g.nodes["B"].bind is not None
    assert g.nodes["B"].bind.fields == ("x",)
    g2 = g.copy()
    assert g2.nodes["B"].bind == g.nodes["B"].bind
    d = g.to_dict()
    assert d["nodes"]["B"]["bind"] == {"fields": ["x"], "producers": ["A"]}
    assert d["nodes"]["A"]["bind"] is None
    json.dumps(d)  # must stay JSON-serialisable
```

- [ ] **Step 2: 跑测试确认失败**

Run: `python -m pytest tests/test_bind.py -q`
Expected: FAIL — `ImportError: cannot import name 'Bind'`

- [ ] **Step 3: 实现**

`tickflow/ir.py` — 在 `InputPolicy` 之后加：

```python
@dataclass(frozen=True)
class Bind:
    """Normalized bind declaration: ordered ``(field, producer)`` entries.

    ``field`` is ``None`` for positional (anonymous) entries. A Bind is either
    all-positional or all-named; mixing is rejected here so the engine never
    sees an ambiguous declaration. Programmatic graph builders pass
    ``Bind.named({...})``; the parser produces the same via text syntax.
    """

    entries: tuple[tuple[str | None, str], ...]

    def __post_init__(self) -> None:
        if not self.entries:
            raise ValueError("bind must declare at least one producer")
        fields = [f for f, _ in self.entries if f is not None]
        if fields and len(fields) != len(self.entries):
            raise ValueError("bind cannot mix positional and named entries")
        if len(set(fields)) != len(fields):
            raise ValueError(f"duplicate bind fields: {fields}")
        prods = [p for _, p in self.entries]
        if len(set(prods)) != len(prods):
            raise ValueError(f"duplicate bind producers: {prods}")

    @classmethod
    def positional(cls, producers) -> "Bind":
        """Anonymous positional bind: parameter i <- producers[i]."""
        return cls(entries=tuple((None, p) for p in producers))

    @classmethod
    def named(cls, mapping) -> "Bind":
        """Named bind: field -> producer (dict or iterable of pairs)."""
        items = mapping.items() if hasattr(mapping, "items") else mapping
        return cls(entries=tuple((f, p) for f, p in items))

    @property
    def fields(self) -> tuple[str, ...]:
        return tuple(f for f, _ in self.entries if f is not None)

    @property
    def producers(self) -> tuple[str, ...]:
        return tuple(p for _, p in self.entries)

    @property
    def is_named(self) -> bool:
        return bool(self.fields)
```

`tickflow/ir.py` — `Node` 增加字段（放在 `inputs` 之后）：

```python
    # ordered bind declaration: which producer feeds which parameter/field.
    # None = auto-bind from the ``inputs`` key order (see engine.bind_entries).
    bind: Bind | None = None
```

`tickflow/ir.py` — `Graph.copy()` 的 `Node(...)` 构造加一行 `bind=node.bind,`（frozen 对象可安全共享）。

`tickflow/ir.py` — `Graph.to_dict()` 的节点 dict 中、`"inputs"` 之后加：

```python
                    "bind": (
                        None if n.bind is None
                        else {"fields": list(n.bind.fields), "producers": list(n.bind.producers)}
                    ),
```

- [ ] **Step 4: 跑测试确认通过 + 全量回归**

Run: `python -m pytest tests/test_bind.py tests -q`
Expected: 全部 PASS（`to_dict` 现有测试只断言顶层键与逐键取值，新增节点内 `bind` 键不破坏）

- [ ] **Step 5: 提交**

```bash
git add tickflow/ir.py tests/test_bind.py
git commit -m "feat(ir): Bind declaration (positional + named) and Node.bind field"
```

---

### Task 2: parser — bind 语法与校验

**Files:**
- Modify: `tickflow/parser.py`
- Modify: `tests/test_bind.py`（追加）

- [ ] **Step 1: 写失败测试（追加到 tests/test_bind.py）**

```python
# --- parser syntax (Task 2) ---

def test_parser_bind_positional():
    g = parse("[A]-->C\n[B]-->C\nC.bind: [A, B]", registry=None)
    assert g.nodes["C"].bind.entries == ((None, "A"), (None, "B"))


def test_parser_bind_named():
    g = parse("[A]-->C\nC.bind: {x: A}", registry=None)
    assert g.nodes["C"].bind.entries == (("x", "A"),)


def test_parser_bind_single_sugar():
    g = parse("[A]-->C\nC.bind: A", registry=None)
    assert g.nodes["C"].bind.entries == ((None, "A"),)


def test_parser_bind_bad_term():
    with pytest.raises(ParseError):
        parse("[A]-->C\nC.bind: [A, 2B]", registry=None)


def test_parser_bind_unterminated():
    with pytest.raises(ParseError):
        parse("[A]-->C\nC.bind: [A", registry=None)


def test_parser_bind_named_bad_term():
    with pytest.raises(ParseError):
        parse("[A]-->C\nC.bind: {x A}", registry=None)


def test_parser_bind_unknown_producer():
    with pytest.raises(ParseError, match="not a node"):
        parse("[A]-->C\nC.bind: [Z]", registry=None)


def test_parser_bind_unreachable_producer():
    with pytest.raises(ParseError, match="no directed path"):
        parse("[A]-->B\nC-->D\nB.bind: [C]", registry=None)


def test_parser_bind_upstream_nonproducer_warns_and_adds_policy():
    g = parse("[A]-->B\nB-->C\nC.bind: [A]", registry=None)
    assert g.nodes["C"].inputs["A"].kind == "latest"


def test_parser_bind_preserves_declared_policies():
    g = parse("[A]-->C\n[B]-->C\nC.inputs: A, B[2]\nC.bind: [B, A]", registry=None)
    assert g.nodes["C"].inputs["B"].kind == "index"
    assert g.nodes["C"].inputs["A"].kind == "latest"
```

文件头部 import 改为 `from tickflow import parse, ParseError`。

- [ ] **Step 2: 跑测试确认失败**

Run: `python -m pytest tests/test_bind.py -q`
Expected: 新增用例 FAIL（`ParseError: unrecognized line`），Task 1 用例 PASS

- [ ] **Step 3: 实现**

`tickflow/parser.py`：

1. import 行改为 `from .ir import Graph, Node, Edge, InputPolicy, Bind`。
2. 正则区（`_JOIN_RE` 之后）加：

```python
_BIND_RE = re.compile(rf"^(?P<node>{_NAME})\.bind\s*:\s*(?P<spec>.+)$")
```

3. `_parse_inputs_spec` 之后加两个函数：

```python
def _split_terms(s: str) -> list[str]:
    return [t.strip() for t in s.split(",") if t.strip()]


def _parse_bind_spec(spec: str, lineno: int) -> Bind:
    """Parse ``[A, C]`` / ``{field: A}`` / single-name sugar into a Bind."""
    text = spec.strip()
    try:
        if text.startswith("{"):
            if not text.endswith("}"):
                raise ValueError("unterminated named bind (missing '}')")
            entries: list[tuple[str, str]] = []
            for term in _split_terms(text[1:-1]):
                m = re.fullmatch(rf"({_NAME})\s*:\s*({_NAME})", term)
                if not m:
                    raise ValueError(
                        f"bad named bind term {term!r} (expected 'field: producer')"
                    )
                entries.append((m.group(1), m.group(2)))
            if not entries:
                raise ValueError("empty named bind")
            return Bind.named(entries)
        if text.startswith("["):
            if not text.endswith("]"):
                raise ValueError("unterminated positional bind (missing ']')")
            terms = _split_terms(text[1:-1])
            if not terms:
                raise ValueError("empty positional bind")
            for term in terms:
                if not re.fullmatch(_NAME, term):
                    raise ValueError(f"bad positional bind term {term!r}")
            return Bind.positional(terms)
        if re.fullmatch(_NAME, text):
            return Bind.positional([text])
        raise ValueError("expected [A, C] or {field: A} or a single producer name")
    except ValueError as e:
        raise ParseError(str(e), lineno) from e
```

4. `parse()` 的声明分支区（`_INPUTS_RE` 块之后、`_BODY_RE` 之前或之后均可，加在 `_INPUTS_RE` 块后）：

```python
        if (m := _BIND_RE.match(line)):
            node = m.group("node")
            _ensure_node(g, node)
            g.nodes[node].bind = _parse_bind_spec(m.group("spec"), lineno)
            continue
```

5. `_validate()`：在 upstream 闭包计算完成之后、现有 `# Validate inputs` 循环之后、`# Warn on inputs from bodyless nodes` 循环之前，插入：

```python
    # Validate binds (existence + reachability, same rule as inputs) and make
    # every bound producer resolvable: default policy is latest_before.
    for name, node in g.nodes.items():
        if node.bind is None:
            continue
        producers = set(g.producers(name))
        for _field, prod in node.bind.entries:
            if prod not in g.nodes:
                raise ParseError(
                    f"node {name!r} binds from {prod!r} which is not "
                    f"a node in the graph",
                    n_lines,
                )
            if prod not in producers:
                if prod not in upstream.get(name, set()):
                    raise ParseError(
                        f"node {name!r} binds from {prod!r} which is not a producer "
                        f"and has no directed path to {name!r} — "
                        f"{prod!r} fires after or independently of {name!r}, "
                        f"so the bound input will always be Missing",
                        n_lines,
                    )
                log.warning(
                    "node %r binds from %r which is not a producer "
                    "(producers: %s) — resolution will use history, not token flow",
                    name, prod, sorted(producers) or 'none',
                )
            node.inputs.setdefault(prod, InputPolicy.latest())
```

6. 模块 docstring 的 Declaration lines 区追加：

```
    B.bind: [A, C]      positional bind: parameter 1 <- A, parameter 2 <- C
    B.bind: {f: A}      named bind: field f <- producer A
    B.bind: A           sugar for [A]
```

- [ ] **Step 4: 跑测试确认通过 + 全量回归**

Run: `python -m pytest tests/test_bind.py tests -q`
Expected: 全部 PASS

- [ ] **Step 5: 提交**

```bash
git add tickflow/parser.py tests/test_bind.py
git commit -m "feat(parser): bind declaration syntax ([A,C] / {field: A} / sugar) + validation"
```

---

### Task 3: registry — `Sig` 注册期签名分类

**Files:**
- Modify: `tickflow/registry.py`
- Create: `tests/test_registry_sig.py`

- [ ] **Step 1: 写失败测试**

```python
# tests/test_registry_sig.py
"""Registration-time signature classification (value mode vs view mode)."""
import pytest

from tickflow.registry import Registry, classify
from tickflow.views import NodeView, DictView


def test_view_by_name_v():
    def f(v): ...
    assert classify(f).mode == "view"


def test_view_by_name_view():
    def f(view): ...
    assert classify(f).mode == "view"


def test_view_by_string_annotation():
    def f(v: "DictView"): ...
    assert classify(f).mode == "view"


def test_view_by_class_annotation():
    def f(v: NodeView): ...
    assert classify(f).mode == "view"


def test_value_plain_names():
    def f(a, b): ...
    s = classify(f)
    assert s.mode == "value"
    assert s.arity == 2
    assert s.param_names == ("a", "b")
    assert s.wants_state is False


def test_value_zero_arity():
    def f(): ...
    s = classify(f)
    assert s.mode == "value"
    assert s.arity == 0


def test_value_wants_state():
    def f(a, *, state): ...
    s = classify(f)
    assert s.mode == "value"
    assert s.arity == 1
    assert s.wants_state is True


def test_async_detection():
    async def f(v): ...
    assert classify(f).is_async is True

    async def g(a): ...
    s = classify(g)
    assert s.mode == "value" and s.is_async is True


def test_registry_records_and_serves_sigs():
    r = Registry()
    r.body("b", lambda a: a)
    r.guard("g", lambda v: True)
    assert r.body_sig("b").arity == 1
    assert r.guard_sig("g").mode == "view"
    with pytest.raises(KeyError):
        r.body_sig("nope")
    with pytest.raises(KeyError):
        r.guard_sig("nope")
```

- [ ] **Step 2: 跑测试确认失败**

Run: `python -m pytest tests/test_registry_sig.py -q`
Expected: FAIL — `ImportError: cannot import name 'classify'`

- [ ] **Step 3: 实现**

`tickflow/registry.py`：

1. 头部 import 区改为：

```python
import inspect
from dataclasses import dataclass
from typing import Callable, Any, Literal
```

2. 类型别名区（`Body = ...`）替换为：

```python
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
    ``NodeView``/``DictView``) is view mode; everything else is value mode."""
    sig = inspect.signature(fn)
    params = list(sig.parameters.values())
    positional = [
        p for p in params
        if p.kind in (inspect.Parameter.POSITIONAL_ONLY,
                      inspect.Parameter.POSITIONAL_OR_KEYWORD)
    ]
    kwonly = [p for p in params if p.kind == inspect.Parameter.KEYWORD_ONLY]
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
        arity=len(positional),
        param_names=tuple(p.name for p in positional),
        wants_state=any(p.name == "state" for p in kwonly),
        is_async=is_async,
    )
```

3. `Registry.__init__` 加两个缓存 dict；`body()`/`guard()` 注册时写缓存；新增查询方法：

```python
    def __init__(self) -> None:
        self._bodies: dict[str, Body] = {}
        self._guards: dict[str, Guard] = {}
        self._body_sigs: dict[str, Sig] = {}
        self._guard_sigs: dict[str, Sig] = {}
```

`body()` 两个注册点（deco 内与直调路径）都加 `self._body_sigs[name] = classify(fn)`；`guard()` 同理加 `self._guard_sigs[name] = classify(fn)`。lookup 区新增：

```python
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
```

4. 模块 docstring 的 Callables 区改为描述双模式（值模式位置/具名/state 尾参；视图模式 `v.input()`/`v.args`/`v.named`）。

- [ ] **Step 4: 跑测试确认通过 + 全量回归**

Run: `python -m pytest tests/test_registry_sig.py tests -q`
Expected: 全部 PASS

- [ ] **Step 5: 提交**

```bash
git add tickflow/registry.py tests/test_registry_sig.py
git commit -m "feat(registry): Sig registration-time classification (value/view mode dispatch)"
```

---

### Task 4: views — `NodeView` / `GuardView` / `DictView` 兼容垫片

**Files:**
- Modify: `tickflow/views.py`
- Create: `tests/test_node_view.py`

- [ ] **Step 1: 写失败测试**

```python
# tests/test_node_view.py
"""NodeView / GuardView consumption API + legacy DictView shim."""
import pytest

from tickflow.views import (
    DictView, GuardView, Missing, NodeView, Resolved, _ReadOnlyStateView,
)


def _node_view(fields=None, values=(), resolved=None, node="n"):
    return NodeView(node=node, fields=fields, values=values,
                    state={"s": 1}, resolved=resolved)


def test_input_none_when_no_inputs():
    v = _node_view(fields=(), values=())
    assert v.input() is None
    assert v.args == ()
    assert v.named == {}


def test_input_single_value():
    v = _node_view(fields=((None, "A"),), values=(7,))
    assert v.input() == 7
    assert v.args == (7,)


def test_input_multi_returns_tuple():
    v = _node_view(fields=((None, "A"), (None, "B")), values=(1, 2))
    assert v.input() == (1, 2)
    assert v.args == (1, 2)


def test_missing_survives_in_args_named_and_input():
    # Correctness contract (design 4.6): Missing is never collapsed to None.
    v = _node_view(fields=(("prev", "P"), ("seed", "S")),
                   values=(Missing, "S"))
    assert v.args == (Missing, "S")
    assert v.named == {"prev": Missing, "seed": "S"}
    assert v.input() == (Missing, "S")
    assert v.field("prev") is Missing


def test_named_empty_for_positional_bind():
    v = _node_view(fields=((None, "A"),), values=(1,))
    assert v.named == {}


def test_field_on_positional_bind_raises_type_error():
    v = _node_view(fields=((None, "A"),), values=(1,))
    with pytest.raises(TypeError):
        v.field("A")


def test_field_unknown_name_raises_key_error():
    v = _node_view(fields=(("x", "A"),), values=(1,))
    with pytest.raises(KeyError):
        v.field("y")


def test_state_and_node():
    v = _node_view(node="me")
    assert v.state == {"s": 1}
    assert v.node == "me"


def test_legacy_name_access_warns_and_resolves():
    v = _node_view(fields=None, values=(5,),
                   resolved={"A": Resolved(value=5, k=None)})
    with pytest.warns(DeprecationWarning):
        assert v.A.value == 5
    with pytest.warns(DeprecationWarning):
        assert v["A"].value == 5
    with pytest.warns(DeprecationWarning):
        assert v.inputs() == {"A": 5}
    with pytest.warns(DeprecationWarning):
        assert list(v.items()) == [("A", 5)]
    with pytest.warns(DeprecationWarning):
        assert "A" in v


def test_legacy_missing_name_raises_without_warning():
    import warnings as w
    v = _node_view(resolved={})
    with w.catch_warnings(record=True) as rec:
        w.simplefilter("always")
        with pytest.raises(AttributeError):
            v.Nope
        with pytest.raises(KeyError):
            v["Nope"]
        assert not any(issubclass(x.category, DeprecationWarning) for x in rec)


def test_dictview_shim_warns_and_behaves():
    with pytest.warns(DeprecationWarning):
        v = DictView({"A": Resolved(value=5, k=None)}, node="legacy")
    assert v.node == "legacy"
    assert v.input() == 5
    with pytest.warns(DeprecationWarning):
        assert v["A"].value == 5


# --- GuardView ---

def _guard_view(**kw):
    defaults = dict(
        src="R", output="out", fields=(("d", "M"),), values=("m",),
        state=_ReadOnlyStateView({"n": 1}),
        resolved={"M": Resolved("m", None), "d": Resolved("m", None),
                  "R": Resolved("out", None)},
    )
    defaults.update(kw)
    return GuardView(**defaults)


def test_guard_view_output_args_named_field():
    v = _guard_view()
    assert v.output == "out"
    assert v.args == ("m",)
    assert v.named == {"d": "m"}
    assert v.field("d") == "m"
    assert v.node == "R" and v.src == "R"


def test_guard_view_has_no_input():
    v = _guard_view()
    with pytest.raises(AttributeError):
        v.input()


def test_guard_view_state_read_only():
    st = _ReadOnlyStateView({"n": 1})
    assert st.get("n") == 1
    with pytest.raises(TypeError):
        st["x"] = 2
    with pytest.raises(TypeError):
        st.x = 2


def test_guard_view_legacy_access_and_precedence():
    v = _guard_view()
    with pytest.warns(DeprecationWarning):
        assert v["R"].value == "out"      # src name -> adjudicated output
    with pytest.warns(DeprecationWarning):
        assert v["M"].value == "m"        # producer name -> declared input
    with pytest.raises(KeyError):
        v["Nope"]                          # undeclared node -> refused (P4)
```

- [ ] **Step 2: 跑测试确认失败**

Run: `python -m pytest tests/test_node_view.py -q`
Expected: FAIL — `ImportError: cannot import name 'NodeView'`

- [ ] **Step 3: 实现**

`tickflow/views.py` — 保留模块 docstring（改写末段指向 NodeView）、`_MissingType`/`Missing`、`Resolved`、`_ResolvedAttr` 不动；**删除整个 `class DictView`**，新增以下内容（放在 `_ResolvedAttr` 之后）：

```python
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
```

文件头部 import 补 `import warnings`。

- [ ] **Step 4: 跑测试确认通过 + 全量回归**

Run: `python -m pytest tests/test_node_view.py tests -q`
Expected: 全部 PASS（engine 仍构造旧式 `DictView(...)`——现在是垫片子类，行为等价 + 构造告警，存量测试不断言警告故不失败）

- [ ] **Step 5: 提交**

```bash
git add tickflow/views.py tests/test_node_view.py
git commit -m "feat(views): NodeView/GuardView consumption API + deprecated DictView shim"
```

---

### Task 5: engine（sync）— bind 解析与 body 分发

**Files:**
- Modify: `tickflow/engine.py`
- Create: `tests/test_dispatch.py`

- [ ] **Step 1: 写失败测试**

```python
# tests/test_dispatch.py
"""Engine dispatch: value-mode / view-mode bodies, bind resolution, Missing."""
import pytest

from tickflow import Registry, Runner, parse
from tickflow.views import Missing


def test_value_mode_positional():
    r = Registry()
    r.body("seed", lambda: "s")
    r.body("incr", lambda a: a + "!")
    g = parse("[S]-->B\nS.body: seed\nB.body: incr", registry=r)
    run = Runner(g, r)
    run.run_until_idle(max_ticks=5)
    assert run.run_state.last_output("B") == "s!"


def test_value_mode_named_bind_kwargs():
    r = Registry()
    r.body("a", lambda: 1)
    r.body("b", lambda: 2)

    def combine(a, b):
        return a + b

    r.body("sum", combine)
    g = parse(
        "[A]-->C\n[B]-->C\nC.bind: {a: A, b: B}\nC.body: sum", registry=r
    )
    run = Runner(g, r)
    run.run_until_idle(max_ticks=5)
    assert run.run_state.last_output("C") == 3


def test_value_mode_state_tail_param():
    # `state` must be keyword-only: positional params are bind values, so a
    # body wanting state + values writes `def f(value, *, state)`.
    r = Registry()

    def count(_prev, *, state):
        n = state.get("n", 0) + 1
        state["n"] = n
        return n

    r.body("count", count)
    # A.bind: [S] pins one entry (self-loop A is a producer too, but the body
    # does not consume it); without the explicit bind E1 would demand arity 2.
    g = parse("[S]-->A\nA.body: count\nA-->A\nA.join: OR\nA.bind: [S]", registry=r)
    run = Runner(g, r)
    for _ in range(4):  # t0 seed; t1..t3 count fires 3 times
        run.tick()
    assert run.run_state.last_output("A") == 3


def test_view_mode_auto_detected_and_legacy_access_warns():
    r = Registry()
    r.body("seed", lambda: "s")

    def sink(v):
        with pytest.warns(DeprecationWarning):
            val = v["S"].value
        return val

    r.body("sink", sink)
    g = parse("[S]-->B\nS.body: seed\nB.body: sink", registry=r)
    Runner(g, r).run_until_idle(max_ticks=5)


def test_identity_body_echoes_first_bind_value():
    r = Registry()
    r.body("seed", lambda: "s")
    g = parse("[S]-->B\nS.body: seed", registry=r)  # B has no body -> identity
    run = Runner(g, r)
    run.run_until_idle(max_ticks=5)
    assert run.run_state.last_output("B") == "s"


def test_index_policy_flows_through_bind():
    r = Registry()
    r.body("sseed", lambda: "x")       # S: start, no producers -> arity 0 ok
    r.body("aseed", lambda s: "x")     # A: one producer -> arity must be 1
    seen = {}

    def two_b(a, b):
        seen["pair"] = (a, b)
        return "done"

    r.body("two_b", two_b)
    g = parse(
        "[S]-->A\nA-->C\nS-->C\nS.body: sseed\nA.body: aseed\n"
        "C.inputs: A, S[1]\nC.bind: [A, S]\nC.body: two_b",
        registry=r,
    )
    run = Runner(g, r)
    run.run_until_idle(max_ticks=5)
    # S[1] pins S's first fire; index policies flow through positional binds.
    assert seen["pair"] == ("x", "x")


def test_missing_survives_bind_resolution_end_to_end():
    r = Registry()
    seen = {}

    def seed_body():
        return "S"

    def loop_body(v):
        seen.setdefault("args", v.args)
        seen.setdefault("named", dict(v.named))
        seen.setdefault("input", v.input())
        return "A-out"

    def sink_body(x):
        return x

    r.body("seed_body", seed_body)
    r.body("loop_body", loop_body)
    r.body("sink_body", sink_body)
    g = parse(
        "[S]-->A\nA-->sink\nsink-->A\nA.join: OR\n"
        "A.bind: {prev: sink, seed: S}\nA.body: loop_body\nsink.body: sink_body",
        registry=r,
    )
    Runner(g, r).run_until_idle(max_ticks=6)
    # At A's first fire, back-edge producer `sink` has not fired yet: the
    # bound element MUST be Missing (not None) so hosts can fall back.
    assert seen["named"]["prev"] is Missing
    assert seen["named"]["seed"] == "S"
    assert seen["args"] == (Missing, "S")
    assert seen["input"] == (Missing, "S")
```

- [ ] **Step 2: 跑测试确认失败**

Run: `python -m pytest tests/test_dispatch.py -q`
Expected: FAIL — `TypeError: <lambda>() takes 0 positional arguments but 1 was given`（engine 仍把 view 传给值模式 body；`test_missing...` 因旧视图无 `.args` 报 AttributeError）

- [ ] **Step 3: 实现**

`tickflow/engine.py`：

1. import 区改为：

```python
from .ir import Graph, Failure, Node, InputPolicy
from .registry import Registry
from .state import NodeState, RunState, _jsonable
from .views import Resolved, NodeView, GuardView, _ReadOnlyStateView
```

2. `_resolve_inputs` 之后新增四个模块级函数（sync/async 共用的单点实现）：

```python
def bind_entries(node: Node) -> tuple[tuple[str | None, str], ...]:
    """The node's bind declaration: explicit entries, or auto-bind derived
    from the ``inputs`` key order (declaration order if the inputs were
    declared explicitly, alphabetical via the parser's producer auto-fill
    otherwise)."""
    if node.bind is not None:
        return node.bind.entries
    return tuple((None, prod) for prod in node.inputs)


def _prepare_fire(
    graph: Graph, node: str, run_state: RunState, t: int, registry: Registry,
) -> tuple[dict[str, Resolved], tuple[tuple[str | None, str], ...], tuple]:
    """Shared Phase-A prep for the sync and async engines: resolve declared
    inputs, derive bind entries, assemble the positional values tuple.

    Missing fidelity: elements pass ``run_state.resolve`` results through
    verbatim — a producer that has not fired yet contributes ``Missing``, and
    only a genuinely-fired ``None`` output yields None (loop / spec-fallback
    semantics downstream depend on this distinction)."""
    nobj = graph.nodes[node]
    resolved = _resolve_inputs(graph, node, run_state, t, registry)
    entries = bind_entries(nobj)
    values: list[Any] = []
    for _f, prod in entries:
        r = resolved.get(prod)
        if r is None:
            policy = nobj.inputs.get(prod) or InputPolicy.latest()
            r = Resolved(
                value=run_state.resolve(prod, policy.kind, policy.k, t), k=policy.k,
            )
            resolved[prod] = r
        values.append(r.value)
    values = tuple(values)
    if len(values) != len(entries):  # pragma: no cover — entries drive the loop
        raise RuntimeError(
            f"node {node!r}: assembled {len(values)} values for "
            f"{len(entries)} bind entries"
        )
    # Named field overlays for legacy by-name access (field wins over a
    # colliding producer key).
    for f, prod in entries:
        if f is not None:
            resolved[f] = resolved[prod]
    return resolved, entries, values


def prepare_body_call(
    registry: Registry,
    body_name: str,
    entries: tuple[tuple[str | None, str], ...],
    values: tuple,
    state_view: Any,
    resolved: dict[str, Resolved],
    node: str,
) -> tuple[Any, tuple, dict]:
    """Resolve the call shape for a node body from its registered signature:
    value mode (positional args / named kwargs / keyword-only ``state`` tail)
    or view mode (one NodeView). Returns ``(fn, args, kwargs)``."""
    body = registry.get_body(body_name)
    sig = registry.body_sig(body_name)
    is_named = any(f is not None for f, _ in entries)
    if sig.mode == "value":
        if is_named:
            kwargs = {f: v for (f, _), v in zip(entries, values)}
            if sig.wants_state:
                kwargs["state"] = state_view
            return body, (), kwargs
        if sig.wants_state:
            return body, values, {"state": state_view}
        return body, values, {}
    view = NodeView(
        node=node, fields=entries, values=values,
        state=state_view, resolved=resolved,
    )
    return body, (view,), {}
```

3. `tick()` 的 Phase A 循环体（`resolved = _resolve_inputs(...)` 到 `output = body(view)`）替换为：

```python
    for node in fireable:
        resolved, entries, values = _prepare_fire(graph, node, run_state, t, registry)
        initial_state = run_state.mutable_state(node)
        state_view = _NodeStateView(initial_state)
        body_name = graph.nodes[node].body
        if body_name is None:
            # Identity: echo the first bound value (None with no inputs).
            output = values[0] if values else None
        else:
            fn, args, kwargs = prepare_body_call(
                registry, body_name, entries, values, state_view, resolved, node,
            )
            output = fn(*args, **kwargs)
```

Phase A 其余（Failure 判定、NodeState 组装、record、slot 消费）不动。`registry.py` 的 `_identity_body` 与 `get_body(None)` 路径保留（API 兼容），但引擎对 `body is None` 已不再走它——在 `_identity_body` docstring 加一句 "The engine inlines identity handling; kept for direct callers."

- [ ] **Step 4: 跑测试确认通过 + 全量回归**

Run: `python -m pytest tests/test_dispatch.py tests -q`
Expected: 全部 PASS。重点确认：`tests/test_engine.py`、`test_loop.py`、`test_async.py`（async 引擎尚未改，仍构造 `DictView` 垫片 → 视图模式 body 照常工作）全绿。

- [ ] **Step 5: 提交**

```bash
git add tickflow/engine.py tests/test_dispatch.py
git commit -m "feat(engine): bind resolution + value/view-mode body dispatch (sync)"
```

---

### Task 6: engine（sync）— guard 裁决语义

**Files:**
- Modify: `tickflow/engine.py`
- Create: `tests/test_guard_view.py`

- [ ] **Step 1: 写失败测试**

```python
# tests/test_guard_view.py
"""Guard adjudication semantics: GuardView contents and value-mode guards."""
import pytest

from tickflow import Registry, Runner, parse


def test_guard_output_is_current_tick_output():
    r = Registry()
    seen = []

    def bump(v):
        n = v.state.get("n", 0) + 1
        v.state["n"] = n
        return n

    r.body("bump", bump)
    r.body("echo", lambda x: x)

    def watch(output):
        seen.append(output)
        return output < 2

    r.guard("watch", watch)
    g = parse(
        "[S]-->B\nB--|watch|-->C\nC-->B\nB.join: OR\nC.body: echo", registry=r
    )
    Runner(g, r).run_until_idle(max_ticks=10)
    assert seen == [1, 2]  # adjudicated output, this tick — not latest_before


def test_guard_view_sees_src_declared_inputs():
    r = Registry()
    seen = {}
    # arity 1: M's auto-bind has one entry (S); zero-arity would fail E1.
    r.body("mbody", lambda s: "M-out")

    def rbody(d):
        return {"got": d}

    r.body("rbody", rbody)

    def watch(v):
        seen["output"] = v.output
        seen["named"] = dict(v.named)
        with pytest.warns(DeprecationWarning):
            seen["legacy_src"] = v["R"].value
            seen["legacy_prod"] = v["M"].value
        with pytest.raises(KeyError):
            v["NotDeclared"]  # P4: undeclared nodes are unreachable
        return False

    r.guard("watch", watch)
    g = parse(
        "[S]-->M\nM-->R\nR--|watch|-->X\nR.bind: {d: M}\nR.body: rbody\nM.body: mbody",
        registry=r,
    )
    run = Runner(g, r)
    run.run_until_idle(max_ticks=5)
    assert seen["output"] == {"got": "M-out"}
    assert seen["named"] == {"d": "M-out"}
    assert seen["legacy_src"] == {"got": "M-out"}
    assert seen["legacy_prod"] == "M-out"


def test_guard_view_mode_gets_guard_view_state_readonly():
    r = Registry()
    r.body("b1", lambda v: "x")

    def watch(v):
        assert v.state.get("n", 0) == 0
        try:
            v.state["n"] = 1
            raised = False
        except TypeError:
            raised = True
        assert raised, "guard state must be read-only"
        return False

    r.guard("watch", watch)
    g = parse("[S]-->B\nB--|watch|-->C\nB.body: b1", registry=r)
    Runner(g, r).run_until_idle(max_ticks=3)


def test_guard_value_mode_receives_output_only():
    r = Registry()
    # arity 1: B's auto-bind has one entry (S); a zero-arity body would fail E1.
    r.body("b1", lambda s: 5)
    seen = []

    def watch(output):
        seen.append(output)
        return False

    r.guard("watch", watch)
    g = parse("[S]-->B\nB--|watch|-->C\nB.body: b1", registry=r)
    Runner(g, r).run_until_idle(max_ticks=3)
    assert seen == [5]
```

- [ ] **Step 2: 跑测试确认失败**

Run: `python -m pytest tests/test_guard_view.py -q`
Expected: FAIL（值模式 guard 被传入 DictView；`test_guard_view_sees_src_declared_inputs` 中 `v["NotDeclared"]` 今天不 raise——这正是 P4）

- [ ] **Step 3: 实现**

`tickflow/engine.py`：

1. `_guard_view` **之前**新增（本任务保留旧 `_guard_view` 函数不删——async_runner 仍 import 它；Task 7 删）：

```python
def _guard_node_view(
    graph: Graph,
    src: str,
    src_output: Any,
    run_state: RunState,
    t: int,
) -> GuardView:
    """Build the adjudication view for a guard on ``src--|g|-->dst``: the
    firing node's current-tick output plus that node's bind-declared inputs,
    resolved with the same policies the body just consumed. Nothing else is
    reachable — guards cannot read undeclared nodes."""
    src_node = graph.nodes[src]
    entries = bind_entries(src_node)
    resolved: dict[str, Resolved] = {}
    values: list[Any] = []
    for f, prod in entries:
        policy = src_node.inputs.get(prod) or InputPolicy.latest()
        r = Resolved(
            value=run_state.resolve(prod, policy.kind, policy.k, t), k=policy.k,
        )
        resolved[prod] = r
        if f is not None:
            resolved[f] = r
        values.append(r.value)
    # The src name maps to the output being adjudicated; it wins over any
    # colliding key so legacy ``view[src]`` keeps meaning "current output".
    resolved[src] = Resolved(value=src_output, k=None)
    return GuardView(
        src=src, output=src_output, fields=entries,
        values=tuple(values),
        state=_ReadOnlyStateView(run_state.mutable_state(src)),
        resolved=resolved,
    )


def prepare_guard_call(
    registry: Registry,
    guard_name: str,
    graph: Graph,
    src: str,
    src_output: Any,
    run_state: RunState,
    t: int,
) -> tuple[Any, tuple, dict]:
    """Resolve the guard call shape for edge ``src--|guard|-->dst``
    (sync + async shared)."""
    guard = registry.get_guard(guard_name)
    sig = registry.guard_sig(guard_name)
    if sig.mode == "value":
        return guard, (src_output,), {}
    return guard, (_guard_node_view(graph, src, src_output, run_state, t),), {}
```

2. `tick()` Phase B 中 guard 求值分支替换：

```python
            elif e.guard is None:
                v = True
            else:
                gfn, gargs, gkwargs = prepare_guard_call(
                    registry, e.guard, graph, e.src, f.output, run_state, t,
                )
                v = bool(gfn(*gargs, **gkwargs))
```

3. `_guard_view` 函数体首行加弃用注释：`# Legacy all-nodes view; only the (unmigrated) async engine still uses it. Removed in the next task.`

- [ ] **Step 4: 跑测试确认通过 + 全量回归**

Run: `python -m pytest tests/test_guard_view.py tests -q`
Expected: 全部 PASS（存量 guard 均为 `def g(v)` 视图模式 → GuardView legacy 访问路径）

- [ ] **Step 5: 提交**

```bash
git add tickflow/engine.py tests/test_guard_view.py
git commit -m "feat(engine): GuardView adjudication semantics (src output + declared inputs)"
```

---

### Task 7: async 引擎镜像 + 删除 `_guard_view`

**Files:**
- Modify: `tickflow/async_runner.py`
- Modify: `tickflow/engine.py`（删 `_guard_view`）
- Create: `tests/test_async_bind.py`

- [ ] **Step 1: 写失败测试**

```python
# tests/test_async_bind.py
"""Async mirror of the bind dispatch: value/view modes, guards, Missing."""
import asyncio

from tickflow import Registry, parse
from tickflow.async_runner import AsyncRunner
from tickflow.views import Missing


def _run(coro):
    return asyncio.run(coro)


def test_async_value_mode_positional_async_body():
    r = Registry()
    r.body("seed", lambda: "s")

    async def incr(a):
        return a + "!"

    r.body("incr", incr)
    g = parse("[S]-->B\nS.body: seed\nB.body: incr", registry=r)
    rn = AsyncRunner(g, r)
    _run(rn.run_until_idle(max_ticks=5))
    assert rn.run_state.last_output("B") == "s!"


def test_async_value_mode_named_and_state():
    r = Registry()
    r.body("s1", lambda: 1)

    async def add1(a, *, state):
        state["n"] = state.get("n", 0) + 1
        return a + state["n"]

    r.body("add1", add1)
    g = parse("[S]-->A\nS.body: s1\nA.bind: {a: S}\nA.body: add1", registry=r)
    rn = AsyncRunner(g, r)
    _run(rn.run_until_idle(max_ticks=5))
    assert rn.run_state.last_output("A") == 2


def test_async_view_mode_and_missing():
    r = Registry()
    seen = {}

    async def loop_body(v):
        seen["args"] = v.args
        return "out"

    async def sink_body(x):
        return x

    r.body("seed_body", lambda: "S")
    r.body("loop_body", loop_body)
    r.body("sink_body", sink_body)
    g = parse(
        "[S]-->A\nA-->sink\nsink-->A\nA.join: OR\n"
        "A.bind: {prev: sink, seed: S}\nA.body: loop_body\nsink.body: sink_body",
        registry=r,
    )
    rn = AsyncRunner(g, r)
    _run(rn.run_until_idle(max_ticks=6))
    assert seen["args"] == (Missing, "S")


def test_async_guard_both_modes():
    r = Registry()
    # arity 1: B/D's auto-bind has one entry each; zero-arity would fail E1.
    r.body("b1", lambda s: 5)
    value_seen, view_seen = [], []

    def watch_value(output):
        value_seen.append(output)
        return False

    async def watch_view(v):
        view_seen.append(v.output)
        return False

    r.guard("watch_value", watch_value)
    r.guard("watch_view", watch_view)
    g = parse(
        "[S]-->B\nB--|watch_value|-->C\nB.body: b1\n"
        "[T]-->D\nD--|watch_view|-->E\nD.body: b1",
        registry=r,
    )
    rn = AsyncRunner(g, r)
    _run(rn.run_until_idle(max_ticks=5))
    assert value_seen == [5]
    assert view_seen == [5]
```

- [ ] **Step 2: 跑测试确认失败**

Run: `python -m pytest tests/test_async_bind.py -q`
Expected: FAIL（async 引擎仍把 view 传给值模式/具名 body；`watch_value` 收到的是 view）

- [ ] **Step 3: 实现**

`tickflow/async_runner.py`：

1. engine import 行改为：

```python
from .engine import (
    Marking, bootstrap, _join_satisfied, _prepare_fire,
    prepare_body_call, prepare_guard_call, _NodeStateView,
)
```

（删除 `_resolve_inputs`、`_guard_view` 的 import；`from .views import DictView` 一行删除。）

2. `async_tick` 的 `_fire` 内部替换为：

```python
    async def _fire(node: str) -> NodeState:
        resolved, entries, values = _prepare_fire(graph, node, run_state, t, registry)
        initial_state = run_state.mutable_state(node)
        state_view = _NodeStateView(initial_state)
        body_name = graph.nodes[node].body
        if body_name is None:
            output = values[0] if values else None
        else:
            fn, args, kwargs = prepare_body_call(
                registry, body_name, entries, values, state_view, resolved, node,
            )
            output = await _maybe_await(fn, *args, **kwargs)
        is_fail = isinstance(output, Failure)
        status: Literal["ok", "failed", "aborted"] = "ok"
        error: str | None = None
        if is_fail:
            error = output.error
            status = "aborted" if output.type == "infrastructure" else "failed"
        return NodeState(
            tick=t, node=node,
            inputs={k: v.value for k, v in resolved.items()},
            output=output, edges_fired=[],
            status=status, error=error,
            mutable_state=initial_state,
        )
```

3. `_produce` 的 guard 分支替换为：

```python
            elif e.guard is None:
                v = True
            else:
                gfn, gargs, gkwargs = prepare_guard_call(
                    registry, e.guard, graph, e.src, f.output, run_state, t,
                )
                v = bool(await _maybe_await(gfn, *gargs, **gkwargs))
```

4. `tickflow/engine.py`：删除整个 `_guard_view` 函数。

- [ ] **Step 4: 跑测试确认通过 + 全量回归**

Run: `python -m pytest tests/test_async_bind.py tests -q`
Expected: 全部 PASS

- [ ] **Step 5: 提交**

```bash
git add tickflow/async_runner.py tickflow/engine.py tests/test_async_bind.py
git commit -m "feat(async): mirror bind dispatch; drop legacy _guard_view"
```

---

### Task 8: Runner 构建期校验 E1–E3 / W1

**Files:**
- Modify: `tickflow/runner.py`
- Create: `tests/test_bind_validation.py`

- [ ] **Step 1: 写失败测试**

```python
# tests/test_bind_validation.py
"""Build-time validation: bind <-> signature consistency (E1-E3) + W1 warning."""
import warnings

import pytest

from tickflow import Registry, Runner, parse


def _r():
    r = Registry()
    r.body("one", lambda a: a)
    r.body("two", lambda a, b: a + b)
    return r


def test_e1_arity_mismatch_raises_at_construction():
    r = _r()
    g = parse("[A]-->C\n[B]-->C\nC.bind: [A, B]\nC.body: one", registry=r)
    with pytest.raises(ValueError, match="expects 1 parameter"):
        Runner(g, r)


def test_e1_ok_when_arity_matches():
    r = _r()
    g = parse("[A]-->C\n[B]-->C\nC.bind: [A, B]\nC.body: two", registry=r)
    Runner(g, r)  # no raise


def test_e2_named_fields_must_match_params():
    r = Registry()
    r.body("named", lambda x, y: x)
    g = parse("[A]-->C\n[B]-->C\nC.bind: {a: A, b: B}\nC.body: named", registry=r)
    with pytest.raises(ValueError, match="do not match named bind fields"):
        Runner(g, r)


def test_e2_ok_when_names_match():
    r = Registry()
    r.body("named", lambda a, b: a + b)
    g = parse("[A]-->C\n[B]-->C\nC.bind: {a: A, b: B}\nC.body: named", registry=r)
    Runner(g, r)


def test_e3_guard_arity_must_be_one():
    r = Registry()
    r.body("b1", lambda v: None)
    r.guard("g2", lambda a, b: True)
    g = parse("[A]-->B\nB--|g2|-->C\nB.body: b1\nA.body: b1", registry=r)
    with pytest.raises(ValueError, match="exactly 1 parameter"):
        Runner(g, r)


def test_w1_ambiguous_auto_bind_warns():
    r = Registry()
    r.body("pos", lambda a, b: a + b)
    g = parse("[A]-->C\n[B]-->C\nC.body: pos", registry=r)
    with pytest.warns(UserWarning, match="auto-bind"):
        Runner(g, r)


def test_explicit_bind_no_w1():
    r = Registry()
    r.body("pos", lambda a, b: a + b)
    g = parse("[A]-->C\n[B]-->C\nC.bind: [B, A]\nC.body: pos", registry=r)
    with warnings.catch_warnings(record=True) as rec:
        warnings.simplefilter("always")
        Runner(g, r)
    assert not any("auto-bind" in str(x.message) for x in rec)


def test_view_mode_body_no_w1_and_no_arity_check():
    r = Registry()
    r.body("viewer", lambda v: "x")
    g = parse("[A]-->C\n[B]-->C\nC.body: viewer", registry=r)
    with warnings.catch_warnings(record=True) as rec:
        warnings.simplefilter("always")
        Runner(g, r)
    assert not any("auto-bind" in str(x.message) for x in rec)
```

- [ ] **Step 2: 跑测试确认失败**

Run: `python -m pytest tests/test_bind_validation.py -q`
Expected: FAIL（校验尚不存在，构造不报错/不告警）

- [ ] **Step 3: 实现**

`tickflow/runner.py`：

1. 顶部 `from .engine import ...` 行追加 `bind_entries`（engine 已被 runner import）；顶部 import 补 `import warnings`（若无）。
2. `_validate_registry_for_graph` 整体替换为：

```python
def _validate_registry_for_graph(graph: Graph, registry: Registry) -> None:
    """Raise ValueError if ``registry`` is missing any body or guard name
    referenced by ``graph``, or if any signature contradicts the graph's bind
    declarations (E1 arity / E2 named fields / E3 guard arity). Warn (W1) on
    multi-producer auto-binds consumed by positional value-mode bodies."""
    missing: list[str] = []
    for node in graph.nodes.values():
        if node.body is not None and not registry.has_body(node.body):
            missing.append(f"body {node.body!r} (required by node {node.name!r})")
    for edge in graph.edges:
        if edge.guard is not None and not registry.has_guard(edge.guard):
            missing.append(f"guard {edge.guard!r} (required by edge {edge.src}-->{edge.dst})")
    if missing:
        raise ValueError(
            "registry missing required entries:\n  " + "\n  ".join(missing)
        )
    _validate_bind_signatures(graph, registry)


def _validate_bind_signatures(graph: Graph, registry: Registry) -> None:
    for node in graph.nodes.values():
        if node.body is None:
            continue
        sig = registry.body_sig(node.body)
        entries = bind_entries(node)
        is_named = any(f is not None for f, _ in entries)
        fields = tuple(f for f, _ in entries if f is not None)
        if sig.mode == "value":
            if is_named:
                if set(sig.param_names) != set(fields):
                    raise ValueError(
                        f"node {node.name!r}: body {node.body!r} parameters "
                        f"{list(sig.param_names)} do not match named bind fields "
                        f"{list(fields)}"
                    )
            else:
                if sig.arity != len(entries):
                    raise ValueError(
                        f"node {node.name!r}: body {node.body!r} expects "
                        f"{sig.arity} parameter(s), bind declares "
                        f"{len(entries)} input(s)"
                    )
                if node.bind is None and len(entries) >= 2:
                    warnings.warn(
                        f"node {node.name!r} relies on auto-bind for "
                        f"{len(entries)} producers; positional order is the "
                        f"inputs key order — declare {node.name}.bind: [...] "
                        f"to pin it against renames",
                        UserWarning,
                        stacklevel=3,
                    )
    for edge in graph.edges:
        if edge.guard is None:
            continue
        gs = registry.guard_sig(edge.guard)
        if gs.mode == "value" and gs.arity != 1:
            raise ValueError(
                f"guard {edge.guard!r} (edge {edge.src}-->{edge.dst}) must take "
                f"exactly 1 parameter (the adjudicated output); got {gs.arity}"
            )
```

3. `tickflow/runner.py` 的 `_BaseRunner.__init__`（`runner.py:186` 起，`self.registry = ...` 赋值之后、`self.marking = bootstrap(graph)` 之前）加：

```python
        self._validate_registry(self.registry)
```

（`Runner` 与 `AsyncRunner` 都经此构造，E1–E3/W1 对两个引擎同时生效。若全量回归中有测试因"先构造、后注册"失败，修正该测试为先注册再构造——解析器文档本就要求 prefer registering first。）

- [ ] **Step 4: 跑测试确认通过 + 全量回归**

Run: `python -m pytest tests/test_bind_validation.py tests -q`
Expected: 全部 PASS（如出现存量失败，按 Step 3 的注记处理并在提交信息中说明）

- [ ] **Step 5: 提交**

```bash
git add tickflow/runner.py tests/test_bind_validation.py
git commit -m "feat(runner): build-time bind/signature validation (E1-E3) + W1 auto-bind warning"
```

---

### Task 9: 导出、文档与端到端验证

**Files:**
- Modify: `tickflow/__init__.py`
- Modify: `tickflow/views.py`、`tickflow/engine.py`、`tickflow/registry.py`（模块 docstring 更新）

- [ ] **Step 1: `tickflow/__init__.py` 导出**

import 区追加：

```python
from .ir import Bind
from .views import NodeView, GuardView, DictView, Missing
```

（`Missing` 此前从 `tickflow.views` 提供，顶层也一并导出。）`__all__` 追加 `"Bind"`, `"NodeView"`, `"GuardView"`, `"DictView"`, `"Missing"`。

- [ ] **Step 2: docstring 收口**

- `views.py` 模块 docstring：改为描述 NodeView/GuardView 消费 API 与 Missing 三态表（无输入 → `input()` 返回 None；已声明未点火 → Missing；真产出 None → None），legacy 访问标注弃用。
- `engine.py` 模块 docstring：补一段 "Bodies and guards are dispatched by registration-time signature: value mode (plain parameters, optional keyword-only `state` tail) or view mode (one NodeView/GuardView)."
- `registry.py` 模块 docstring：Callables 区改为双模式描述 + 判定规则（单位置参数且名为 v/view 或注解 NodeView/DictView → 视图模式）。

- [ ] **Step 3: 全量测试**

Run: `python -m pytest tests -q`
Expected: 全部 PASS（含 Task 0 基线的 186 个 + 新增）

- [ ] **Step 4: 跑全部示例（legacy 活体回归）**

```bash
for f in examples/*_beh.py; do echo "== $f"; python "$f" || echo "FAILED: $f"; done
```

Expected: 无 `FAILED` 行、无 Traceback。examples 的 body 全是 `def f(v)` 视图模式 + legacy 访问（含 `v.items()`、`v.A.value`），应仅产生 DeprecationWarning 照常运行。

- [ ] **Step 5: 提交**

```bash
git add tickflow/__init__.py tickflow/views.py tickflow/engine.py tickflow/registry.py
git commit -m "feat: export Bind/NodeView/GuardView; docstrings for the bind-era API"
```

---

## 明确不做（与设计 §8 对齐）

- 不合并 `inputs` 与 `bind`；不做 `BodyResult`；guard 不支持独立 bind/index pin；mermaid 不渲染 bind。
- 不动 README（工作区有用户未提交改动）；不动 SpecModule（下游侧迁移是独立计划，见设计 §5）。
