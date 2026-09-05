# bind 迁移综合设计（tickflow 上游 × SpecModule 下游）

日期：2026-09-05
输入：[change.md](change.md)（DictView 绑定耦合问题与位置参数方案）+ SpecModule 侧三条需求（Missing 保真、名字↔位置契约、0/1/N 多态与兼容窗口）与两个前置决策委托。

---

## 0. 结论速览

| 事项 | 决策 |
| --- | --- |
| 前置决策 1：auto-fill 去留 | **保留**。bind 缺省 = `node.inputs` 键序；多生产者 + 位置式 body 时告警建议显式 bind |
| 前置决策 2：guard 语义 | guard 是**裁决者不是消费者**：`GuardView` = 触发节点当前 tick 输出 + 该节点声明输入（body 同款解析）+ 只读 state；**没有 `.input()`** |
| body API | **双模式分发**：`def f(a, c)` 值模式（真参数，change.md 的 P7 目标）/ `def f(v)` 视图模式（SpecModule 的 state/node 依赖），按签名自动判定 |
| bind 形态 | **位置式 `[A, C]` + 具名式 `{outline: A}` 双形态**；具名形态让 SpecModule 的 zip 难点直接消失（下游建议采纳） |
| Missing（需求 1，correctness 级） | bind 解析出的值**逐元素原样传递**，Missing 永不坍缩为 None；`v.input()` arity=0 才返回 None（语义="无输入可消费"） |
| 0/1/N 多态（需求 3） | `v.input()` 返回 `None \| 值 \| tuple`；另提供 `v.args`（恒为 tuple）作为免 shim 通道 |
| 兼容窗口 | `v.<name>` / `v[name]` / `v.inputs()` / `.k` 保留一个过渡版本 + `DeprecationWarning`；SpecModule 一次切过去 |

---

## 1. 两份输入的张力，以及本文的取舍

change.md 的理想终点是 `body(*values)`——真参数、IDE 可补全（P7）。SpecModule 的现实是：harness 闭包深度依赖 `view.state`（审计链 `_prompt`/`_llm_raw`/`_usage`、循环计数 `attempt`）、`view.node`（事件溯源），且它的输入天然是**字段名寻址**（`task.inputs: {field: producer}`，prompt 占位符 `{field}`）。纯位置参数会迫使下游重造 state 通道并做 zip 对齐。

**取舍：值模式满足 change.md 的全部 P 问题，视图模式作为一等公民长期保留（非仅过渡）。** 二者共享同一套 bind 声明与解析，只是消费形态不同。理由：

1. P1–P6 的根因是"绑定信息隐含在 body 的按名访问里"。只要 bind 成为图声明、按名访问退化为具名 bind 的消费形态（字段名是函数局部词汇，不是图拓扑名），P1/P2 即消除——**具名 bind 对 P1/P2 的解决能力与位置参数等价**。
2. P7（IDE）只有值模式能给；而 SpecModule 这类"引擎宿主"场景 body 是程序生成的闭包，IDE 无意义、state/node 是硬需求。两类用户都真实存在，API 应同时成立。
3. 现存所有 `def f(v)` 签名自动落入视图模式，升级零破坏；新代码写值模式获得全部静态收益。

---

## 2. 两个前置决策（先定，下游依赖）

### 2.1 auto-fill：保留

**决策：保留自动补全。** 无显式 `bind` 的节点，bind 自动派生：

```
bind 缺省 = [(None, prod) for prod in node.inputs 的键序]
```

- `inputs` 显式声明过 → 声明序；否则沿用现有 auto-fill（`g.producers()` 字母序）。
- 单生产者节点零声明（change.md §5.3 语义不变）；多生产者节点零声明时**顺序有定义但发警告**（仅当 body 为值模式位置式时，见 §4.5 W1）——纯视图模式/具名消费不受顺序影响，不告警。

**为什么不移除**：SpecModule 的 flow 文本不含任何 inputs/bind 声明（`graph_builder.py:80` 注释明确"no body/input declarations"），现有的 inputs auto-fill 是它的隐式依赖；bind 沿用同一机制则下游**零额外工作**。移除方案（graph_builder 从边合成 bind）也可行但纯增工作量，且把"边即数据通道"这一既有语义砍掉了。SpecModule 唯一写 bind 的地方是具名形式（§5.2），那是为了字段名，不是顺序。

### 2.2 guard 语义：裁决视图，无 `.input()`

guard 不消费输入，它裁决的是**待发输出**。定义：

```
GuardView（边 src--|g|-->dst 的 guard g 收到）：
  .output         src 本 tick 刚产出的输出（与今天 f.output 一致，非 latest_before）
  .args / .named  src 节点 bind 声明的输入，按 body 同款 policy 解析（即 body 消费的那份值）
  .state          src 的可变状态（body 写入后的只读视图，与今天一致）
  无 .input()     —— 类型层面表达"guard 不是消费者"
```

**为什么给 guard 看 src 的声明输入**：这不是偷读的复古。`fact_review_loop.has_issues` 需要读 `Merge` 的 `attempt`，而 Merge 恰是 Review 的声明生产者（边 `Merge --> Review`，`Review.inputs` 含它）。今天 `_guard_view` 全节点遍历给它的值（latest_before(t)）与 Review body 消费的值**本就是同一份解析**；新模型只是把"碰巧相等"变成"定义相等"——guard 裁决所依据的数据与 body 消费的数据强一致（含 index policy 场景，比今天更正确）。

**P4 的消除方式**：值模式 guard `def g(output)` 只见输出，结构上无法偷读；视图模式 GuardView 的键空间 = {src 名 → 当前输出} ∪ {bind 字段/生产者名 → 声明输入}，访问声明之外的名字 `KeyError`——从"静默给值"变为"显式拒绝"。

**下游迁移判定**：已审计 SpecModule 全部 guard（`fact_review_loop.has_issues/clean`、`academic_writer._make_loop_guards` 闭包对）：读取面均为「触发节点 + 其声明生产者」，**全部落在 GuardView 键空间内，升级后原样可跑**（deprecation 警告下）。`submodule.py:128` 的 `reg.guard(gname, gfn)` 重注册机制无需任何改动。无"没有迁移目标"的 guard。

---

## 3. 方案对比

| | A. 纯位置参数（change.md 原案） | B. 纯视图 + `v.input()` | **C. 双模式 + 双形态 bind（选定）** |
| --- | --- | --- | --- |
| P1/P2 解耦拓扑名 | ✅ | ✅（具名 bind 时） | ✅（两形态皆可） |
| P7 IDE 真参数 | ✅ | ❌ | ✅（值模式） |
| SpecModule state/node | ❌ 需重造通道（尾参/ContextVar） | ✅ | ✅（视图模式保留） |
| SpecModule zip 难点 | ❌ 需 shim + 顺序契约 | ⚠️ shim 可关 | ✅ 具名 bind 直接消失 |
| 现存代码升级 | ❌ 全量改签名 | ✅ | ✅（`def f(v)` 自动视图模式） |
| guard 迁移目标 | 只见输出，SpecModule guard 断供 | 含糊 | ✅ GuardView 明确定义 |

方案 A 是 change.md 的原始主张，被否的点是它把 P7 的收益建立在对 SpecModule 真实依赖（state/事件/字段名）的忽视上；方案 B 对手写小图不够好（P7 落空）。C 的成本是签名分发的一层约定（§4.4），收益是两类用户各得其所。

---

## 4. 详细设计（tickflow 侧）

### 4.1 IR：`Node.bind`

```python
@dataclass(frozen=True)
class Bind:
    """归一化的 bind 声明：有序 (field, producer) 条目。
    field 为 None 表示位置式匿名条目；全部为 None = 位置式，
    全部非 None = 具名式（不允许混用，parse 期拒绝）。"""
    entries: tuple[tuple[str | None, str], ...]

    @classmethod
    def positional(cls, producers: Sequence[str]) -> "Bind": ...
    @classmethod
    def named(cls, mapping: Mapping[str, str]) -> "Bind": ...
    @property
    def fields(self) -> tuple[str, ...]: ...        # 具名式的字段序
    @property
    def producers(self) -> tuple[str, ...]: ...     # 消费序的生产者名
```

`Node` 增加 `bind: Bind | None = None`（None = auto-fill，见 §2.1）。与 `inputs` 的关系：

- `inputs` 不动，仍是 producer → `InputPolicy` 的解析策略表（控制流与数据策略解耦的现状保留）。
- 校验不变式：`set(bind.producers) ⊆ set(inputs) ∪ producers(node)`；bind 引用的 producer 若不在 `inputs` 中，自动补 `InputPolicy.latest()`（与现有 inputs auto-fill 同向）。
- `Graph.copy()` / `to_dict()` 带上 bind；`to_mermaid()` 不渲染（拓扑图不承载绑定，避免视觉噪音）；快照不含结构（现状），`remap_graph` 换图时经 parse 期校验兜底。

### 4.2 DSL 语法

```
B.bind: [A, C]              # 位置式：第 1 参 ← A，第 2 参 ← C
B.bind: {outline: A}        # 具名式：字段 outline ← A
B.bind: A                   # 单值糖，等价 [A]
```

- parser 新增 `_BIND_RE`；具名式用小 tokenizer 解析（容忍空白），字段名字符集与节点名相同 `[A-Za-z_][A-Za-z0-9_]*`。
- 位置/具名混用、重复字段、未知语法 → `ParseError`（带行号）。
- 程序化构造（SpecModule）直接赋 `node.bind = Bind.named({...})`，不走文本。

### 4.3 引擎：解析与消费

Phase A 中 `_resolve_inputs` 之后新增一步：

```python
entries = bind_entries(graph.nodes[node])          # 显式 bind 或 auto-fill
values  = tuple(run_state.resolve(p, policy.kind, policy.k, t)
                for p in entries.producers)        # ← Missing 原样进元组，任何路径不坍缩
```

调用分发（sync/async 共用这一层，async 侧再套 `_maybe_await`）：

```python
sig = registry.body_sig(node.body)                 # 注册时 inspect 一次（§4.4）
if sig.mode == "value":
    if bind 是具名式:  output = body(**{f: v for f, v in zip(entries.fields, values)}, state=state_view)  # wants_state 时
    else:              output = body(*values, state=state_view)
else:  # view 模式
    output = body(NodeView(node=node, fields=entries, values=values, state=state_view,
                           resolved=resolved))     # resolved 供 legacy 按名访问
```

`NodeView`（`views.py`，`DictView` 保留为弃用别名，`Missing` 导出不动）：

```python
class NodeView:
    node: str
    state: _NodeStateView                # 读写代理，语义不变
    def input(self) -> Any               # arity 0 → None；1 → 值（可为 Missing）；N → tuple
    @property args -> tuple[Any, ...]    # 恒为 tuple；Missing 保真；免 shim 通道
    @property named -> dict[str, Any]    # 具名式：{字段: 值}（未点火字段值为 Missing）；位置式：{}
    def field(self, name) -> Any         # 具名查值；位置式 bind 上调用 → TypeError（提示改具名）
    # —— 过渡期 legacy（全部 DeprecationWarning）——
    __getattr__ / __getitem__            # 键空间 = 字段名 ∪ inputs 全部键（字段优先）
    inputs() / items()                   # 今天的生产者→值 dict 语义
    v.A.k                                # 解析策略元数据，仅 legacy 暴露
```

identity body（`body: None`）：取 bind 首值，无输入时 `None`，语义与今天对齐。

### 4.4 注册与签名分发

```python
@dataclass
class _Sig:
    mode: Literal["value", "view"]
    arity: int | None              # 值模式：位置参数个数（不含 keyword-only 的 state）
    param_names: tuple[str, ...]   # 值模式具名校验用
    wants_state: bool              # 值模式：存在 keyword-only 参数 state（change.md §5.5 方案 A）
    is_async: bool
```

**判定规则**（注册时 `inspect.signature` 一次，存入 Registry）：

- 恰有一个位置参数，且参数名 ∈ {`v`, `view`} **或** 注解为 `NodeView`/`DictView` → **视图模式**。
- 其余一切签名 → **值模式**。

现存代码全部 `def f(v)` / `def body(view)` / `def has_issues(view)`——自动视图模式，升级即兼容。change.md §6.3 的"过渡三阶段"由此收敛为：值模式与视图模式**长期并存**（前者面向手写图，后者面向引擎宿主），废弃的只是**按名访问输入**这一种消费形态，不是视图对象本身。状态传递采用 §5.5 方案 A（keyword-only 尾参 `state`），否决方案 B（`BodyResult`）：双返回形态会波及 Failure 判定与 async 镜像，收益不抵。

### 4.5 静态验证（P3 落地 + 下游要的个数断言）

| 级别 | 检查 | 时机 |
| --- | --- | --- |
| ParseError | bind 语法、混用、未知 producer、不可达 producer（复用 inputs 的 upstream 规则） | `parse()` |
| E1（硬错误） | 值模式 + 位置式：`arity(+wants_state?) == len(bind)` —— 即下游要求的"值个数 == 字段名个数"断言，前移到构建期 | Runner 构造（`_validate_registry_for_graph` 扩展），SpecModule 侧经 TasklistValidator→Runner 提前引爆 |
| E2（硬错误） | 值模式 + 具名式：`set(param_names) == set(fields)`（state 参数除外）；调用走 `**fields`，参数顺序无关 | 同上 |
| E3（硬错误） | 值模式 guard arity 必须 == 1 | 同上 |
| W1（警告） | 多生产者节点 auto-bind 且 body 为值模式位置式：位置序实为字母序，建议显式 bind（改名会静默换序——正是下游需求 2 指出的隐式契约风险） | Runner 构造 |
| 运行时断言 | 引擎组装 `values` 后 `len(values) == len(entries)` 防御性断言（单一出口点，成本 O(1)） | 每次 fire |

视图模式无法静态校验（无签名契约），由运行时 `KeyError`/`TypeError` 显式失败——与今天 `AttributeError` 相比，失败面从"任意名字"收窄到"声明之外的名字"。

### 4.6 Missing 契约（下游需求 1，correctness 级）

1. `values` 元组逐元素等于 `run_state.resolve(...)` 的返回值；**任何一层（bind 解析、NodeView、GuardView、`v.input()`、`v.args`、`v.named`、审计 `NodeState.inputs`）不得把 Missing 转 None**。
2. 语义区分表（写进 docstring 与文档）：

| 情形 | 值 | 含义 |
| --- | --- | --- |
| 节点无任何 bind 输入 | `v.input()` → `None` | "没有输入可消费" |
| 输入已声明、生产者尚未点火 | 元素值 = `Missing` | "声明了但还没来"——harness 据此走 spec 兜底（`harness.py:89` 的 `val is not Missing` 分支原样保真） |
| 生产者真的产出 None | 元素值 = `None` | 与上者可区分（LLM JSON 输出 null 是日常） |

3. 专项测试：循环起点未点火场景（`[seed]-->A`，A latest 未命中）断言 `args[0] is Missing`、`named["x"] is Missing`、`v.input() is Missing`（arity=1）；arity=0 断言 `v.input() is None`。

### 4.7 兼容窗口（下游需求 3）

- **本期（tickflow 0.x）**：新 API 全量上线；legacy 按名访问（`v.<producer>` / `v[producer]` / `v.inputs()` / `v.items()` / `.k`）保留并 `DeprecationWarning`（`stacklevel=2`，每访问点告警）；`DictView` 名称保留为 `NodeView` 别名（SpecModule `from tickflow.views import DictView` 不炸）。
- **过渡期**：SpecModule 一次切换（§5）；期间双轨由同一套 bind 解析供数，无行为分叉。
- **移除期**（tickflow 1.0）：删 legacy 访问与 `DictView` 别名。
- SpecModule 的 shim 建议：因 `v.args` 提供恒 tuple 通道，shim 可简化为一行 `vals = v.args`；若坚持 `v.input()`，shim `None→() / 值→(v,)` 亦成立。两条路都在设计内。

### 4.8 async 镜像

`async_runner.async_tick` 与 sync `tick` 共享：bind 解析 helpers、`NodeView`/`GuardView` 构造、`_Sig` 分发（仅最外层套 `_maybe_await`）。**禁止两处手写解析逻辑**——抽 `engine._bind_values(graph, node, run_state, t)` 单点实现，防语义漂移。

### 4.9 其余影响面

- `registry.py`：`Body`/`Guard` 类型注解改为 `Callable[..., Any]`；新增 `_sig` 缓存与 `body_sig()/guard_sig()`。
- `state.py`：不动（resolve 的 Missing 语义已是正确源头）。
- `checker.py`：不动（deadlock/SCC 与绑定无关）。
- README / 文档：body/guard 契约、bind 语法、Missing 表、双模式判定规则。

---

## 5. SpecModule 迁移方案（下游侧，一次切换）

### 5.1 `graph_builder.py`

步骤 5（L100–107）改为：非常量 `task.inputs` 写**具名 bind** + 补 inputs 策略：

```python
named = {}
for field_name, producer in task.inputs.items():
    if _is_constant_ref(producer):
        continue                     # 常量仍走 spec_inputs（_register_harness 不变）
    named[field_name] = producer
    graph.nodes[key].inputs.setdefault(producer, InputPolicy.latest())
graph.nodes[key].bind = Bind.named(named) if named else None
```

不再需要往 inputs 塞 field/producer 两个 key 的兼容技巧（原 L105–107）。字段序即 task.inputs 声明序，名字↔位置契约由具名形态整体消解。

### 5.2 `core/harness.py`

- alias 合并循环（L81–90）整体删除：`{field}` 占位符经 `v.named` 直达（字段未点火值为 Missing → 现有 spec 兜底分支保留，`is not Missing` 判断不变）。`spec_inputs`（常量）通道不动。
- body 签名维持 `async def body(view)`（视图模式零迁移）；过渡完成后可选改 `v.named`。

### 5.3 `core/prompt.py`

`_substitute` 的 `view[key].value` 改为 `v.named` / `extra_values` 二级查找：命中字段直接用值；未命中落 `extra_values`（常量）；再未命中保留原样。逻辑更短，语义不变。

### 5.4 脚本（`@script`）与 submodule body

- 脚本改为字段名消费：`view["Fix"]` → `view.field("fixed")`（或一次性 `vals = view.named`）。以 `fact_review_loop.merge` 为例：`fixed = view.field("fixed")`、`seed = view.field("seed")`，`view.state` 不变。
- submodule 闭包（`graph_builder.py:318`）`view[producer].value` → `view.field(field_name)`——顺带修正了"field 名在旧 view 里恒为 Missing、真值绕道 producer 名"的别扭。

### 5.5 guards

**零改动**（§2.2 已审计）。过渡期在 deprecation 警告下运行；后续可选迁到 `g.output` / `g.field("draft")`。`submodule.py:128` 重注册机制、pack 的 guards 导出均不动。

### 5.6 其余

`translator.py` 的合成 `DictView({"spec": ...})`（引擎外构造）：`NodeView` 构造签名保持兼容（或经 `DictView` 别名 + legacy 访问），不阻塞。`cli/command.py` 仅用 `view.node`，不动。`scaffold.py` 模板注释更新。

---

## 6. tickflow 文件级改动清单

| 文件 | 改动 |
| --- | --- |
| `ir.py` | `Bind` 数据类；`Node.bind`；`copy()/to_dict()` 携带 |
| `parser.py` | `_BIND_RE` + 具名 tokenizer；bind/inputs 不变式；ParseError 各条 |
| `checker.py` | 不动 |
| `views.py` | `NodeView`/`GuardView`；`DictView` 弃用别名；legacy 访问告警 |
| `engine.py` | `_bind_values` 单点解析；分发调用；`_guard_view` 重写为 GuardView（src 输出 + src bind 输入 + state）；运行时个数断言 |
| `async_runner.py` | 消费同一套 helpers，仅 await 包装 |
| `registry.py` | `_Sig` 注册期检测与缓存；类型注解放宽 |
| `runner.py` | `_validate_registry_for_graph` 扩展 E1–E3/W1 |
| `__init__.py` | 导出 `Bind`/`NodeView`/`GuardView` |
| tests / examples | 全量迁移（examples 的 `def f(v)` 可保留——它们自动进视图模式，正好当作 legacy 路径的活体回归用例；新增值模式对照用例） |

## 7. 测试计划

1. **Missing 保真组**（§4.6 第 3 条场景 × args/named/input × sync/async）——correctness 级，最先写。
2. bind 解析组：三种语法、混用拒绝、auto-bind 键序、`Bind.named` 程序化构造。
3. 分发组：值模式位置/具名/`state` 尾参、async body、视图模式 `def f(v)` 自动判定、注解判定。
4. 验证组：E1/E2/E3 在 Runner 构造期报错、W1 告警、guard 访问声明外名字 KeyError。
5. guard 组：GuardView.output 是当前 tick 输出（循环回边场景：guard 见到的是本轮产出而非上一轮）、声明输入与 body 消费一致（index policy 对齐案例）、失败节点不咨询 guard（回归）。
6. 兼容组：legacy 访问路径全回归 + DeprecationWarning 断言；`DictView` 别名导入。
7. 端到端：`examples/*_beh.py` 双模式各跑一遍，输出与迁移前一致（golden）。

## 8. 明确不做

- 不合并 `inputs` 与 `bind` 为单一声明（保持策略表与消费序正交，控制迁移半径）。
- 不做 `BodyResult` 状态返回（§4.4）。
- 不让 guard 支持 index pin / 独立 bind（guard 裁决 body 的同一份数据，不另立解析）。
- 不在 mermaid 渲染 bind。

## 9. 实施后记（2026-09-05）

Task 1–9 落地后的偏差与定案记录（与上文设计条款冲突时，以本节为准）：

- **legacy/resolved 双键空间**：视图的 by-name 键与审计记录的键严格分离——字段名只叠加在视图的 legacy 键空间上（字段撞名时读取来自纯 resolved 表，绝不互相污染），审计 `NodeState.inputs` 只记真实生产者，审计纯净。
- **E1/E2 对缺省参数严格（有意收紧，已文档化）**：位置模式要求 arity 精确相等、具名模式要求参数名集合精确相等；被绑输入之外的带默认值参数在 Runner 构造期即被拒绝（尽管实际调用本可运行）。`classify` 与 `_validate_bind_signatures` docstring 已注明。
- **arity=None（`*args` / 不可自省调用方）跳过 E1/E3**：E1 与 E3 一致地跳过未知元数（一个 `*args` guard 确实接受 1 个参数），交由运行时调用。
- **guard 侧 index 策略与 body 同源**：guard 按节点声明的同一 InputPolicy 重解析（`M[1]` 对齐案例已钉）；已知限制——sync 引擎同 tick 中段记录，index 策略在极端驱逐边缘下 guard 时刻的重解析窗口可能偏移（`latest` 免疫），已在 `_guard_node_view` docstring 如实记载，行为不改。
- **GuardView 键空间 = {src → 当前输出} ∪ 声明输入**：src 名映射到被裁决的本轮输出（撞名时 src 优先），其余只能读 bind 声明内的字段/生产者，声明外一律 KeyError（P4）。
- **兼容垫片 `DictView` 保留至 1.0**：构造 shim + by-name 访问的 DeprecationWarning 窗口，examples 的 `def f(v)` legacy 路径作为活体回归持续运行；1.0 前后按下游 SpecModule 迁移进度移除。
