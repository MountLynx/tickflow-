# Tickflow DictView 绑定耦合：问题描述与解决方案

---

## 1. 问题概述

Tickflow 当前的 body/guard 接口通过 `DictView` 对象向函数传递输入，函数内部以 `v.A.value` 的方式按上游节点名访问数据。这一设计将**图的拓扑信息（接线）**泄露进了**函数实现（计算）**中，违反了"函数只管计算、接线由外部决定"的基本原则，引发了一系列连锁问题。

---

## 2. 当前设计

### 2.1 数据流

引擎在每个 tick 为触发的节点构造 `DictView`，调用 body：

```python
# engine.py L170-L177
resolved = _resolve_inputs(graph, node, run_state, t, registry)
view = DictView(resolved, state_view, node)
body = registry.get_body(graph.nodes[node].body)
output = body(view)            # ← body 接收 DictView
```

### 2.2 DictView 结构

```python
# views.py L87-L141
class DictView:
    _inputs: dict[str, Resolved]   # 键 = 生产者节点名
    _state: _NodeStateView         # 可变状态代理
    _node: str                     # 当前节点名

    def __getattr__(self, name):   # v.A → _ResolvedAttr
    def __getitem__(self, name):   # v["A"] → _ResolvedAttr
    def inputs(self):              # → {"A": value, "B": value}
    def items(self):               # → 迭代器
```

### 2.3 Body 的典型写法

```python
@registry.body("incr")
def _incr(v):
    return v.A.value + 1

@registry.body("combine")
def _combine(v):
    return v.A.value + v.C.value
```

### 2.4 Guard 的视图构造（关键差异）

```python
# engine.py L260-L270 — _guard_view()
resolved[src] = Resolved(value=src_output, k=None)
for name in graph.nodes:           # ← 遍历全部节点！
    if name == src:
        continue
    v = run_state.resolve(name, "latest", None, t)
    resolved[name] = Resolved(value=v, k=None)
```

Guard 的 DictView 包含**图内所有节点**的输出，而非仅限声明边连接的生产者。

---

## 3. 问题清单

### P1: Body 与图拓扑名硬耦合

body 函数内部硬编码上游节点名，导致函数不可跨图复用：

```python
@registry.body("incr")
def _incr(v):
    return v.A.value + 1    # "A" 是图拓扑名，不是函数参数

# 另一个图中上游叫 fetch_result，同一个逻辑必须重写：
@registry.body("incr_v2")
def _incr_v2(v):
    return v.fetch_result.value + 1
```

**后果**：计算逻辑完全相同，仅因上游命名不同就需要重复编写 body。

### P2: 图文本不是完整的图定义

`GRAPH_TEXT` 声明了控流拓扑，但**数据绑定**隐含在 body 函数体内：

```
图文本声明：A-->B（存在一条边）
body 声明：v.A.value（B 读 A 的值）

完整边 = 控流拓扑（图文本）+ 数据绑定（body 函数体）
```

图文本自称是"图的定义"，实际只是控流骨架。另一半散落在 Python 代码中，且无法被图验证检查。

**后果**：
- 图无法自包含地验证完整性
- 图无法安全重构（改节点名 → body 静默崩）
- 图无法自动生成（绑定信息不在声明层，无法程序化构造）

### P3: 错误全部延迟到运行时

body 中写了不存在的节点名，parse 和 check 阶段均无法发现：

```python
# 图：A-->B-->C（没有 A-->C 边）
@registry.body("combine")
def _combine(v):
    return v.A.value + v.B.value    # v.A → AttributeError，运行时才炸
```

**后果**：绑定错误不在编译期/验证期捕获，只在运行时以 `AttributeError` 爆发。

### P4: Guard 可以偷读图外的数据

`_guard_view()` 将**全部节点**的输出填入 DictView，guard 可以读取没有边连接的节点：

```python
# 图：A-->B--|g|-->C（没有 A-->C 边）
@registry.guard("g")
def _g(v):
    return v.A.value > 5    # ✅ 居然能跑！guard 建立了图中不存在的数据依赖
```

**后果**：guard 通过 `v.A` 建立隐式数据通道，绕过了图声明，且无任何警告或错误。

### P5: Body 和 Guard 的 DictView 语义不一致

| 维度           | Body 的 DictView     | Guard 的 DictView |
| -------------- | -------------------- | ----------------- |
| 内容来源       | 仅声明边连接的生产者 | **全部节点**      |
| 非声明节点访问 | `AttributeError`     | 静默返回值        |
| 偷读可能性     | 不可能               | 可能              |

**后果**：同一个 API，两种语义，心智模型分裂。

### P6: API 冗余，同一件事多种写法

```python
v.A                    # 属性访问
v["A"]                 # 字典访问
v.A.value              # 显式取值
v.inputs()["A"]        # 字典取值
list(v.items())[0][1]  # 迭代取值
```

五种写法访问同一个数据，增加认知负担和混用出错概率。

### P7: IDE 和类型系统无法支持

`v.A` 通过 `__getattr__` 动态合成，IDE 无法自动补全，静态类型检查无法验证，`.value` / `.k` 的存在也不可见。

---

## 4. 根因分析

### 4.1 抽象边界错位

```
完整图 = 控流拓扑 + 数据绑定 + 计算逻辑
         ─────────   ─────────   ─────────
         GRAPH_TEXT   body 中的    纯函数
                     v.X.value
```

**数据绑定**这一层本应是图声明的职责，当前却被埋进了函数实现中。函数同时承担了"接线"和"计算"两个职责，违反了单一职责原则。

### 4.2 DictView 的身份混乱

`DictView` 同时充当了：
1. **输入容器**（承载解析后的值）
2. **绑定契约**（键空间 = 生产者节点名，隐式声明了数据依赖）
3. **元数据载体**（`.k` 暴露解析策略细节）
4. **状态代理**（`view.state` 提供可变状态读写）

这四个职责本应分离，当前被揉进一个对象。

### 4.3 设计取舍的回顾

当前设计选择了"省掉绑定声明层，让 body 直接按名访问"，代价是上述全部问题。在小规模、人工维护的工作流中，显式耦合确实比抽象间接层更直观；但当图需要重构、复用、自动生成或大规模管理时，这个设计成为天花板。

---

## 5. 解决方案

### 5.1 核心思想

**函数只接收值，不接收接线信息。接线由图声明决定。**

```python
# 当前：body(view) — 函数从 view 中按名取值
# 改后：body(*values) — 函数按位置接收值
```

### 5.2 Body 签名变更

```python
# ── 当前 ──
@registry.body("combine")
def _combine(v):
    return v.A.value + v.C.value

# ── 改后 ──
@registry.body("combine")
def _combine(a, c):
    return a + c
```

函数签名即契约：参数数量、位置语义、类型提示——全部可见、可验、可补全。

### 5.3 图声明绑定顺序

```
A-->B
C-->B
B.body: combine
B.bind: [A, C]          # 声明：第一个参数 ← A，第二个参数 ← C
```

单输入节点可省略 `bind`（自动绑定唯一生产者）：

```
A-->B
B.body: incr            # 自动：B 的唯一输入 ← A
```

### 5.4 引擎调用变更

```python
# ── 当前 engine.py L176-L177 ──
body = registry.get_body(graph.nodes[node].body)
output = body(view)

# ── 改后 ──
body = registry.get_body(graph.nodes[node].body)
bind_order = graph.nodes[node].bind           # ["A", "C"]
values = tuple(resolved[p].value for p in bind_order)
output = body(*values)                        # combine(3, 7)
```

核心改动仅一行：`body(view)` → `body(*values)`。

### 5.5 可变状态的处理

当前 `view.state` 是 DictView 上的一个特殊通道。改为位置参数后，需要独立机制：

**方案 A：约定尾部参数**

```python
@registry.body("retry")
def _retry(data, state):          # 约定最后一个参数为状态
    state["attempts"] += 1
    return data if data else None

# 引擎调用
output = body(*values, state_proxy)
```

**方案 B：返回值携带状态（更纯）**

```python
from tickflow import BodyResult

@registry.body("retry")
def _retry(data, attempts):       # 状态作为显式输入
    return BodyResult(
        value=data,
        state={"attempts": attempts + 1}
    )

# 引擎：传入当前状态，从 BodyResult 中提取新状态
```

两种方案都比 `view.state` 更显式——状态在函数签名中可见，不再是隐藏通道。

### 5.6 Guard 签名变更

Guard 同理，按位置接收值：

```python
# ── 当前 ──
@registry.guard("cont")
def _cont(v):
    return v.B.value <= 10

# ── 改后 ──
@registry.guard("cont")
def _cont(b_output):
    return b_output <= 10
```

Guard 的输入来源在边的声明中已知（`B--|cont|-->A`，guard 的触发节点是 B），无需遍历全部节点。**P4（偷读）从根本上消除。**

### 5.7 IR 层扩展

```python
@dataclass
class Node:
    name: str
    is_start: bool = False
    join: Literal["AND", "OR"] = "AND"
    body: str | None = None
    inputs: dict[str, InputPolicy] = field(default_factory=dict)
    bind: list[str] = field(default_factory=list)   # ← 新增：绑定顺序
```

`bind` 是生产者名的有序列表，声明了 body 参数的来源顺序。

### 5.8 注册时静态验证

```python
import inspect

def body(self, name: str, fn=None):
    # ...注册逻辑...
    sig = inspect.signature(fn)
    # 存储参数数量，供图验证时比对
    self._body_arity[name] = len(sig.parameters)
```

图解析阶段：

```python
# parse() 或 check() 中
arity = registry.body_arity(node.body)
if len(node.bind) != arity:
    raise ParseError(
        f"Node {node.name}: body '{node.body}' expects {arity} parameters, "
        f"but bind declares {len(node.bind)} inputs"
    )
```

**P3（运行时错误）变为编译期错误。**

---

## 6. 变更影响分析

### 6.1 各问题解决状态

| 问题                    | 当前             | 改后                     | 状态   |
| ----------------------- | ---------------- | ------------------------ | ------ |
| P1: Body 耦合拓扑名     | `v.A.value`      | 位置参数 `a`             | ✅ 消除 |
| P2: 图不自包含          | 绑定隐含在 body  | 绑定在 `.bind` 声明      | ✅ 消除 |
| P3: 运行时才报错        | `AttributeError` | `inspect.signature` 验证 | ✅ 消除 |
| P4: Guard 偷读          | 全节点 DictView  | 仅接收触发节点输出       | ✅ 消除 |
| P5: Body/Guard 语义分裂 | 两套 DictView    | 统一位置参数             | ✅ 消除 |
| P6: API 冗余            | 5 种写法         | 1 种（函数参数）         | ✅ 消除 |
| P7: IDE 不支持          | `__getattr__`    | 原生参数                 | ✅ 消除 |

### 6.2 需要改动的模块

| 模块                                          | 改动内容                                          | 影响范围 |
| --------------------------------------------- | ------------------------------------------------- | -------- |
| [`ir.py`](tickflow/ir.py#L79-L86)             | `Node` 新增 `bind: list[str]` 字段                | 数据模型 |
| [`views.py`](tickflow/views.py#L87-L141)      | DictView 可废弃或保留做内部用                     | API 层   |
| [`engine.py`](tickflow/engine.py#L170-L177)   | `body(view)` → `body(*values)`                    | 核心引擎 |
| [`engine.py`](tickflow/engine.py#L244-L270)   | `_guard_view()` 简化，不再遍历全节点              | 核心引擎 |
| [`registry.py`](tickflow/registry.py#L33-L34) | `Body` 类型从 `Callable[[Any], Any]` 改为可变参数 | 类型定义 |
| [`parser.py`](tickflow/parser.py)             | 解析 `B.bind: [A, C]` 语法                        | 解析器   |
| [`checker.py`](tickflow/checker.py)           | 新增 bind 与 body 参数数量一致性检查              | 验证器   |
| 所有 examples / tests                         | body 签名从 `def f(v)` 改为 `def f(a, b)`         | 全量迁移 |

### 6.3 向后兼容策略

- **过渡期**：Registry 同时支持 `Callable[[DictView], Any]` 和 `Callable[..., Any]`，通过 `inspect.signature` 自动判断参数是否名为 `v`/`view`
- **废弃期**：DictView 接口标记 `DeprecationWarning`
- **移除期**：仅保留位置参数接口

---

## 7. 迁移示例

### 7.1 计数循环

**当前：**

```python
GRAPH_TEXT = """
[seed]-->A
seed.body: seed_start
A.join: OR
A-->B
B.body: incr
B--|cont_le10|-->A
"""

@registry.body("seed_start")
def _seed(v):
    return 1

@registry.body("incr")
def _incr(v):
    return v.A.value + 1

@registry.guard("cont_le10")
def _cont(v):
    return v.B.value <= 10
```

**改后：**

```python
GRAPH_TEXT = """
[seed]-->A
seed.body: seed_start
A.join: OR
A-->B
B.body: incr
B--|cont_le10|-->A
"""

@registry.body("seed_start")
def _seed():
    return 1

@registry.body("incr")
def _incr(a):
    return a + 1

@registry.guard("cont_le10")
def _cont(b):
    return b <= 10
```

### 7.2 多输入合并

**当前：**

```python
@registry.body("combine")
def _combine(v):
    return {"a": v.A.value, "b": v.B.value}
```

**改后：**

```python
# 图声明：
# A-->C  B-->C  C.body: combine  C.bind: [A, B]

@registry.body("combine")
def _combine(a, b):
    return {"a": a, "b": b}
```

### 7.3 纯函数复用

**当前**（不可复用）：

```python
@registry.body("incr_A")
def _incr_a(v):
    return v.A.value + 1

@registry.body("incr_fetch")
def _incr_fetch(v):
    return v.fetch.value + 1       # 同一逻辑，重写 body
```

**改后**（可复用）：

```python
def increment(x):                  # 纯函数，一处定义
    return x + 1

@registry.body("incr_A")
def _incr_a(a):
    return increment(a)

@registry.body("incr_fetch")
def _incr_fetch(data):
    return increment(data)         # 复用纯函数，body 仅做薄适配
```

---

## 8. 设计原则总结

| 原则           | 当前设计                    | 改后设计                    |
| -------------- | --------------------------- | --------------------------- |
| 函数只管计算   | ❌ body 内含接线逻辑         | ✅ body 仅接收值、返回值     |
| 接线由外部决定 | ❌ 接线隐含在 `v.X.value` 中 | ✅ 接线在图声明的 `.bind` 中 |
| 错误尽早发现   | ❌ 运行时 `AttributeError`   | ✅ 解析/注册时静态验证       |
| 图可自包含     | ❌ 绑定散落在 body 中        | ✅ 绑定在图 IR 中显式声明    |
| 函数可复用     | ❌ 耦合拓扑名                | ✅ 位置参数，拓扑无关        |

**核心命题**：函数就是函数。接线是图的事，计算是函数的事。两者不应混在一起。

---

## 9. 参考文献

- [`views.py`](tickflow/views.py#L87-L141) — DictView 当前实现
- [`engine.py L170-L177`](tickflow/engine.py#L170-L177) — body 调用点
- [`engine.py L244-L270`](tickflow/engine.py#L244-L270) — guard 视图构造（偷读根因）
- [`ir.py L79-L86`](tickflow/ir.py#L79-L86) — Node IR 定义（需新增 `bind` 字段）
- [`registry.py L33-L34`](tickflow/registry.py#L33-L34) — Body/Guard 类型定义
- [输入策略与视图](9-input-policies-and-views) — DictView 完整语义
- [注册表与行为](19-registry-and-behaviours) — body/guard 注册契约
- [解析器与图 IR](8-parser-and-graph-ir) — 图验证与 InputPolicy
- [Tick 执行语义](5-tick-execution-semantics) — 引擎 tick 循环

---

## FAILED 终态落地：饿死 run 不再误报 done（0.3.0，2026-09-30）

状态机文档自始承诺四个终态，但 `FAILED` 从未被赋值：AND-join 饿死（等一个
永不来的 True 槽位）→ 空 tick → status 停在 IDLE → 嵌入层（SpecModule
`_finalize_phase`）把 IDLE 映射成 done——"图里有工作永远点不着火"的 run 被
报成成功。

修复：`runner.py` / `async_runner.py` 两处 tick 状态判定，空 tick 时按
`_has_pending()`（armed_starts 非空或任意槽位 True）区分——有 pending →
FAILED（新终态，ticking 停止），无 pending → IDLE（不变）。同步屏障语义下
空 tick 即不动点，无误报面：producer 写槽与 consumer 点火逐 tick 交替，
mid-flight 不产生空 tick；pause 在 tick 前 break；cancel 是独立终态；
max_ticks 耗尽停在 RUNNING（truncated，可 resume）。

嵌入侧：SpecModule `_finalize_phase` 的 FAILED 分支文案改为
"starved: work pending but nothing fireable"，并附未点火节点清单
（任务全集 − 已点火，模块层由 run_state 边历史推导，不新增引擎 API）。
