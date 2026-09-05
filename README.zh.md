# tickflow

[English](README.md) | 简体中文

一个小巧的 Petri 风格工作流控制框架，用于构建简短、可审计、可回退的
流程。你用类 mermaid 的语法描述图结构；引擎将其作为同步 Petri 网的*步进*
（step）运行在布尔槽位（slot）标识（marking）之上，所有运行时状态集中在
`RunState` 中——因此快照、暂停、回退、重放都极其廉价。

> 这是状态机吗？不是——它是一个 **Petri 网**（更准确地说，是带 AND/OR
> 汇合的标记图）。有限状态机是"整个网络任意时刻恰有一个令牌存活"的退化
> 特例。`tickflow` 支持多个并发起点、AND-join（等待所有上游）、OR-join
> （任一上游即激发）以及循环——这些都不是普通 FSM 能原生表达的。参见
> [设计说明](#设计说明)。

## 快速开始

```bash
pip install tickflow-py

python -m tickflow run examples/counter_loop.txt -b examples/counter_loop_beh.py
```

图文件只声明**结构**；具体的 body/guard 可调用对象在一个 Python
"behaviours" 文件中注册到 `tickflow.registry`。

### 图语法

```
[A]-->B                  # A 是起始节点；普通边 A→B（恒为 True）
B--|g1|-->C              # 守卫边：当且仅当 guard g1(view) 为 True 时槽位为 True
B--|g1|-->A              # 环 / 回边
C.inputs: A, B[2]        # C 读取 A（latest_before）和 B 的第 2 次激发（1-based）
C.body: compute_c        # C 的 body 是已注册的 compute_c
C.join: OR               # 覆盖 join 类型（默认 AND）
```

- `[A]` 标记一个**起始节点**。允许多个起点，它们在 tick 0 并发激发。
- `-->` 是普通数据流边：源节点激发时，总是向下游槽位写入 `True`。
- `--|name|-->` 是守卫边：写入 `guard(view)` 的结果（因此守卫失败会写入
  **`False`**，一次显式覆盖——上一轮循环残留的 `True` 永远不会泄漏进
  循环）。
- `node.inputs:` 和 `node.body:` 行绑定行为。两者都可省略（默认 body =
  恒等/回显；默认 inputs = 所有生产者，策略均为 `latest_before`）。
  `inputs` 可以引用非直接生产者节点（例如 `A[k]` 固定 A 的第 k 次激发），
  只要被引用的节点在图中位于**上游**（存在到达消费者的有向路径）。
- `#` 开头是注释。

### Behaviours 文件

```python
from tickflow import registry
from tickflow.views import Missing

@registry.body("incr")
def incr(v):
    return v.A.value + 1          # v.A 是生产者 A 解析后的值

@registry.guard("cont_lt3")
def cont(v):
    return v.B.value < 3          # B 出边上的 guard 能看到 B 的输出
```

body 和 guard 收到一个 `DictView`：`v.A`（或 `v["A"]`）按节点的输入策略
解析声明的生产者。`v.A.value` 是裸值；`v.A.k` 是所用激发的序号（对
`latest_before` 为 `None`）。生产者尚无合格激发时得到 `Missing` 哨兵
（假值）。

## 架构

### RunState — 单一事实来源

所有运行时状态都在 `RunState` 中，按职责划分为四个内层：

```
RunState
    _edges       dict[node, list[(tick, output)]]    窗口化（每节点最近 2 次激发），供 resolve() 使用
    _fire_counts dict[node, int]                     激发序数——把窗口条目映射回 A[k]
    _state       dict[node, dict]                    每节点当前可变状态，O(1)
    _records     list[NodeState]                     内存审计：keep_records 且无持久化后端时才维护
```

- **`_edges`** — 供 `resolve()` 使用的窗口化快速查询索引：每个节点只在
  内存中保留最近两次激发；更早的激发存放在后端。内存占用为
  O(节点数 × 2 × 输出大小)，与节点激发过的总次数无关。
- **`_fire_counts`** — 每节点激发计数器；把窗口条目映射回其 `A[k]` 序数，
  使索引读取在窗口化之后仍然正确。
- **`_state`** — 每节点当前可变状态（body 通过 `view.state` 写入的内容）。
  始终维护，O(1) 访问。
- **`_records`** — 内存中的完整 `NodeState` 记录。仅在 `keep_records=True`
  **且**无持久化后端时维护。有后端时（默认），激发记录按 tick 批量刷入
  后端，`audit()` 按需查询。

派生数据从这些层中提取——挂了后端时则从后端提取：

| 查询 | 来源（持久化后端） | 来源（NullBackend） |
|-------|-----------------------------|----------------------|
| `resolve(latest)` — 输入解析 | `_edges` 窗口（O(1)，零 I/O） | `_edges` 窗口（相同） |
| `resolve(index)` — 第 k 次激发 | 先查窗口，再查 `backend.firing_at` | 窗口；窗口外 → `Missing` |
| `firings_of()` — 输出历史 | `backend.firings_of`（完整） | `_edges` 窗口 |
| `audit()` — 完整审计日志 | `backend.list_firings` | `_records`（keep_records） |
| `to_snapshot_data()` — 快照 | 窗口 + state + `fire_counts`（审计开启时含 `records`） | 相同 |

### NodeState — 一次激发，全部数据

```python
@dataclass
class NodeState:
    tick: int                          # 哪个 tick
    node: str                          # 哪个节点
    inputs: dict[str, Any]             # 解析后的输入值
    output: Any                        # body 返回值
    edges_fired: list[tuple[...]]      # 下游边的写入结果（Phase B 填充）
    status: "ok" | "failed" | "aborted"
    error: str | None
    mutable_state: dict[str, Any]      # body 运行后的节点状态
```

这是某个节点在某个 tick 发生的一切的**单一事实来源**。

### 省内存模式

```python
rn = Runner(graph, registry, keep_records=False)
```

`keep_records=False` 时不填充 `_records`——节省内存——但 `_edges`、
`_fire_counts` 和 `_state` 仍然维护。无论这个开关如何，输入解析和节点
可变状态都正常工作。快照会省略 `"records"` 键。后端持久化（激发、快照）
不受影响。

默认情况下（`backend=None`），Runner 会在系统临时目录创建一个临时
SQLite 后端，自动生成 `session_id`，并在 Runner 被垃圾回收时删除数据库
文件——因此持久化、审计、检查点和 `A[k]` 索引读取开箱即用。显式传入
`NullBackend()` 得到零 I/O 的内存运行，或传入具体后端得到持久化运行。
`NullBackend` 同时启用**快进模式**：完全跳过每 tick 快照持久化（其隐含
的审计序列化在长运行中是 O(n²) 的），代价是没有冷历史——窗口外的 `A[k]`
读取和完整 `firings_of()` 退化为 `Missing` / 2 条目的窗口。

## 语义

### 模型

引擎是一个**纯函数**：

```
tick: (marking_t, run_state, t) -> (marking_{t+1}, firings_t)
```

只存在两个可变状态容器：
- **marking** — `dict[(dst, src), bool]`，每条入边一个布尔*槽位*，外加
  一个*待激发*起始节点集合（一次性）。
- **run_state** — `RunState` 实例（窗口化历史、状态、审计）。

没有任何隐藏的中间态。这正是快照廉价的原因。

### 一个 tick

1. 计算*可激发*节点集合：节点可激发当且仅当
   - 它是待激发的起点（激发一次后解除武装），**或**
   - 其 join 谓词在输入槽位上成立：AND 为 `all`，OR 为 `any`。
2. 每个可激发节点并发激发：body 读取输入（`latest_before(t)`——严格早于
   当前 tick，所以同侪看不到彼此同一 tick 的写入），输出记入 `RunState`，
   其输入槽位被消耗（重置为 `False`）。
3. 全部激发完成后，每个已激发节点的出边写入下游槽位：普通边写 `True`，
   守卫边写 `guard(view)`。

### 输入策略

- **`latest`**（默认）：生产者最近一次 `tick < t` 的激发。这是与 marking
  一致的读取——节点看不到同侪同一 tick 的写入。在循环中，这意味着"上一
  轮迭代的输出"。
- **`A[k]`**（索引）：生产者 overall 第 `k` 次激发（1-based），与 tick
  无关。用于跨迭代固定和审计重放。

生产者尚无合格激发时得到 `Missing`（假值）；body 应当处理它。

inputs 可以引用非直接生产者的节点（没有边直接进入消费者），例如
`C.inputs: A[1]` 而 A 经由更长的路径位于 C 上游。只要 A 有到 C 的有向
路径（因此 A 比 C 先激发）就是合法的。

### 汇合（Join）

- **AND-join**（默认）：当且仅当*所有*输入槽位为 `True` 时激发。用于
  "等待所有上游"。
- **OR-join**：当且仅当 *≥1* 个输入槽位为 `True` 时激发。用
  `node.join: OR` 声明。适用于一个节点有多个生产者但任意一个到达即应
  激发的场景（例如节点既在循环中又被一个一次性起点播种）。

### 死锁检测

如果 AND-join `M` 有 ≥2 个生产者位于 XOR 分叉器 `B`（有 ≥2 条守卫出边
的节点）的**互斥分支**上，`M` 会死锁：`B` 每次激发至多让一个分支的槽位
变 `True`，于是 `M` 永远等不到另一半。检查器会标记这种情况，并在 CLI 中
提示把 `M` 提升为 OR-join。OR-join 在此不会死锁，因为同步步进语义使
"≥1 槽位"谓词可判定——无需回答"还会有更多令牌到来吗？"（开放 Petri 网
OR-join 问题）。

```python
from tickflow import parse, check, promote
g = parse(text, registry=r)
for s in check(g):       # list[DeadlockSuggestion]
    promote(s, g)        # 把 s.node.join 翻转为 "OR"
```

带着未解决的建议构造 `Runner` 会抛出 `DeadlockError`（绝无静默死锁）。

### 静态警告（解析期）

解析器对常见陷阱发出警告：

| 警告 | 条件 |
|---------|-----------|
| 消费者读取无 body 的生产者 | `C.inputs` 引用了没有 body 的节点——C 将收到 `None` |
| 非生产者输入 | `C.inputs` 引用了没有边相连的节点——经由历史而非令牌流解析 |
| 无守卫循环 | 环上没有守卫边——将永远循环 |

运行时，当返回 `Failure` 的节点拥有守卫出边时引擎也会告警（失败节点的
守卫不会求值——所有出边写 `False`）。

## 快照、暂停、回退

```python
rn = Runner(graph, registry)
rn.run_until_idle(max_ticks=100, pause_at={5})   # 在 tick 5 之前停下
snap = rn.snapshot()                              # 可 JSON 化的 dict
rn.run_until_idle(max_ticks=100)                  # 跑完
rn.restore(snap)                                  # 回退到 tick 5
rn.run_until_idle(max_ticks=100)                  # 重放（body 纯净时结果一致）
```

- **`snapshot()`** 返回 `{"tick", "marking", "run_state", "status",
  "cancel_reason", "fireable"}`——纯 JSON。`run_state` 包含 `edges`
  （窗口化输出索引）、`fire_counts`（每节点激发序数）、`state`（每节点
  可变状态）和 `records`（内存审计——仅当 `keep_records=True` 时包含；
  传 `snapshot(include_records=False)` 可剥离）。快照是**自包含**的：
  edges/state 始终随快照携带，因此可以恢复进一个没有任何自身历史的新
  Runner。
- **`restore(snap)`** 回退：从 `snap` 设置 tick/marking/run_state/status。
  `tick >= snap["tick"]` 的内存记录被丢弃；有持久化后端时，磁盘上的激发
  记录作为审计历史保留，而窗口、激发计数和节点状态从持久化行重建。恢复
  出的终止状态（ABORTED/CANCELLED/FAILED）被重置为 IDLE 以便继续运行。
  `audit()` 会按 `(tick, node)` 去重（保留首条），因此恢复后重放绝不会
  重复计数。
- **`pause_at={n}`** 在 tick `n` 之前的边界停下——没有激发到一半的状态
  需要保存。
- **分支 / what-if**：`copy.deepcopy(snap)` 后 `restore` 进独立的
  Runner。库不维护时间线森林。有持久化后端时审计在磁盘上，因此从深拷贝
  快照建立的分支在再次激发前 `audit_log()` 为空——执行语义（窗口、状态、
  经后端的 `A[k]`）完好。要保留审计地分叉，请用携带完整轨迹的
  `to_json()` / `from_json()`。
- **`to_json()` / `from_json()`** 把完整状态序列化为单个 JSON 对象；有
  持久化后端时审计轨迹会被重新嵌入，保证往返不丢。图和 registry *不*
  存入其中——重载时需要自行提供。

**Body 纯净性**：body 应当是其输入视图的纯函数（写状态除外）。如果 body
不纯净，恢复后重放可能与原始运行产生分歧（审计日志仍会记录当初实际发生
了什么）。

## 失败、状态与控制

### Failure（body 错误信号）

body 可以返回 `Failure(error, type=...)` 而非正常值：

```python
from tickflow import Failure

@registry.body("call_llm")
def call_llm(v):
    try:
        return llm_client.chat(...)
    except NetworkError as e:
        return Failure(str(e), type="infrastructure")  # 终止整个运行
    except ParseError as e:
        return Failure(str(e), type="llm")              # 跳过下游
```

- **`type="llm"`**（默认）：逻辑性/可恢复失败。节点出边写 `False`，下游
  AND-join 因此不激发（="上游失败，跳过下游"）。运行继续。
- **`type="infrastructure"`**：不可恢复失败。出边写 `False` **且** runner
  进入 `ABORTED`，停止后续所有 tick。

`Failure` 仍会带着 `NodeState.status`（`"failed"` / `"aborted"`）和
`NodeState.error` 记入 `RunState` 与审计日志。

> **注意**：失败节点向**所有**出边写 `False`——守卫不再求值。要实现
> 可控路由（例如失败重试），让 body 返回结果 dict、由守卫检查输出值，
> 而不是返回 `Failure`。

### RunStatus

`Runner.status` 是 `RunStatus` 枚举：

| 状态 | 含义 |
|--------|---------|
| `IDLE` | 静默（上个 tick 无激发，或尚未开始） |
| `RUNNING` | 某 tick 有激发（瞬态；下一步变为 IDLE 或终止态） |
| `ABORTED` | 发生了 infrastructure `Failure`；已停止 |
| `CANCELLED` | 调用了 `cancel()`；已停止 |
| `FAILED` | （保留）所有节点失败且无可激发节点 |

```python
rn.cancel("user requested")        # -> CANCELLED；tick 变为空操作
rn.is_idle()                       # status == IDLE
rn.is_terminal()                   # ABORTED/CANCELLED/FAILED，或 IDLE 且无事可做
rn.reset()                         # 把非 RUNNING 状态清回 IDLE
```

### 节点状态（`view.state`）

每个节点有一个由 `RunState` 管理的可变状态 dict（因此参与快照/恢复），
并且对守卫可见。body 读写自己的状态；守卫收到只读视图：

```python
@registry.body("retryable")
def retryable(v):
    v.state["attempts"] = v.state.get("attempts", 0) + 1
    return {"ok": v.state["attempts"] >= 3, "attempt": v.state["attempts"]}

@registry.guard("should_retry")
def should_retry(v):
    out = v.B.value
    return isinstance(out, dict) and not out.get("ok") and v.state.get("attempts", 0) < 3
```

重试自循环就是这样表达的：body 在 state 中跟踪 `attempts`，返回结果
dict，守卫同时检查结果与状态来决定是否循环。

## 钩子（观察者接缝）

`on_fire` / `on_tick_end` / `on_tick_start` 是 tickflow 与外部世界（事件、
记录存储、进度）之间的唯一接缝。它们在每次节点激发 / tick 结束 / tick
开始时触发。钩子异常会被记录并吞掉，行为不端的观察者无法破坏运行。

```python
rn.on_fire(lambda ns: event_bus.emit(ns.node, ns.output))
rn.on_tick_start(lambda tick, fireable: ui.highlight(fireable))
rn.on_tick_end(lambda tick, firings: db.save(tick, rn.snapshot()))
```

`AsyncRunner` 接受异步钩子（`async def`）。

## 持久化后端

以 `backend=...` 和 `session_id=...` 构造的 `Runner`，在每个 tick 结束时
持久化：每条 `NodeState`（过程记录，按 tick 批量刷写）以及新 tick 索引
处的一份轻量快照。持久化快照会剥离 `records`（它们在后端的激发表中）但
保留 `edges`/`state`——保持自包含——并附上 `fired` 轨迹，列出该 tick
激发过的节点。快进模式（`NullBackend`）完全跳过每 tick 快照持久化。

```python
from tickflow import JsonBackend

be = JsonBackend("tickflow/sessions")
rn = Runner(graph, registry, backend=be, session_id="sess-1")
rn.run_until_idle(max_ticks=100)

# 之后 / 其他进程：从最近持久化的 tick 恢复。
snap = be.load_snapshot("sess-1", be.latest_tick("sess-1"))
rn2 = Runner(graph, registry)
rn2.restore(snap)
```

- **`Backend`**（Protocol）：`save_snapshot` / `load_snapshot` /
  `latest_tick` / `list_snapshots` / `save_firing` / `save_firings` /
  `list_firings` / `firing_at` / `firings_of` / `save_checkpoint` /
  `list_checkpoints` / `load_checkpoint`。
- **`JsonBackend(storage_dir)`**：每 session 一个目录，`tick_<N>.json` +
  `firings.jsonl` + `checkpoints.json`。人类可读、易检查。
- **`SqliteBackend(db_path)`**：单个 SQLite 文件（WAL），含 `snapshots` /
  `firings` / `checkpoints` 表。更适合高 tick 吞吐；写入经由内部锁串行，
  因此一个实例可以安全地跨线程共享。这是默认后端（临时文件，自动清理）。
- **`NullBackend`**：内存快进模式——零磁盘 I/O、无每 tick 快照、无冷
  历史（窗口外的 `A[k]` 退化为 `Missing`）。适合测试和热点循环。

### 命名检查点

叠加在后端之上：

```python
rn.checkpoint("after_prep")        # 以标签保存当前状态
rn.list_checkpoints()              # [(label, tick), ...]
rn.rollback_to("after_prep")       # 恢复到该检查点
```

### 图重映射（热替换图结构）

回退之后，可以在继续运行前替换图结构和/或 registry：

```python
rn.rollback_to("safe_point")
rn.remap_graph(new_graph, registry=new_registry)
rn.run_until_idle(max_ticks=100)
```

两个图中都存在的槽位保留当前值；新槽位从 `False` 开始；被移除的槽位被
丢弃。`RunState` 会过滤掉新图中不存在的节点。

## AsyncRunner

对 body 需要 IO（LLM 调用、HTTP、DB）的图，使用 `AsyncRunner`。body 和
guard 可以是 `async def`；同一 tick 内的可激发节点经 `asyncio.gather`
**并发**激发。语义与同步 `Runner` 完全一致。

由于默认后端是临时 SQLite 文件，未显式传 `backend=` 的 `AsyncRunner`
会在每个 tick 结束时做一次小的同步 SQLite 写入——LLM 延迟占主导时无碍，
但零 I/O 的异步运行请传 `NullBackend()`。

```python
from tickflow.async_runner import AsyncRunner

@registry.body("harness")
async def harness(v):
    return await llm_client.chat(prompt=v.seed.value)

rn = AsyncRunner(graph, registry, backend=be, session_id="sess-1")
await rn.run_until_idle(max_ticks=100)
```

同步和异步的 body/guard 可以在同一图中混用。`Runner` 与 `AsyncRunner`
通过 `_BaseRunner` 共享全部状态管理逻辑。

## 可视化与前端集成

### 静态图导出

```python
graph.to_dict()      # 可 JSON 化的 {nodes, edges, starts}，供前端渲染
graph.to_mermaid()   # mermaid "graph TD" 文本（README / 调试器）
```

`to_dict()` 为每个节点附带派生的 `producers`，前端无需重算邻接。
`to_mermaid()` 将起始节点渲染为体育场形 `([name])`，普通边 `A --> B`，
守卫边 `A -->|g| B`。

### 可激发预览与节点状态

```python
rn.fireable()         # 下个 tick 会激发的 [节点名]
rn.node_states()      # {node: {状态 dict}} — 只读副本（如 attempts）
```

`fireable()` 与引擎内部检查的计算方式完全相同，因此能精确预测下一次
`tick()`。`node_states()` 返回 `RunState.all_mutable_states()` 的副本。

### Tick 生命周期钩子

```
on_tick_start(tick, fireable[])   # 本 tick 任何节点激发前
  → 引擎运行（body/guard 执行）
  → on_fire(ns) × N               # 每个节点激发后（收到 NodeState）
on_tick_end(tick, firings[])      # 所有激发提交后
```

### 审计日志

```python
for ns in rn.audit_log():          # list[NodeState]
    print(f"t{ns.tick} {ns.node}: {ns.inputs} → {ns.output} [{ns.status}]")

rn.audit_json()                    # JSON 字符串
```

每条 `NodeState` 记录包含 `inputs`（body 读到了什么）、`output`（它返回
了什么）、`edges_fired`（令牌如何传播）、`status`、`error` 和
`mutable_state`（body 运行后的节点状态快照）。

## CLI

```
python -m tickflow run      graph.txt -b beh.py [--max-ticks N] [--pause-at T ...]
python -m tickflow step     graph.txt -b beh.py [--from-snapshot snap.json] --ticks N
python -m tickflow snapshot graph.txt -b beh.py --out snap.json [--max-ticks N]
python -m tickflow audit    run.json
```

死锁建议：`--auto-promote` 全部接受，`--no-promote` 全部拒绝（并报错）。
在非交互环境（CI、脚本）中，CLI 默认报错并给出指引——绝不挂在 stdin 上
等待。

## 示例

| 示例 | 说明 |
|---------|-------------|
| `counter_loop` | 计数器小于 3 时循环，随后停止。循环 + 终止守卫 + OR-join。 |
| `xor_merge` | XOR 分支汇入 Merge。检查器标记 AND-join 死锁 → 提升为 OR。 |
| `retry_loop` | 用 `view.state` 计数实现重试。守卫同时读取输出与状态。 |
| `fan_out` | 并行 worker + AND-join 合流。三个分支并发激发。 |
| `pipeline` | 三级流水线，`A[k]` 索引策略固定 A 的首次激发。 |
| `state_machine` | 审批流：submit → review → approve/reject → done。XOR 分叉器。 |
| `checkpoint_restore` | 完整流程：检查点 → 回退 → 重映射 → 续跑。Python 脚本。 |
| `keep_records_false` | 省内存模式演示：无审计，但状态照常持久。Python 脚本。 |

## 设计说明

**为什么是 Petri 网而不是 FSM？** 最初的设计——每条入边一个布尔槽位、
槽位 AND 出可激发、激发即消耗、向下游产出——正是一个标记图（Petri 网的
子类）。FSM 是恰有一个存活令牌的特例；`tickflow` 允许多个并发起点和
AND/OR 汇合，这些是 FSM 原生表达不了的。环（循环）在 Petri 网中自然
存在，而在 DAG 调度器中被禁止。

**为什么是同步步进？** 它使 OR-join 可判定（无需回答"还可能有更多令牌
到来吗？"），并让快照成为平凡的一个 JSON dict——没有激发到一半的中间态
需要保存，暂停恰好落在 tick 边界上。

**为什么是分层 RunState？** 旧设计把 `History`、`audit` 列表和
`Marking.node_state` 作为三个独立且部分冗余的结构。分层的 `RunState`
（`_edges` + `_fire_counts` + `_state` + `_records`）把它们统一到唯一
属主下、职责清晰。`_edges` 和 `_state` 始终维护（引擎需要它们）；
`_fire_counts` 把窗口条目映射回 `A[k]` 序数；`_records`（详细审计）受
`keep_records` 与"无持久化后端"双重门控。

**刻意不做的事：** OR 之外的 inclusive/XOR-join 语法；分布式/多 worker
调度；内置时间线森林（用 `deepcopy`）；LLM token 流式输出（那是
harness/EventBus 的事——tickflow 只记录到节点粒度）。

## 项目结构

```
tickflow/
  ir.py           Node, Edge, Graph, InputPolicy, Failure
  parser.py       类 mermaid 文本 → Graph（带静态警告）
  checker.py      死锁检测、OR-join 提升、无守卫环检测
  state.py        NodeState, RunState — 所有运行时数据的单一事实来源
  engine.py       Marking, tick（纯函数）, join 逻辑
  runner.py       Runner, _BaseRunner（同步/异步共享逻辑）
  async_runner.py AsyncRunner（异步 body，并发激发）
  registry.py     body/guard 注册
  views.py        DictView, Resolved, Missing
  persistence.py  Backend 协议, JsonBackend, SqliteBackend, NullBackend
  cli.py          python -m tickflow ...
tests/            17 个文件，186 个测试
examples/         8 个示例（6 图 + 2 Python 脚本）
```

## 运行测试

```bash
python -m pytest tests/ -q
```
