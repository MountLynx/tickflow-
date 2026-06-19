# ModuleHarness 重构报告

> 本报告基于对 `C:\Users\xingy\Desktop\开发\ModularHarness` 全部相关源码的阅读，给出用
> `flow`（`C:\Users\xingy\Desktop\开发\Graph\flow`）重构会话/快照/编排层的方案。
> **本报告只描述方案，不改动 ModularHarness 任何文件。** 实施待第二阶段。

## 1. 现状四个文件的职责与问题

### 1.1 `engine/orchestrator.py` — DAG 编排引擎

**职责**：`_build_dag` 把 `Spec.tasks`（带 `depends_on`）建成 DAG → `_topological_layers`
用 Kahn 算法分层 → `DAGExecutor.execute` 层内 `asyncio.gather` 并行、层间串行 →
`_validate_layer` 合规校验重试 → 基础设施错误终止整图。`Orchestrator` 是对外 API，
负责 Session 生命周期、事件发射、`execute_resume`。

**问题**：
1. **死代码 / 未接线**：`DAGExecutor.__init__` 不接收 `mode` / `session_store`，但
   `_run_layer` 引用 `self._mode` 和 `self._session_store`（第 ~行 `if self._mode.value
   == "tracked"`）。TRACKED 模式下这里会 `AttributeError`，说明进度更新实际由
   `HarnessRunner` 自己做（harness_runner.py 第 151 行的 mode 守卫），`_run_layer` 的
   `update_progress` 调用是死代码。
2. **合规重试与流程控制混在一起**：`_validate_layer` 内嵌 `for attempt in
   range(max+1)` 循环 + 重新 `runner.run`，这是流程控制逻辑硬编码在执行器里，不可复用、
   不可快照中间状态。
3. **恢复靠"跳过已完成 task"**：`execute_resume` 把已完成 task 标 `COMPLETED`，
   `DAGExecutor` 检测到就跳过。这是 ad-hoc 的，没有真正的 marking/快照语义，崩溃在
   层中间无法恢复。
4. **`RouteDecider` 与 `Router` 两套路由**：`RouteDecider` 是回调选 harness，
   `Router` 做合规——两者都在 orchestrator 里纠缠。

### 1.2 `engine/session_store.py` — Session 持久化

**职责**：磁盘布局 `sess-{id}/{manifest,spec,session,tasks/,records/*.jsonl}`，
L1/L2/L3 三层记录，异步 debounce 写 session.json + 同步 fsync 写 JSONL，
`recover_session` 重放 JSONL 重建计数 + 自动修复状态机。

**问题**：
1. **两套恢复机制冲突**：`recover_session` 重放 L3 JSONL 重建 L1 计数和状态机；而
   `SnapshotManager` 是另一套"结果快照"机制。两者语义重叠且不一致（JSONL 重放只恢复
   计数，不恢复 marking/中间状态；snapshot 恢复 spec+results 但丢 Spec 对象）。
2. **状态散布**：Session 状态（计数、current_task）在 session.json，task 状态在
   tasks/*.json，调用记录在 records/*.jsonl——三处需要保持一致，`recover_session` 的
   "自动修复"是对这种不一致的补丁。
3. **过度设计**：debounce + per-session lock + run_in_executor，对小规模流程过重。
4. **L3 记录粒度低于流程节点**：LLM/Tool/Script 调用记录是 HarnessRunner 内部产生的，
   硬塞进 SessionStore 让它同时承担"会话状态"和"调用日志"两个职责。

### 1.3 `snapshot/manager.py` — 检查点管理

**职责**：`Snapshot(spec, results, metadata, label)` 打包落盘 `snap_*.json`，
`create/restore/rollback_to/list_labels/clear`。

**问题**：
1. **回滚不删盘**：`rollback_to` 只截断内存 `_snapshots` 列表，不删后续 `snap_*.json`
   文件；下次 `_load_existing` 又会把它们读回来。
2. **Spec 丢失**：`_load_existing` 从 dict 恢复时把 Spec 对象设 None（信息损失），
   `restore` 返回的 spec 不可直接用。
3. **快照与流程脱节**：快照是"某时刻所有 task 结果的不可变切片"，不感知执行顺序/marking，
   回滚后只能靠"跳过已完成 task"重新执行，无法精确恢复到某个 tick 的中间状态。
4. **与 SessionStore 的 spec 来源不一致**：snapshot 存 spec，session.json 也存 spec，
   两者可能不同步。

### 1.4 `models/session.py` — 数据模型

**职责**：`Session`（L1）/`TaskRecord`（L2）/`{LLMCallRecord, ToolCallRecord,
ScriptExecRecord}`（L3）纯 dataclass + 序列化。

**问题**：
1. **L1/L2 与 L3 耦合在一个文件**：L3 是调用级日志（RecordStore 该管），L1/L2 是流程级
   状态（flow 该管），混在一起导致 SessionStore 必须同时处理两者。
2. **Session 字段冗余**：`total_input_tokens` 等计数本可从 L3 聚合，却冗余存一份，
   导致 `recover_session` 要重放修复。
3. `TaskRecord` 是 `HarnessResult` 的超集，两者字段重复。

---

## 2. 重构目标（对齐 重构.md）

| 重构.md 原则 | 落地方式 |
|---|---|
| "一个 module 的运行就是一个进程" | 一次 `AsyncRunner.run_until_idle` = 一次进程 |
| "整个进程进行过程记录" | flow `Backend.save_firing`（节点级）+ `RecordStore`（调用级） |
| "快照粒度：一个 tick" | flow `Backend.save_snapshot` 每 tick 自动落盘 |
| "script 就是普通 node" | script handler 作为 flow node 的 body，不加特殊钩子 |
| "harness 是 node 的 body" | 每个 TaskItem → flow node，`body = harness_runner.run(harness)` |
| "spec 与 tasklist 两种输入模式" | spec+tasklist 直接转 flow graph；纯 spec 走 translator（v2） |

---

## 3. 新架构

```
CLI / MCP Server
      │
      ▼
ModularHarnessSDK (sdk.py)  ── 组装 ──► FlowOrchestrator + RecordStore + EventBus + Backend
      │                                       │
      │ run_module / get_session_status /     ├── FlowOrchestrator (engine/flow_orchestrator.py)
      │ rollback_session / cancel_session     │     ├─ spec.tasks → flow graph
      ▼                                       │     ├─ AsyncRunner(graph, registry, backend, session_id)
FlowOrchestrator                               │     ├─ on_fire 适配器 → EventBus + RecordStore
      │                                       │     └─ on_tick_end 适配器 → checkpoint
      ├── spec.tasks → flow Graph             │
      ├── Registry: body=harness_runner.run   │
      ├── AsyncRunner (flow.async_runner)     RecordStore (engine/record_store.py)
      │     ├── on_fire → flow_adapter        │     ├─ record_llm_call / record_tool_call
      │     │        → EventBus + RecordStore │     └─ record_script_exec  (L3 JSONL/SQLite)
      │     └── backend → JsonBackend         │
      │                (tick 级快照 + firings) ◄── EventBus (events/event_bus.py, 保留)
      │
      └── HarnessRunner (engine/harness_runner.py, 基本不动)
              └─ 写 RecordStore（替代原 SessionStore 的 L3 写入）
```

### 3.1 新增文件

| 文件 | 职责 |
|---|---|
| `engine/flow_orchestrator.py` | 用 flow 重写编排，保持 `Orchestrator` 公开契约 |
| `engine/flow_adapter.py` | `Firing` → EventBus 事件 + RecordStore 写入的适配层 |
| `engine/record_store.py` | 纯调用级日志（LLM/Tool/Script），从 session_store 拆出 |
| `engine/tasklist_translator.py` | spec → flow graph 文本翻译（v1 留空，支持 spec+tasklist 输入） |

### 3.2 删除/替换/精简

| 旧文件 | 处置 |
|---|---|
| `engine/orchestrator.py` | **删除**，由 `flow_orchestrator.py` 替代 |
| `engine/session_store.py` | **拆分删除**：状态持久化 → flow `Backend`；L3 日志 → `record_store.py` |
| `snapshot/manager.py` | **删除**，由 flow `checkpoint` 机制替代 |
| `models/session.py` | **精简**：保留 L3 dataclass 给 RecordStore；L1 `Session`/L2 `TaskRecord` 改为从 flow snapshot 派生的只读视图 |

### 3.3 保留不动

`harness_runner.py`（仅把 `SessionStore` 依赖换成 `RecordStore`）、`router.py`、
`event_bus.py`、`loader.py`、`models/{spec,harness_def,module_def}.py`、`llm/`、`tools/`、
`cli.py`、`mcp_server.py`、`sdk.py`（内部装配改用新组件，对外契约不变）。

---

## 4. spec.tasks → flow graph 转换规则

`FlowOrchestrator._spec_to_graph(spec, module_def) -> Graph`：

| Spec 元素 | flow graph 表达 |
|---|---|
| `TaskItem` | 一个 flow node，name = task.id，`body = harness_runner.run(harness, inputs)` |
| `task.depends_on = [d1, d2]` | 普通边 `d1-->task`、`d2-->task`（AND-join 天然"等所有上游"） |
| 无 depends_on 的 task | `[task]` 标记为起点 |
| `task.input_data` | 注入 body 的全局 input（通过 node_state 或 registry 闭包） |
| `RouteDecider(task, carry) -> harness_name` | 在 body 内部调用，选 harness（**不**建模成多分支，避免按 task 爆炸图） |
| 合规校验重试 | node 的 self-loop `task--|compliance_ok|-->task`，guard 调 `Router.check_compliance`，body 内 `view.state["attempts"] += 1`，guard 读 `view.state["attempts"] < max_retries` |
| 基础设施错误 | body 返回 `Failure(error, type="infrastructure")` → flow 自动 ABORTED |
| LLM 错误 / 合规失败 | body 返回 `Failure(error, type="llm")` → 下游 AND-join 不满足 = 跳过 |

**graph 文本示例**（由 translator 生成或手写）：
```
[task_prep]-->task_extract
[task_prep]-->task_transform
task_extract-->task_merge
task_transform-->task_merge
task_merge.body: run_harness_merge
task_merge--|compliance_ok|-->task_merge
task_merge.join: OR
```

**body 注册**（在 FlowOrchestrator 装配时）：
```python
for task in spec.tasks:
    harness = module_def.get_harness(task.harness_name)
    async def make_body(harness, task):
        async def body(v):
            inputs = _merge_inputs(v, task)        # 上游输出 + task.input_data
            if route_decider:
                harness = module_def.get_harness(route_decider(task, _carry(v)))
            result = await harness_runner.run(harness, inputs, spec, session_id)
            v.state["attempts"] = v.state.get("attempts", 0) + 1
            if result.error_type == "infrastructure":
                return Failure(result.error, type="infrastructure")
            if result.status != HarnessStatus.COMPLETED:
                return Failure(result.error or "failed", type="llm")
            return result.output
        return body
    registry.body(task.id, make_body(harness, task))
```

**guard 注册**：
```python
async def make_compliance_guard(task, router, spec):
    async def guard(v):
        if v.state.get("attempts", 0) >= task.max_retries:
            return False  # 重试上限，停止循环
        result = _last_result(v)  # 从 v 读本次 body 产出的 HarnessResult
        ok, reason = await router.check_compliance(task, result, spec)
        return not ok  # 不合规 → True（继续循环重试）；合规 → False（停止）
    return guard
registry.guard("compliance_ok", await make_compliance_guard(...))
```

> 注意：`Router.check_compliance` 是 async，flow 的 `AsyncRunner` 支持 async guard，
> 完美匹配。同步 `Runner` 不适用于含 LLM 调用的 harness 流程。

---

## 5. 对外契约保持清单

以下契约被 SDK / CLI / MCP 依赖，重构后必须保持可用：

### 5.1 Orchestrator（被 sdk.py / cli.py 调用）
- `Orchestrator.from_module(module_def, api_config, tool_registry=, compliance_api_config=, session_store=, mode=, event_bus=) -> Orchestrator`
- `async def execute(module_def, spec, input_data=, override_session_id=) -> dict`（返回值含 `_session_id`）
- `async def execute_resume(module_def, spec, session_id, input_data=) -> dict`
- `set_route_decider(decider)`
- `on_layer_complete(callback)` → 适配为 `on_tick_end`（tick ≈ 拓扑层）
- `visualize(spec) -> str`
- 属性 `runner`、`router`

### 5.2 SDK 4 操作（被 mcp_server.py / cli.py 调用）
- `run_module(module_file, spec, *, event_callback, dev_mode, dev_options, override_session_id) -> RunModuleResult`
- `get_session_status(session_id, *, verbosity, task_filter, since_timestamp) -> SessionStatusResult`
- `rollback_session(session_id, snapshot_id, new_spec) -> RollbackResult`
- `cancel_session(session_id, reason) -> CancelResult`
- `list_sessions(state_filter) -> ListSessionsResult`
- `create_snapshot(session_id, label) -> str`
- 同步包装 `run_module_sync` 等

### 5.3 CLI / MCP
- `cli.py` 命令不变
- `mcp_server.py` 调 SDK，不变

### 5.4 数据契约
- `result["_session_id"]` 注入
- `RunModuleResult` / `SessionStatusResult` / `RollbackResult` / `CancelResult` 字段不变
- 4 级 verbosity（summary/progress/debug/trace）语义不变

---

## 6. 持久化方案

### 6.1 flow Backend 取代 session_store 多层布局

| 旧 session_store | 新 flow Backend |
|---|---|
| `manifest.json` + `spec.json` | `Backend` 不存 spec（spec 由 FlowOrchestrator 持有，从 module_file 重载） |
| `session.json`（L1 计数+状态） | flow `snapshot()` 的 `marking` + `status` |
| `tasks/*.json`（L2 TaskRecord） | flow `history`（节点输出）+ `firings.jsonl`（Firing 记录） |
| `records/*.jsonl`（L3 调用日志） | `RecordStore`（独立，见 6.2） |
| `recover_session`（JSONL 重放） | `Backend.load_snapshot(latest_tick)` + `restore`（权威恢复源） |

**布局**（`JsonBackend`）：
```
{storage_dir}/{session_id}/
├── tick_<N>.json     # 每 tick 的完整快照（marking + history + status + node_state）
├── firings.jsonl     # 节点级 fire 记录（全过程记录 - 节点级）
└── checkpoints.json  # 命名快照（label -> snapshot）
```

### 6.2 RecordStore 取代 L3 JSONL

从 session_store 拆出纯调用级日志，HarnessRunner 直接写：

```python
class RecordStore:
    def __init__(self, storage_dir: Path): ...
    async def record_llm_call(self, session_id, record: LLMCallRecord) -> None
    async def record_tool_call(self, session_id, record: ToolCallRecord) -> None
    async def record_script_exec(self, session_id, record: ScriptExecRecord) -> None
    async def record_event(self, session_id, event: HarnessEvent) -> None
    def get_llm_calls(self, session_id, task_id=None) -> list[LLMCallRecord]
    def get_tool_calls(self, session_id, task_id=None) -> list[ToolCallRecord]
    def get_script_execs(self, session_id, task_id=None) -> list[ScriptExecRecord]
    def get_events(self, session_id, event_filter=None) -> list[HarnessEvent]
```

- 磁盘：`{storage_dir}/{session_id}/records/{llm_calls,tool_calls,script_execs,events}.jsonl`
  （与原布局一致，迁移成本低）
- HarnessRunner 把 `self._session_store.record_*` 全换成 `self._record_store.record_*`
- RecordStore 不参与状态恢复（只做审计）；状态恢复由 flow Backend 的 snapshot 负责

### 6.3 checkpoint 取代 SnapshotManager

```python
# 旧
snap = snapshot_manager.create(spec, results, label, metadata={"session_id": sid})
snapshot_manager.restore(snap_id)  # 返回 (spec, results)
snapshot_manager.rollback_to(snap_id)

# 新
runner.checkpoint(label)            # 存当前 flow snapshot
runner.list_checkpoints()           # [(label, tick)]
runner.rollback_to(label)           # restore 到该 tick
```

- flow checkpoint 是真正的 tick 级中间状态快照（含 marking），不是"结果切片"
- 回滚后 `restore` 把 marking/history 恢复，继续 `run_until_idle` 即从该 tick 续跑
- 不再有"回滚不删盘"问题（Backend 的 checkpoint 是覆盖写）

---

## 7. resume / rollback 新实现

### 7.1 execute_resume（崩溃恢复 / 续跑）

```python
async def execute_resume(self, module_def, spec, session_id, input_data=None):
    # 1. 从 Backend 加载最新 tick 的 snapshot
    latest = self._backend.latest_tick(session_id)
    if latest is None:
        raise ValueError(f"no snapshot for session {session_id}")
    snap = self._backend.load_snapshot(session_id, latest)

    # 2. 构建 flow graph + 注册 bodies
    graph = self._spec_to_graph(spec, module_def)
    registry = self._build_registry(spec, module_def, session_id)

    # 3. 创建 AsyncRunner 并 restore
    rn = AsyncRunner(graph, registry, backend=self._backend, session_id=session_id)
    rn.restore(snap)   # marking/history/tick 恢复，status 重置为 IDLE

    # 4. 注册 on_fire 适配器
    self._wire_hooks(rn, session_id)

    # 5. 续跑
    await rn.run_until_idle(max_ticks=self._max_ticks)

    # 6. 收集结果
    return self._collect_results(rn, session_id)
```

无需"跳过已完成 task"——restore 后 marking 已反映哪些节点 fire 过，flow 自动只跑剩余的。

### 7.2 rollback_session（回滚到检查点）

```python
async def rollback_session(self, session_id, snapshot_label, new_spec=None):
    # snapshot_id 在新方案里是 checkpoint label
    rn = self._build_runner(session_id, new_spec or current_spec)
    rn.rollback_to(snapshot_label)   # restore 到该 checkpoint
    session.status = RUNNING; session.resumed_at = now
    await rn.run_until_idle(max_ticks=self._max_ticks)
    return RollbackResult(...)
```

### 7.3 cancel_session

```python
async def cancel_session(self, session_id, reason):
    # 若 runner 在跑：rn.cancel(reason)；否则只改 session 状态
    self._active_runners.get(session_id)?.cancel(reason)
    session.status = CANCELLED
```

---

## 8. FlowOrchestrator 与外部组件的接缝

### 8.1 on_fire → EventBus + RecordStore

```python
def _wire_hooks(self, rn, session_id):
    async def on_fire(firing: Firing):
        if firing.status == "ok":
            await self._event_bus.emit_task_completed(
                session_id, firing.node, firing.node,
                duration_s=...,  # 从 firing 推算
            )
        elif firing.status == "failed":
            await self._event_bus.emit_task_failed(
                session_id, firing.node, firing.error or "",
            )
        elif firing.status == "aborted":
            await self._event_bus.emit_session_failed(
                session_id, firing.error or "", task_id=firing.node,
            )
        # RecordStore 的 L2 TaskRecord 写入也在这里
        await self._record_store.record_task(session_id, _firing_to_task_record(firing))

    async def on_tick_end(tick, firings):
        # 层完成回调（兼容旧 LayerCallback）
        if self._on_layer:
            self._on_layer(tick, _firings_to_results(firings), self._spec)

    rn.on_fire(on_fire)
    rn.on_tick_end(on_tick_end)
```

### 8.2 Router / RouteDecider

- **RouteDecider**：在 body 内部调用（见第 4 节），不进 flow 结构
- **Router.check_compliance**：在 compliance guard 内部调用（async guard 匹配 async Router）

### 8.3 Session 视图

`Session` 改为从 flow snapshot + RecordStore 派生的只读视图：
```python
class SessionView:  # 替代旧 Session dataclass 的查询用途
    @property
    def status(self) -> SessionStatus:
        return _flow_status_to_session(self._runner.status)
    @property
    def progress(self) -> float:
        return len(self._runner.history) / self._total_tasks
    @property
    def total_input_tokens(self) -> int:
        return sum(r.input_tokens for r in self._record_store.get_llm_calls(self._sid))
    # ... completed_tasks / failed_tasks / current_task_id 等
```

`get_session_status` 的 4 级 verbosity 从 `Backend`（snapshot）+ `RecordStore`（L3）聚合。

---

## 9. 迁移步骤与端到端验证

### 9.1 迁移步骤
1. 新建 `engine/record_store.py`，从 session_store 拆出 L3 日志方法，独立测试。
2. `harness_runner.py`：`self._session_store` → `self._record_store`，删 L2 TaskRecord 写入（改由 flow_adapter 的 on_fire 写）。
3. 新建 `engine/flow_orchestrator.py` + `engine/flow_adapter.py`，实现 `Orchestrator` 契约。
4. 精简 `models/session.py`：保留 L3 dataclass；L1/L2 改为 SessionView 派生类。
5. 删除 `engine/orchestrator.py`、`engine/session_store.py`、`snapshot/manager.py`。
6. `sdk.py`：装配改用 FlowOrchestrator + RecordStore + flow Backend；`rollback_session` 改用 `runner.rollback_to`；`get_session_status` 改用 Backend+RecordStore 聚合。
7. `cli.py`：`SnapshotManager` 引用换成 flow Backend。

### 9.2 端到端验证清单
- [ ] INLINE 模式：纯内存 flow，0 IO，跑通 examples agent
- [ ] TRACKED 模式：flow Backend 落盘 + RecordStore 日志 + EventBus 事件
- [ ] resume：跑到一半 kill 进程 → 重启 → `execute_resume` 从 latest tick 续跑，结果与一次性跑完一致
- [ ] rollback：`checkpoint("cp")` → 改 spec → `rollback_to("cp")` → 重跑剩余
- [ ] cancel：跑中途 `cancel_session` → status=CANCELLED，ticks 停止
- [ ] 合规重试：故意产出不合规输出 → guard 触发 self-loop 重试 → 达 max_retries 停止
- [ ] infra 错误：模拟网络错误 → body 返回 infra Failure → status=ABORTED，下游不跑
- [ ] 4 级 verbosity 查询返回正确数据
- [ ] MCP server 调 SDK 4 操作不变
- [ ] CLI 命令不变

---

## 10. 范围外（本次重构不做）

- **spec → tasklist 自动翻译**：`tasklist_translator.py` 留空实现；只支持 spec+tasklist 输入模式。纯 spec 模式待 v2。
- **SqliteBackend**：先用 JsonBackend 跑通；SQLite 后续作为另一种 Backend 实现（同一协议）。
- **submodule 嵌套执行优化**：submodule 先当一个普通 flow node 跑（body 内递归建子 flow），不做专门的嵌套调度优化。
- **Router LLM 路由策略改进**：保持现有 RULE/LLM/HYBRID 不变。
- **图可视化反向生成**（mermaid）：后加。
- **分布式 / 多 worker**：单进程同步/异步。
- **EventBus 重构**：保留现有 6 类事件设计，仅发射点迁移到 on_fire。
- **LayerCallback 语义升级**：保持 `Callable[[int, dict, Spec], None]`，由 on_tick_end 适配，不引入新的层概念。

---

## 附：flow 已具备的能力清单（第一阶段交付，支撑本重构）

| 能力 | flow API | 重构用途 |
|---|---|---|
| 异步 body + 并发 fire | `AsyncRunner` | harness.run 是 async，层内并行 |
| 失败语义 | `Failure(type="llm"/"infrastructure")` | LLM 错误跳过下游 / infra 错误终止 |
| 运行状态机 | `RunStatus` + `cancel()` | cancel_session / ABORTED |
| 节点可变状态 | `view.state` + `Marking.node_state` | 合规重试计数器 |
| 钩子 | `on_fire` / `on_tick_end` | EventBus / RecordStore / 层回调 接缝 |
| 持久化后端 | `Backend` + `JsonBackend` | tick 级快照 + 过程记录 |
| 命名快照 | `checkpoint()` / `rollback_to()` | 取代 SnapshotManager |
| 快照/恢复 | `snapshot()` / `restore()` | resume / what-if |

测试：89 个全过（42 原有 + 47 新增），覆盖 async/failure/status/node_state/hooks/persistence/checkpoints。
