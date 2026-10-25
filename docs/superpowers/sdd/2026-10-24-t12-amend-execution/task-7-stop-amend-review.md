# T12 Task 7：未触发 Stop Amend 独立审查

## 裁定

**Task 7 尚不通过。Critical：1；Important：1；Minor：0。**

已实现的 child 数量/限价、触发价、触发前到期迁移具有清晰的只读预检和提交边界，定向测试通过。但执行计划明确要求的 trigger-reference 与 protection 成功修改仍被全部拒绝；现有 DAY/GTD child 的触发执行缺口也没有被本小步补齐或在验收中证明可用。不能将报告中的显式 deferred 自动视为用户批准的范围缩减。

本次读取执行计划、Task 7 实施报告、Task 6 独立审查、真实 status/diff、未跟踪的两模型候选/Stop Amend/测试、StopBook、触发、取消、到期与 shadow 生命周期调用链。隔离 worktree 缺少 ignored `assignment/`，已从主 checkout `D:/rust_workspace/tide-matching-engine/assignment/` 读取工作规则、唯一交接、历史任务书及 v6 §20，并核对交接中用户冻结的数量、事件与 shadow 所有权决策。本次仅新增此审查文件，没有修改 Rust、提交或维护第二份 handoff。

## Findings

### Critical

1. **必须完成 trigger-reference 与 child protection 的合法修改，不能把一律拒绝作为 Task 7 的完成边界。**

   位置：`production_orderbook/amend_candidate.rs:529–534`、`reference_orderbook/amend_candidate.rs:474–479`；对应执行计划 `2026-10-24-t12-amend-execution.md:73`。

   两模型遇到任何 `new_trigger_reference: Some(_)` 或 `new_market_protection: Some(_)` 都返回 `InvalidOrderCombination`。因此合法的 reference 变更无法分配新 StopPriority 并迁移来源索引，child protection 的 Keep/Clear/Set 也全部不可用；尤其 Keep 本应保留现有保护，当前却会连同一个原本合法的数量/到期补丁一起拒绝。`tests/amend_stop.rs:398` 将这些情况固定成拒绝，只证明拒绝原子性，不能作为规格成功矩阵的证据。

   v6 §20.3 明确要求 child protection 修改保 StopPriority、trigger_reference 改变丢 StopPriority 并迁入新来源对应容器；本轮批准的 Task 7 计划重复了这两项要求。`StopOrder` 基线没有这两个运行态字段以及来源分区，确实解释了依赖缺口；选择显式拒绝也优于静默忽略，但没有任何已读的用户冻结决策授权删除这两项要求。故这是 **Critical 规格覆盖缺口**，而不是可自行认可的实现范围边界。

   修复方向：在两模型各自建立能保存并执行这两个字段的运行态和来源路由，完整预检后应用；reference-only change 必须移入新来源目标档尾部并只分配一个 StopPriority，未改变实际来源则保序；保护修改只更新 child 模板并保序。补上成功矩阵、来源隔离、原序事件、真实 child 保护执行，以及非法来源/保护/高水位的完整失败快照。继续保持 Reference 的独立 Vec 模型。若父会话确需缩减此规格，必须将其保留为未完成项并由有权范围决策解决，不能仅修改报告称为完成。

### Important

1. **DAY/GTD child Amend 成功后仍不能执行；当前 TIF 矩阵只验证模板保存，没有验证合法触发。**

   位置：`production_orderbook/amend_stop.rs:41`、`reference_orderbook/amend_stop.rs:85`；两模型 `stop_trigger.rs:124–151` 附近；`tests/amend_stop.rs:751`。

   Amend 与 StopBook 预检接受合法 `TimeInForce::Day/Gtd` 并输出成功 `OrderAmended`，但两模型 `validate_triggered_child_tif` 仍只接受 StopLimit GTC，后续执行仍调用 `place_limit_gtc_with_events`。例如在 now=10 把未触发 Stop 的 child 改成 GTD=40，之后在 20 前触发，虽然 child 尚未过期，仍会立即 `TriggeredOrderRejected(InvalidOrderCombination)`，连 Trigger-R1 都不会调用；DAY 同样如此。测试中的九组 child TIF 迁移均通过触发前 GTD=20 先到期，没有触发 DAY/GTD child，唯一真实触发测试使用 GTC。

   这是 T10 已记录的继承缺口，不是本次新改触发逻辑产生的回归；但在当前完整 Amend 目标下，不能把 DAY/GTD child 的字段保存描述成对应有效期行为已经支持。修复应让触发阶段取得明确的逻辑时间/当前 session，独立重校验 child 时效，并通过 timed child 下单路径维护实际 residual 的 regular expiry 索引。增加两模型真实 DAY/GTD 触发、余量到期、到期/错 session 触发拒绝和清理测试。若作为另一个经批准小步处理，当前应明确保留其未完成状态，不能由现有模板矩阵替代此证据。

### Minor

无。

## 已确认的正确性与质量

- **模型独立：** Reference 直接扫描自身源 Vec，`ReferenceStopAction` 持有新载荷、源位置、目标 StopPriority、下一计数器和事件；Production 自行读取 StopBook，以自身 `StopReplacementPlan` 维护价档、位置和到期索引。没有调用另一模型的业务迁移 helper，也没有把生产输出作为参考答案。共享领域模式校验只表达协议。
- **写屏障：** Production 所有可返回的业务错误，包括 shadow/身份、模式、零预算、child TIF、触发前到期和 StopPriority 高水位，都发生在 `commit_replacement` 之前；事件也预先构造。提交函数不返回普通业务 `Err`，只消费计划并以不变量断言定位/分配结果。单 writer 使预检到提交期间不存在外部状态漂移。Reference 同样先生成完整动作再一次提交。
- **已实现优先级矩阵：** child 数量增减、child 限价/TIF、触发前到期、相同 stop_price 保留原位置/StopPriority；实际改变 stop_price 时分配一次新 StopPriority 并进入已有目标档尾部。两模型未触发 Amend 均不分配 QueuePriority。高水位仅阻止需要重新分配的路径，纯 child/expiry 修改仍能成功。
- **数量模式与事件：** 未触发 Stop 没有已成交量，`new_qty` 直接作为严格正 child 目标量；quote 模式仅使用严格正 `new_quote_budget`，不会切换为 Base。Market child 拒绝限价字段。成功仅产生一个同 OrderId 的 `OrderAmended`，Limit price 是 child 限价，Market 是 None，BaseRemaining/QuoteBudget 保持类型；`priority_retained` 对应 StopPriority。无 Trade 或 OrderReplaced。
- **到期索引迁移：** stop_price 重排路径删除旧位置和旧到期引用，再插入新位置对应引用；不重排路径只在有效期变化时替换引用。实际 GTC/GTD/DAY 矩阵、GTD 20→30 和真实 EndSession/expiry 调用覆盖旧引用删除、新引用可达和 GTC 无引用；StopBook validator 反向检查引用的载荷、位置及唯一性。
- **聚合到期 shadow 修复：** Production 聚合 `expire_gtd_through` 与 `end_session` 现调用带 shadow 清理的 Stop wrapper，保留原有 GTD 时间/ID 与 DAY ID 排序；Reference 自身扫描终结时清理 shadow。此修改与新增 reservation-aware Stop 生命周期直接相关，范围合理。
- **取消与触发清理：** Production `cancel_stop` 和 Reference 对应删除同步清理 shadow；Production 触发收集从 StopBook/expiry 删除后清理 shadow，Reference 复用自己的 cancel 路径。已有真实 GTC 触发测试证明修改后的模板进入 Trigger-R1、StopPriority 结束、只有实际 residual rest 获得新的 QueuePriority，旧 trigger expiry 不会再终结该单。实际 child rest 的 reservation 接续尚留待 Task 8，应继续保持该边界说明。
- **失败原子性：** Pair 在每次拒绝前后比较两模型各自完整状态快照；Production 包含真实 StopBook/expiry、Arena/free-list、价档、按 ID 排序的身份/shadow、参考价和两个计数器，Reference 包含自身源 Vec、shadow、参考价和计数器。成功与失败均运行 regular book、regular expiry 和 StopBook validator；没有用事件排序掩盖业务顺序。Debug 投影只是同实例结构相等证据，不冒称 canonical digest。
- **shadow/token 阶段边界：** legacy Stop 缺少 shadow 时明确拒绝，没有生成默认 reservation；携带调整 token 时在写屏障前拒绝，成功保留既有 shadow/version。本小步尚未核算 coverage 或应用真实调整，这与执行计划 Task 8 的显式整合顺序一致，不能外推为 v6 §20.1 的最终资金验收。
- **工程约束：** 新说明注释为中文；无新增依赖、数值换算公式、浮点、unsafe、wall clock、锁、网络/RPC、无界队列或 I/O。StopPriority 使用 checked allocator，HashMap 只参与身份查询与校验，不决定事件/队列顺序。WAL/crash/snapshot/durable replay-digest 不属于本小步，未伪造相应验证。

## 本次实际验证

2026-10-25 独立运行：

- `cargo test -p matching-orderbook --locked amend_stop --lib --quiet`：11 passed，0 failed，474 filtered，exit 0。

本次没有修改 Rust，未重跑实施者已记录的完整 485 项 orderbook、Clippy/fmt 或最终 workspace gate；这些仍是实施者另次运行证据。上述两个 Findings 由真实调用链直接确定，不需要新增临时业务源码来证明。本次定向全绿只覆盖现有断言，不能覆盖缺失的成功 reference/protection 路径或 DAY/GTD 实际触发。
