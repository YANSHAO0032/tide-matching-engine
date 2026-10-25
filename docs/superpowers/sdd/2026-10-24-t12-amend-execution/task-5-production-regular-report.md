# T12 Task 5：Production 常规改单报告

## 本轮范围与起始事实

在隔离 worktree `codex/t12-amend` 中完成 Production 常规 Amend，不提交、推送、合并或修改 main。
起始状态已经有前次未验收的 Production Amend 草稿，以及 `OrderNode`/`PriceLevel` 原位减量原语；
本轮先添加真实结构测试并运行，再替换不满足预检/提交边界的草稿，未将这些已有实现冒称本轮从零 TDD。

本轮读取执行计划、Task4 报告、两次审查、真实源码及 git diff/status。
worktree 没有 ignored `assignment/`；读取主 checkout `D:/rust_workspace/tide-matching-engine/assignment/`
中的工作规则及唯一交接，不另建第二个 handoff。Task5 的交接由本报告提供给父执行会话更新。

## RED → GREEN

1. 初次新增 9 项 Production 定向测试后，运行
   `cargo test -p matching-orderbook --locked amend_regular --lib`，exit 1：18 passed / 2 failed。
   无成交改价错误多发 `OrderPartiallyFilled`（事件长度 2，而预期 1）；冰山 maker 的 3 片成交
   被普通 `FillPlan` 错算为 1 笔，执行阶段触发 `insufficient TradeIds for iceberg batch`。
2. 追加冰山 TradeId/补量优先级原子性测试后，运行
   `cargo test -p matching-orderbook --locked production_orderbook::tests::amend_regular --lib`，
   exit 1：7 passed / 3 failed。`catch_unwind` 后完整快照显示旧单已经从 Arena、ID 索引、
   买侧深度和 shadow reservation 删除，证明旧草稿在取消后才检查冰山 TradeId 容量。
3. 使用 Production 自己的只读片成交规划，先冻结全部 fills、补量 priority、TradeId 数量及 residual
   RestPlan，再进入不返回 `Result` 的提交函数。原 10 项 Production 测试转绿。
4. 绿色后补充头/中/尾/单节点重排，以及 priority ceiling 上全成无需 residual allocation 的直接证据。
   这些补充测试没有引入新业务修改，不冒称它们曾观察到 RED。

## 实现与提交边界

- `production_orderbook/amend_candidate.rs` 消费独立 Production candidate；不调用 Reference helper。
- 同价减量通过既有 PriceLevel 原语原位更新最新绝对总量、正余量及 TIF；保持 Arena 槽位、
  ID 索引、双向链接、FIFO 和 QueuePriority。
- 同价增量先以旧单移除后的计数、聚合量及实际尾节点建立 `RestPlan`；提交时解除旧链接、
  建立保存完整绝对总量的替换节点，并只分配一个新 priority。覆盖旧单原为头、中、尾和唯一节点。
- 改价的 `RegularRepriceCommitPlan` 保存 candidate、目标载荷、生产片成交计划及可选正 residual。
  它在首次结构写入前完成所有可返回业务拒绝、checked 数量、TradeId 容量、冰山补量优先级、
  residual priority 和目标档聚合/计数预检。
- `commit_amend_reprice` 仅消费已冻结计划，返回 `Vec<EngineEvent>`，没有提交后的业务 `Err`。
  复用 Production 的 Arena/PriceLevel/ID 索引和自身片成交提交；Reference 仍使用自己的 Vec 规划。
- 删除旧单保证至少一个可复用 Arena 槽位，不需要删除前额外容纳一个节点；对手侧成交不会
  增加本侧挂单需求。提交仍通过既有 `commit_rest_target` 比较目标快照并维护结构断言。
- 全成不构造 `Qty(0)`、不为目标分配 residual priority、清除目标 shadow；部分成交或重新挂单
  保留完整原 shadow 内容/version。被全成 maker 沿用 Production 的终结清理。
- 输出固定为 `OrderAmended → Trade* → existing Order*`，改单事件保留重新撮合前候选余量；
  无成交只发 `OrderAmended`，不会虚构部分成交。冰山 maker 原有生命周期与补量事件保留。
- GTC/GTD/DAY 使用真实输入逻辑时间和会话完成候选预检，提交原子迁移普通到期索引；
  纯 TIF 变化保留原 slot/FIFO/priority。Hard Gate A 仍由共享目标总量校验严格拒绝等于已成交量。
- `ReservationAdjustmentToken::Some` 与缺失 shadow 仍明确拒绝 `InvalidReservation`；
  不创建假执行上下文、不 verify/apply reserve，也不递增 reservation version。

## 可见性裁定与 lint

最初保留两模型的 crate-private 真实入口时，strict Clippy 如实报 15 项 `dead_code`：
尚无非测试命令路由，整条内部调用链均被判定未用。父执行会话随后裁定把两模型的
`amend_regular_with_events` 提升为正式 `pub` 订单簿 API，与已有 public place/rest 风格一致。

本轮仅调整两套真实入口及其中文 rustdoc，不增加 preview、re-export、lint allow 或测试专用生产接口。
Candidate、plan 和 build helper 继续内部可见。该裁定提前固定 regular Amend 公开 surface，
Rustdoc 明确绝对总量、队列动作、事件顺序、当前 shadow 边界、尚不支持的改单类型及 TradeId fail-stop。
此后 strict affected Clippy 已通过，原 Task3+4 lint coupling 在本小步解除。

## 测试覆盖与最终证据

Production 新增 12 项测试：双向减量及部分成交后二次绝对目标、双向增量尾插、
头/中/尾/唯一节点重排、无成交改价、双向部分/完全成交与 shadow 终结、冰山 maker 切片事件顺序、
GTC/GTD/DAY 全部九组类型转换、Hard Gate A/非法时效/数量模式/未验签 token/缺失 shadow 拒绝、
priority 与聚合溢出拒绝、ceiling 上全成、普通及冰山 TradeId 不足和补量 priority 不足。

失败快照包含完整 Arena 节点及 free-list、排序后的 ID 索引与完整 shadow state、双侧价档端点/
计数/聚合、Stop 有序载荷与 priority、两个 allocator 水位、普通 expiry 索引及全部价格参考。
失败后及成功后都运行常规结构、普通 expiry 与 Stop 校验器。测试包含不参与当前改单的双侧订单、
非空 reserves、未触发 GTD Stop 和价格参考状态，避免空状态比较冒充原子性证据。

最终实际运行（2026-10-25，均 exit 0）：

- `cargo test -p matching-orderbook --locked amend_regular --lib`：23 passed（Production 12、Reference 11）。
- `cargo test -p matching-orderbook --locked --quiet`：463 passed、0 failed，30.29s；crate doctest 0。
  包含已有 fixed-seed 100k、双次 10k 与完整 differential/property 回归，但尚未加入 Amend differential 命令。
- `cargo clippy -p matching-orderbook --all-targets --locked -- -D warnings`：通过，无 warning。
- `cargo fmt --check`：通过。
- `git diff --check`：通过，仅有仓库已有 LF→CRLF 转换提示。

本轮没有 domain/risk 逻辑或依赖变更；未重跑整个 workspace gate，它仍属于 Task11 最终验收。
未新增公式、wall clock、网络/RPC、随机业务决策、unsafe、锁或队列。
新辅助结构/入口的构造和无分支委托由上述消费者行为验证，不机械增加镜像单测。
无持久化/I/O/snapshot 改动，crash/replay-digest 测试不适用。

## 后续范围与已报告事项

本小步停止等待独立 review；不进入 Task6。Stop/冰山 Amend、真实 reservation verification/apply、
Amend differential/tapes 和完整 T12 gate 均未完成。

审读时另向父执行会话报告既有 Production `expiry.rs` 的聚合普通+Stop 到期入口直接使用
`stop_book.expire_*`，可能绕过 `stop_orders.rs` 的 shadow 清理包装。此为既有 Stop terminal
整合问题，未在 regular Amend 小步顺手更改；应由后续限定审查确认和处理。
