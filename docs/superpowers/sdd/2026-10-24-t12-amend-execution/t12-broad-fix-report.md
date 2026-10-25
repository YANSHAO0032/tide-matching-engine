# T12 广泛审查阻塞项修复报告

日期：2026-10-25。范围仅为广泛审查确认的四类缺陷及其回归、说明。未提交、暂存、合并或推送；未进入后续任务。修复后停在 scoped re-review，不能据本报告直接宣称整个 T12 最终完成。

## 读取与范围

- 工作目录为 `C:/Users/10703/.codex/worktrees/t12-amend/tide-matching-engine`；保留此前全部未提交 T12 实现。
- 读取 `target/broad-review-probe/src/main.rs`、实际源代码和测试、Task 11 gate 报告、当前 git 状态与差异。worktree 不含 ignored assignment，改读主 checkout `D:/rust_workspace/tide-matching-engine/assignment/` 的指导、唯一交接、T12 任务书和 v6 对应总量、身份、固定费率与失败原子性条款。
- 根代理确认初始化仅校验现有可表达身份，不为市场身份新增 API。普通下单没有独立 MarketId/ReservationId；Stop 没有 MarketId，但携带 ReservationId。不得用 shadow 自己的字段伪造第二份身份来源。
- 使用 systematic-debugging、test-driven-development、verification-before-completion。曾启动一个 LastTrade 调查子代理，根代理告知本轮不再委派后立即中断，未采纳子代理改动；本文所述实现和验证均由本实施代理直接完成。

## 根因与修复

### 1. 接收总量在立即成交后被残余挂单覆盖

两个 `place_limit_gtc_with_reservation` 原先直接调用 legacy GTC 入口，后者以 residual qty 构造普通挂单。总量 10、立即成交 3 后，两个模型的 `original_order().qty` 都变为 7，后续 Amend 无法还原累计已成交 3，错误地把目标总量 9 解释为增量挂单。

修复分别位于 `production_orderbook/iceberg.rs` 和 `reference_orderbook/iceberg.rs`。入口保存接收载荷，在同一独占调用里、实际正余量挂单成功后，以既有无失败的载荷替换方法恢复接收总量，保持 residual、priority、TIF、链接和深度不变，再登记 shadow。全成路径仍不登记活动 shadow。Reference 独立查找其源 Vec；没有复用生产匹配结果。

回归覆盖买卖两侧、普通和冰山 maker；总量 10 → 成交 3 → 剩余 7 → 同价总目标 9，必须发出余量 7 → 6 的 OrderAmended 并保 QueuePriority，两个 allocator 不推进。

### 2. 预留初始化遗漏用户与 Stop 预留身份

两本簿的共享初始化检查此前只核对 supplied `order_id`、重复活动 ID 和已有 shadow 冲突。错误用户可获得活动订单的 shadow；Stop 载荷和 shadow 的 ReservationId 也可指向不同预留。

两本簿各自的 `validate_new_reservation` 现在同时核对订单和用户，所有普通、timed、GTC、冰山入口传入真实载荷的 UserId。Stop 初始化从实际 Limit/Market 载荷取 UserId，并在进入 StopBook 前额外核对 ReservationId。内部辅助函数签名变化，公共 API 不变，既有正向 fixtures 无须修改。

回归遍历四个普通初始化入口，使用完整生产/参考快照核对拒绝零写入；GTC 分支提供可成交 maker 和足量 TradeId，证明身份拒绝发生在成交前。StopLimit 与 StopMarket 分别覆盖错误用户和错误 ReservationId，并核对 priority、索引和 shadow 完全不变。既有回归继续覆盖错误订单 ID。

市场边界：初始化没有独立市场身份可比较，保留这一事实；完整 Amend 仍由 `context.prepare` 将 `state.market_id` 与 `amend.market_id` 绑定。未添加默认市场身份或新的市场 API。

### 3. Stop 固定费率未绑定执行上下文

v6 §7.4.3 要求令牌费率版本与候选订单固定策略一致。原实现只验证 token 与 context 相互一致，而 Stop 载荷始终保留接收阶段固定版本；因此 Stop 固定版本 5、context/token 同为 6 仍可接受。

两模型的 Stop 数值/上下文预检现在核对 context 的 fee_schedule_version 与原 Stop 载荷。StopLimit、StopMarketByBase、StopMarketByQuote 都在任何簿或 reservation 写入前拒绝不匹配版本，reason 沿用 InvalidReservation。已有验证器继续核对 token 与 context；未新增 wire reason 或 token 字段。

回归覆盖三类 Stop 的无 token 和真实签名 token 六种情形，错误 context/token 相互一致也必须拒绝且完整状态不变。原有相同固定版本的成功与验签回归保持通过。`AmendExecutionContext` 的字段注释同步明确该边界。

### 4. root LastTrade 更新后的后代序号溢出

原 `trigger_stops` 仅按更新前 LastTrade 检查 `source_seq + trade_ids.len()`。当旧序号为 1，root 输入更新为 u64::MAX，并触发可成交 StopLimit 时，预检通过；真实 Stop 已移除、maker 已成交、child 已挂余量后，生成内部 LastTrade 才返回 ArithmeticOverflow。

两模型分别在有界的价格参考状态副本上先接受 root 输入，验证来源与单调性，再按该副本的 LastTrade 检查后代容量；真实参考、Stop、成交和 shadow 均尚未修改。沿用既有以调用方 TradeId 数量预留最大后代容量的保守规则。仅复制最多四种来源的参考价状态，没有复制/回滚整个订单簿。

回归以完整快照证明 root 序号 u64::MAX 与一个后代成交的溢出拒绝原子性；同一原簿再输入 u64::MAX−1，恰好一个成交成功将 LastTrade 推进至最大值，余量为 7。原有 stale/static source 回归仍通过。

## RED → GREEN 证据

新文件 `production_orderbook/tests/broad_review.rs` 含五个有业务断言的测试，由 tests.rs 登记。

1. 首次编译发现测试误把 VerifyingKey 转为字节数组；仅修正测试参数类型。这次编译失败不计为 RED。
2. `cargo test -p matching-orderbook broad_review -- --nocapture` 在生产修复前实际运行：exit 1，0 passed / 5 failed。总量断言观察到 `(7, 7)` 而非 `(10, 10)`；普通错误用户与 Stop 错误 ReservationId 都返回成功；Stop 错误费率返回 OrderAmended；序号溢出返回错误后完整簿快照已改变。
3. 按根因依次修复并运行各自定向测试，每项转 GREEN；随后五项全部通过。最后补充非空 identity 快照与最大合法序号成功对照，再次五项通过，日志 `target/t12-broad-directed.log`。

## 验证与文档

- 完整 `cargo test -p matching-orderbook --lib`：534 passed，exit 0，日志 `target/t12-broad-orderbook.log`。包含完整 differential、property、固定 seed 100k 和双次 10k tape。
- `cargo test --workspace`：708 项测试（domain 153 unit + 10 integration、orderbook 534、risk 11）和 2 doctest 通过，exit 0；其余 skeleton crate 没有业务测试。最终日志 `target/t12-broad-workspace.log`。
- `cargo fmt --check`、`cargo clippy --workspace --all-targets -- -D warnings`、普通与 cached `git diff --check`：exit 0。Clippy 日志 `target/t12-broad-clippy.log`。
- `RUSTDOCFLAGS=-D warnings cargo doc --workspace --no-deps --document-private-items`：exit 0，日志 `target/t12-broad-rustdoc.log`。
- `docs/amend.md` 同步接收总量、初始化可验证身份、Stop 固定费率与 root 后参考序号预检边界。没有新增计算公式或改变既有公式语义，`docs/numerical_formulas.md` 未由本修复修改。

## 审查结论与续接

新状态检查早于首次可见写入；总量恢复仅执行既有无失败载荷提交，不新增 fallible commit。无 float、unsafe、wall clock、网络/RPC、无界队列、新依赖或协议扩展。HashMap 只按 ID 定位，不决定业务顺序；Reference 保留独立 Vec 业务实现。该修复没有 I/O/WAL/snapshot，crash 与 durable digest 不适用；已有确定性 tape 已运行。中文注释已检查。未为简单字段读取或内部参数转发机械添加独立测试，其行为由五组跨入口业务回归与原有 suites 覆盖。

待 scoped re-review 核对本报告的四类修复和新测试。根代理应将本次证据同步到唯一 `assignment/SESSION_HANDOFF.md`，继续区分本地 gate、整体 T12 审查、远端 CI 和提交授权；本报告不替代唯一交接。
