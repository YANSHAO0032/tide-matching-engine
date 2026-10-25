# T12 Task 10 完整差分与命令序列独立审查

日期：2026-10-25。结论：**通过；Critical 0 / Important 0 / Minor 0**。结论仅关闭 Task 10 审查，不代表 Task 11 或整个 T12 验收完成，也不授权提交、合并或推送。

## 审查依据与边界

独立读取执行计划 Task 10、实施报告、真实源码和 diff、Task 9 审查，并直接核对用户绑定实施任务书 `C:/Users/10703/.codex/attachments/4b1d84b9-e4c2-4af6-a4af-1a0b2fc07723/已粘贴的文本.txt` §26–§30。worktree 没有 ignored `assignment/`，指导规则及唯一交接从 `D:/rust_workspace/tide-matching-engine/assignment/` 读取。旧导师任务书不替代此次用户冻结的实施契约。

本次没有修改业务源码、测试或配置，没有提交，也没有修改唯一 handoff。实施报告的历史 RED 作为历史证据读取；本次没有重新注入缺陷，以下测试结果为本次实际运行。

## 核验结果

1. **实际命令与独立执行。** `amend_full::Harness::run` 分别调用 Reference/Production 自身的完整 `amend_with_context`，不共享 candidate、计划或 mutation。全部输入、真实 Ed25519 签名及资金需求在回放前生成，不用某模型结果决定另一模型输入；随机生成器不读取订单簿。两模型触发核验分别读取自身命令开始的完整预留、身份、冻结费率和两资产覆盖，不是恒成功核验。9000/11000 是此受控域的调用方需求 fixture，不是资产映射或外部风险系统实现。

2. **每命令完整比较。** 命令前后直接比较原序 `Result`/完整 `EngineEvent`，从而包括原序 Trade，不排序事件。状态包括买卖真实价序/FIFO、完整普通订单载荷、visible/hidden/total/peak、TIF、两个优先级及 next、深度和最优价、完整 Stop 载荷/来源/保护/数量模式、完整预留 map/version、三个来源的价格和 source_seq，以及真实普通 GTD/DAY 和 Stop 到期桶引用。身份投影排序仅消除容器布局差异，队列和事件保留业务顺序。时间与 session 明确为调用方输入；没有将本地投影冒充 durable digest。

3. **真实结构与拒绝原子性。** 每次 Production 投影先运行 `validate_book`、`validate_regular_expiry` 和 `validate_stop_book`。独立性质检查 visible+hidden=total、visible 聚合精确、FIFO、StopLevel 同来源同价 priority 递增、next 高于活动 priority、跨普通/Stop 身份唯一和预留无孤儿。任一业务拒绝另比较各模型自己的完整物理快照：Production 含档位、Arena/free-list/链接、身份索引、完整 expiry/StopBook、两个 allocator 和来源；Reference 含其源 Vec、完整预留、两个计数器和来源。

4. **固定边界与成功性质。** 七项固定测试覆盖 §28 的 20 类要求，包括双向真实部分成交后的绝对目标、低于及等于 filled 拒绝、同价增量队尾、非交叉改价、跨两档完全/残余成交及冻结事件顺序、TIF 和真实到期、三类 Stop 的 child/expiry/触发位置矩阵、报价模式禁止切换、冰山峰值/暴露、有效签名之后真实聚合溢出和 stale-version 重放。五容器双方向的 token 重放后还用新 version 成功，排除永远拒绝的假证据。固定 seed `0xA12_5ACC` 的 48-case property 与每次成功 Amend 的独立输入/前态推导共同固定 priority、实际 rest 才分配、身份/数量模式、事件候选余量、精确 next 增量及一次 version apply。失败性质延续 Task 9，并在长 tape 的每次拒绝复核完整状态。

5. **真实长序列与重复 transcript。** `amend-full-v1-ChaCha` 的 `[0xA2;32]` 实际生成并执行 100000 条，没有按模型结果跳过命令，每步完整比较；不是仅比较终态。`[0x3A;32]` 和 `[0xD7;32]` 各生成 10000 条，并重新生成同 seed 输入逐条相等比较。首次回放保存每步完整 `Record { outcome, state }`，第二次逐条严格相等并检查 transcript 恰好耗尽；不是计数或 hash 相等。每次运行强制存在成功/失败 Amend、签名成功、Stop、冰山、Trade、触发和到期。失败输出带版本、完整 seed、step 和 command，可确定性重现。1024 个预备 TradeId 与受控身份/数量上界相符，未吞掉容量 panic。

6. **Reference 预留清理修复。** `retain_active_reservations` 仅扫描 Reference 自身普通 Vec 和未触发 Stop Vec，保留二者身份并清除真实终结身份。四处消费者是 `commit_slices`、普通 IOC/FOK 批量提交、regular reprice 和 iceberg reprice；没有读取 Production 或补造预留。固定 `amend_full_fixed_regular_mutations_preserve_unrelated_stop_reservations` 依次覆盖五个分支，保留三个无关 Stop 的完整预留。根因和修复吻合：只根据普通订单清理共享预留 map 会误删未触发 Stop。报告中的 seed 早期失败及先新增固定 RED 是历史实施证据，本次复跑确认修复后的真实路径通过。

7. **历史证据和范围。** 对照 HEAD 检查，历史 differential 的 GTC/市场/IOC/FOK/PostOnly/冰山/随机 tape、共享观察和断言未弱化；既有 expiry fixture 唯一调整是补显式 LastTrade 字段。入口新增模块，完整命令域独立追加。新增测试访问器仅在 `cfg(test)` 下暴露真实索引；无必要机械镜像单测，其消费者由上述逐命令比较覆盖。本步无新依赖、wire/公式变化、float、unsafe、业务系统时间、网络/数据库、锁、无界队列或 T13 实现；无 I/O，WAL/crash/持久化 digest 不适用。

## 本次实际验证

`cargo test -p matching-orderbook --locked amend_full --lib -- --nocapture`：exit 0；10 passed，0 failed，519 filtered out，36.67s。包含七项固定、一项 48-case property、一条 100k 测试和两组各双次 10k 测试。

实际输出与实施报告逐项一致：

| seed | 命令数/运行 | 成功/拒绝 Amend | 签名成功 | Stop/冰山成功 Amend | Trade/触发/到期 |
| --- | --- | --- | --- | --- | --- |
| `[0xA2;32]` | 100000 ×1 | 19994 / 33265 | 782 | 13427 / 3604 | 4582 / 1838 / 5073 |
| `[0x3A;32]` | 10000 ×2 | 每次 1970 / 3302 | 每次 78 | 每次 1337 / 341 | 每次 451 / 169 / 514 |
| `[0xD7;32]` | 10000 ×2 | 每次 1970 / 3306 | 每次 78 | 每次 1292 / 362 | 每次 439 / 172 / 516 |

本次遵循聚焦审查范围，没有重跑 workspace gate；实施报告中的完整 differential、workspace test、fmt 和 Clippy 是实施者已有证据，未冒充本次命令。Task 11 的文档、全分支审查与最终 gate 仍须完成，远端 CI 未验证。
