# 挂单修改与本地预留授权

`ReferenceOrderBook` 与 `ProductionOrderBook` 提供普通限价、冰山及未触发止损订单的 Amend。成功保留 `OrderId`；两模型各自构造不可变候选、完整只读计划，再提交自身容器。Reference 使用 Vec 扫描与稳定排序，Production 使用 Arena、价格档和侵入式 FIFO。共享领域值、数值契约和验签器不替代两套业务规划。

## 输入与事件

2026-10-24 用户冻结的协议裁定优先于旧任务书与 v6 原始文字：总目标严格大于已成交量，零余量终结使用 Cancel；改单事件采用可选价格与显式 `AmendedQuantity`，使报价预算可以准确表达。原始规格保留为历史来源，当前实现与以下说明按该裁定执行。

`AmendOrder.new_qty` 是包含已成交量的绝对总目标。当前保存的总目标减当前总余量得到累计已成交量，再从新总目标扣除；没有数量补丁时保留最近接受的总目标。新目标必须严格大于已成交量；相等或更小返回 `InvalidQty`，需要终结余量时调用 Cancel。不能用 Amend 切换 BaseQty 与 QuoteBudget 模式。`new_quote_budget: Option<QuoteAmount>` 仅适用于未触发 StopMarketByQuote，且显式拒绝零预算。

关联预留的 GTC 下单在立即成交后仍保存完整接收总量；例如总量 10、立即成交 3、挂入余量 7，后续同价改总量 9 得到余量 6，并保留原有优先级。

`new_trigger_expiry` 和 `new_market_protection` 分别使用可选 patch。未提供或 `Keep` 保留，`ClearToGtc` 清除触发前到期，`Clear` 清除市价保护，`Set` 写入新值。数量、价格、显示峰值和 TIF 依现有订单类型校验；普通和冰山不接受止损专属字段。

成功首先输出 `OrderAmended`，`old_price/new_price: Option<Price>` 对市价 child 为 `None`；`old_quantity/new_quantity: AmendedQuantity` 明确区分基础余量与报价预算。新数量是再次撮合之前的候选值。改价事件固定为 `OrderAmended → Trade* → 订单事件`：先输出全部原序成交，再输出保持相对顺序的 maker 生命周期/补量事件，最后按既有协议输出目标订单成交状态；无成交只发改单事件。`priority_retained` 对普通簿表示 QueuePriority，对未触发 Stop 表示 StopPriority。

## 队列与到期

| 修改 | 队列行为 |
|---|---|
| 普通同价减量或数量不变 | 保留原位置与 QueuePriority |
| 普通同价增量 | 获得新 QueuePriority，进入同价尾部 |
| 普通或冰山改价 | 以候选全部余量撮合，仅实际正余量挂单时分配目标新 priority |
| 冰山同价、总量与峰值均不增加 | 保序；可见片裁剪为旧可见片、新峰值、新余量的最小值 |
| 冰山总量或峰值增加 | 失去 QueuePriority，重新挂单时建立新显示片 |
| Stop child 数量、预算、限价、TIF、保护或触发前到期修改 | 保留 StopPriority 与触发队列位置 |
| Stop 触发价或触发来源实际改变 | 只分配一个新 StopPriority，进入新价格/来源档尾部 |

冰山始终满足可见量加隐藏量等于总余量，公开深度只计算可见量；峰值必须小于绝对总目标，可以大于尾部剩余量。具体公式见[数值计算公式](numerical_formulas.md)。

普通与冰山允许合法 GTC/GTD/DAY 迁移；纯 TIF 修改本身不丢优先级。GTD 必须严格晚于输入逻辑时间，DAY 必须匹配输入 session。Production 同一成功提交删除旧 expiry/session 引用并建立新引用；Reference 继续从自身载荷扫描。Stop 的 child TIF 与触发前有效期独立迁移。未触发 Stop 不取得普通 QueuePriority。

Stop 按 LastTrade、MarkPrice、IndexPrice 分区；StaticOpening 不可触发。触发入口明确接收逻辑时间和当前 session，先移出 Stop 与到期引用，再重校验 child 并执行 Trigger-R1。合法 DAY/GTD 限价 child 的实际余量进入普通到期索引。成功挂单的 child 接续原 shadow reservation，并保存已接受的完整总目标；例如总量 12、成交 2、余量 10 后再改总量 11，应得到余量 9。拒绝、全成和市价 child 不留下活动 shadow。

## 完整数值和资金入口

完整入口是 `amend_with_context(amend, now, current_session_id, trade_ids, context)`。`AmendExecutionContext` 必填实际 `MarketMath`、最终 CommandId、固定费率版本/层级、预加载 `RiskKeySet` 和本次候选的 `required_reserves`。上下文数值预检覆盖 tick/lot、显示峰值、Stop 触发价/保护最差价、候选本金及改价实际 maker 的逐笔与累计 checked 本金。

当前核心不拥有资产映射、费率表或费用取整策略。调用者必须根据候选订单及真实配置计算包含本金、最大费用和取整安全量的完整资金需求；不能从令牌复制需求或填入默认资产。需求列表非空、严格按资产升序、无重复且 atoms 严格正。这是调用者信任边界，上游需求计算器与真实账户冻结尚无实现证据。

无调整令牌时，已有 shadow 必须逐资产覆盖需求；成功保留原 reserves 和 version，降低需求也不自动释放。有令牌时，`matching-risk` 使用已加载版本化公钥执行真实 Ed25519 严格验签，核对命令、订单、用户、市场、预留身份、预期版本、固定费率与目标覆盖。版本的 checked 递增先作用于可丢弃副本；所有簿预检成功后，仅为仍活动的目标安装新状态。全成终结保持清理结果；后续拒绝不应用副本。同一令牌再次使用因版本不匹配被拒绝，这不等于完整命令幂等或账户侧去重。

未触发 Stop 的费率版本固定在接收载荷中。改单上下文必须与该版本一致；携带令牌时令牌也必须绑定同一版本。即使令牌与上下文相互一致，只要偏离该 Stop 的固定版本，仍在任何状态提交前拒绝。

低层 `amend_regular_with_events`（含冰山）及 `amend_stop_with_events` 要求调用方已完成数值与资金校验，并拒绝直接携带调整令牌。它们不能替代完整上下文入口。两模型保留旧无预留挂单入口，并提供 reservation-aware 的 rest、timed rest、GTC、冰山只挂单及 Stop 初始化入口，在同一独占调用中关联实际活动余量与所给状态；缺少 shadow 的活动订单不能 Amend。Cancel、Fill、Expire 与 Stop 终结同步清理 shadow。

预留初始化在首次写入前核对订单和用户身份，Stop 另核对载荷中的预留身份。当前单市场簿和下单载荷没有独立 MarketId，不能在初始化时捏造市场核验；后续 Amend 必须把命令 MarketId 与 shadow 中的 MarketId 绑定。

## 失败边界与验证

数量、时间、保护、资金、算术、档位聚合与 priority 的普通拒绝都发生在写屏障之前；拒绝保持载荷、FIFO、深度、Arena/free-list、身份与到期索引、两种 priority 及 allocator、来源参考和完整预留/version。TradeId 不足遵循既有调用方容量契约，在首个写入前 fail-stop；不是客户业务 Reject。提交阶段消费已冻结计划，不返回新的普通业务错误。

参考更新触发级联前，先在独立参考价快照中应用 root 输入，再检查后代成交所需的 LastTrade 序号容量。root 自身提高来源序号后的溢出也必须在移除 Stop、撮合和推进真实参考前拒绝；恰好使用到最大合法序号仍可成功。

固定边界、生成式失败性质和完整混合差分逐命令比较原序结果/事件、所有普通与 Stop 载荷、真实 FIFO、深度、到期引用、两个 allocator、来源价格/序号和完整预留。固定 seed 100k 及两组各双次 10k 比较完整逐步 transcript；状态投影用于本地测试，不是 durable digest。实现入口见[模块说明](orderbook_modules.md)，执行顺序见[逻辑流程](logic_flows.md)。

当前不包含 MarketState/KillSwitch/路由 fencing、完整 PriceBand/STP/PreOpen、账户与 Settlement、WAL/快照/HA、durable envelope、完整命令去重或 CancelReplace。止损 root reference update 一次冻结更新后参考作为全级联用户保护上下文，后代成交只更新触发检测状态；完整市场价格带配置仍由后续接入。
