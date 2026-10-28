# 原子撤单替换与预留归属

`CancelReplaceOrder` 用完整独立的 `ReplacementOrder` 终结旧身份并接入新身份。Reference 与 Production 各自构造不可变计划、验证 `BookWithout(old)`，再在单写者调用中提交。新单失败不能先删除旧单，也不通过回插旧单实现回滚。

## 支持矩阵

| 对象 | 当前支持 |
|---|---|
| 旧单 | 普通限价、普通簿中的冰山、未触发 StopLimit；必须具有有效活动 reservation shadow |
| 普通限价 replacement | GTC、IOC、FOK、GTC + PostOnly、DAY、GTD |
| 冰山 replacement | GTC、GTC + PostOnly；峰值严格小于总量且满足 lot |
| 资金过渡 | ReuseOld，无调整或真实签名调整；ReplaceWithNew，真实签名的新预留 |

StopMarket 两种计量模式的旧单，以及所有 Market/Stop replacement 均不支持。PostOnly 仅可配 GTC；非 GTC 冰山、reduce-only 和依赖尚未实现的 STP、PriceBand、MarketState 的请求不可降级接入。可表达但不支持的组合返回 `InvalidOrderCombination (2001)`；当前 sum type 本身不提供 Market/Stop replacement。

活动旧身份缺失或 command 的所有权/执行市场不符返回 `OrderNotFound (2010)`；活动旧单缺失或不匹配 shadow 返回 `InvalidReservation (4002)`。replacement 的用户/市场必须与命令一致，新 OrderId 不得等于 old 或任何其他活动身份，否则按结构错误或 `DuplicateOrderId (2008)` 拒绝。市场身份由可信执行上下文及旧 shadow 核对，不能从新 token 反推。

## 只读预检与提交

Reference 的 `ReferenceBookWithout` 独立过滤自身 Vec 的完整 old 身份；Production 的 `ProductionBookWithout` 借用真实价格索引和 FIFO，所有片扫描显式携带同一个 excluded OrderId。普通旧单的可见量、价档贡献、FIFO，冰山的隐藏量和未来补量全部排除；StopLimit 仅从止损/到期逻辑视图排除，从不成为普通可执行流动性。

两侧依次核对旧身份及 shadow/registry parity、replacement 结构、真实 MarketMath 的 tick/lot/峰值/精确本金、逻辑时间/session、BookWithout 的 FOK/PostOnly、逐笔与累计 checked 本金、补量、余量挂单、优先级、TradeId 容量及终结 maker shadow。随后才验证金融过渡，并预计算全部终结订单的绑定批次。资金需求来自可信调用者按实际资产、最大费用和安全量计算的 `ExpectedReplacementReservation`，不能复制 token 覆盖量作为需求。

当前 replacement 和执行 context 没有市价保护、STP 或市场价格带输入。因此这里验证真实限价边界，不声称验证了不可达的保护分支。已有保护市价入口由既有回归保留。

Reference 在可丢弃副本完成自身正常 Place 预演，真实 Apply 比较完整簿及 registry 前态后安装已验证后态。Production 冻结自身片成交、FIFO/价档后态、Arena 槽位/free-list、expiry、两个 allocator 和 registry，再直接消费计划。两套计划、maker 扫描、BookWithout 和提交不共享。

Apply 前态漂移返回 `ReservationApplyError::StateChanged`，属于必须停止处理的不变量错误。首次真实写入后不再执行正常业务拒绝、重新验签或回滚。TradeId 不足沿用调用方契约，在首次写入前 fail-stop。普通旧单和全成 maker 的槽位释放顺序由生产计划预测并在提交核对。新单仅实际 rest 才取得新 QueuePriority；补量也使用新 priority。旧 QueuePriority/StopPriority 不转移或返还。

## 金融状态与 PendingR2

`ReservationRegistry` 保存完整 ReservationId 历史及私有归属。每个金融 `Active` 预留恰好属于 `ActiveOrder(OrderId)` 或 `PendingR2`；`Released` 无归属。PendingR2 表示订单生命周期已终结、金融处理仍待完成，不能解释成资金已扣减或释放。

- ReuseOld 保留 ReservationId，将预留重绑新 OrderId，并以 checked 运算推进一次版本。原覆盖不足时必须有绑定最终 CommandId、旧 OrderId、身份、版本、费率及资产的真实 Ed25519 adjustment；不释放旧预留。
- ReplaceWithNew 严格验证 RiskKeySet 中的已加载公钥、新 token 的规范签名字节、身份、费率、资产集合和足额覆盖；新 ReservationId 不得与活动、PendingR2 或 Released 历史冲突。旧金融 status/version/reserves/fee 全部保持，仅归属转 PendingR2；新预留版本从零开始。
- 全成 maker 与不 rest 的 replacement 也预先规划为 PendingR2；实际 residual 保持 ActiveOrder。批次和金融过渡在不可见候选中组合，通过全部校验后才提交真实状态。

稳定编号为 `CancelReason::CancelReplace = 1109`、`ReservationReleaseReason::FilledRemainder = 1301`、`UserCancel = 1302`。后两项在本功能中均不发射。只有未来真实 R2 在扣减并计算剩余预留后才能决定释放；本实现没有 R2、SettlementDelta、费用扣减或账户余额计算。此用户裁定取代初始任务书中的提前 Released/释放事件顺序。

失败的 ReplaceWithNew 只返回 `CancelReplaceRejection` 中的 `UnadoptedReservation` 清理身份。该身份来自输入，不表示验签通过、已接管、已登记或已释放；拒绝不能修改 registry 或生成资金释放事件。

## 事件与验证边界

成功事件严格为 `OrderCanceled(old, CancelReplace)`，再接本模型既有正常 Place 的原序事件。旧冰山取消数量使用 total remaining；普通 maker 沿用 Trade，冰山 maker 沿用 Partial/Filled/Refresh，IOC 正余量追加既有取消。当前普通 Place 不具备的 Accepted/Opened envelope 不被伪造；没有 `OrderReplaced`。未来真实 R2 的 release 只能在 replacement Place 链之后。

永久测试固定 old-only FOK、冰山全部身份排除、PostOnly 过滤后最优价、Stop 非流动性、真实签名和拒绝优先级。完整失败/漂移快照覆盖 Arena/free-list、链表/价档、活动身份、真实 regular/Stop expiry、两个 allocator、reference 与 registry 金融/归属历史；重复 Plan 相等且不改变状态。

差分磁带使用两个独立适配器和持续 registry，逐命令比较原序结果/事件及完整状态，保留固定种子 100k 和两组各双次 10k。旧窄 Place/Cancel/Amend/expiry 入口的测试驱动显式维护其外部归属边界；只有 CancelReplace 自身直接提交完整 registry 计划，不能把测试适配器视为所有旧 API 已获得原子资金闭环。当前没有 WAL/快照/HA、durable replay digest、远端 CI 或生产性能验收证据。
