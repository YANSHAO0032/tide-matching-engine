# 匹配引擎关键逻辑流程图

本文件是项目关键逻辑流程图的长期入口。当前收录已验收的工程基础与订单簿流程；后续经过验收的重要流程也应补入此文件。现有图不表示尚未实现的差分测试、扩展订单规则、持久化或运行时能力。

## 工程基础与本地质量门

```text
Cargo workspace members + fixed Rust toolchain
                    |
                    v
         cargo fmt --check
                    |
                    v
 cargo clippy --workspace --all-targets -D warnings
                    |
                    v
          cargo test --workspace
                    |
          +---------+---------+
          |                   |
          v                   v
       pass: evidence      fail: retain error,
       for review          do not claim acceptance
```

工程基础固定 workspace、工具链和 lint 继承；质量门只提供自动化证据，不能替代对确定性、checked arithmetic 和业务不变量的审查。

## 领域值构造与错误边界

```text
external integer input
        |
        v
strict domain constructor (Price / Qty / amount)
        |
   +----+-----------------+
   |                      |
   v                      v
valid canonical value   RejectReason
private representation  (invalid value / arithmetic overflow)
        |
        v
typed order and trade payloads
```

领域模型使用整数类型表示价格、数量和金额。订单簿只接收已构造的严格正 `Price`、`Qty`，并使用 checked arithmetic 保存失败原子性。

## 参考订单簿的撮合基线

```text
incoming Limit GTC
        |
        v
choose opposite-side maker from Vec + stable sort
        |
        +-- no crossing or incoming exhausted --> stop; do not rest incoming
        |
        v
plan one fill (maker ID, maker price, min remaining qty)
        |
        v
checked deductions for maker and incoming
        |
        +-- maker exhausted --> remove from Vec
        |
        v
emit caller-supplied TradeId as TradeEvent
        |
        v
repeat until stop
```

参考簿保留 `Vec + sort`，为后续的行为比较提供基线。它不会将 incoming 余量自动挂单；生产 GTC 入口对这一流程差异作明确适配。

## 订单簿存储、FIFO 与优先级

```text
resting node
    |
    v
OrderArena::insert
    |
    +-- free-list nonempty --> reuse its LIFO tail slot
    |
    +-- otherwise ----------> append a new stable slot
    |
    v
PriceLevel::push_back
    |
    +-- empty level --> head = tail = new slot
    |
    +-- nonempty ----> old tail.next <-> new.prev; tail = new slot
    |
    v
count / total_visible_qty updated with checked arithmetic
    |
    v
QueuePriority allocator advances only after a successful rest
```

同价 FIFO 的权威顺序是 `PriceLevel` 从 head 到 tail 的 intrusive 链，不是 OrderId、Arena 槽位号或 HashMap 遍历顺序。删除必须先解除链接和 active ID 引用，再释放 Arena 槽位，避免旧 ID 因槽位复用命中新节点。

## 生产订单簿的完整 Limit GTC

```text
LimitGtcOrder + caller-supplied TradeIds
                 |
                 v
1. reject duplicate active OrderId
                 |
                 v
2. read-only FillPlan
   Buy: asks low -> high; Sell: bids high -> low; FIFO per level
                 |
                 v
3. assert enough TradeIds before every mutation
                 |
       +---------+---------+
       |                   |
       v                   v
  exact fill          positive residual
       |                   |
       |           4. common rest preflight
       |              duplicate/priority/aggregate/Arena
       |                   |
       +---------+---------+
                 |
                 v
5. commit continuous maker fills in planned order
   partial maker: reduce node + level aggregate
   full maker: pop FIFO head, remove ID index, clear empty level
                 |
       +---------+---------+
       |                   |
       v                   v
  no residual       residual remains
       |                   |
       |           assert book is strictly uncrossed
       |           commit rest at own level tail
       |           write ID index, then advance priority
       +---------+---------+
                 |
                 v
return TradeEvents in actual matching order
```

生产订单簿使用 BTreeMap 决定双侧价格优先，使用 `PriceLevel` 决定同价 FIFO，HashMap 只做单个 OrderId 定位。所有可预期的 `RejectReason` 在第一笔成交前结束；预检与提交发生分歧属于内部不变量损坏，按 fail-closed 处理。
