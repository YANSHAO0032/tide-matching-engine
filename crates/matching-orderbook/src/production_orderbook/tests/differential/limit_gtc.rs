//! 普通限价单与取消的双向差分案例、输入域边界和固定种子回放。
use super::*;

/// 主方向固定命令序列。
///
/// 预期：
///
/// 卖出 30 @105 x3
/// 卖出 10 @105 x7
///
/// 买入 100 @110 x8
///   -> 挂单 30 成交 3
///   -> 挂单 10 成交 5
///   -> 全部成交，不挂单，不消耗优先级
///
/// 买入 101 @110 x5
///   -> 挂单 10 成交 2
///   -> 余量买入 101 @110 x3
///
/// 下单重复标识符 101：
///   当前活动 101 是买入@110；
///   故意发送卖单101@100，它本应价格交叉，
///   用来证明重复标识符检查在撮合前完成。
///
/// 取消订单101
/// 取消订单101 再次 -> OrderNotFound
///
/// 再用 101 挂卖出@120 x4，证明取消后标识符可复用。
fn buy_taker_tape() -> Vec<Command> {
    vec![
        Command::Place(order(30, Side::Sell, 105, 3)),
        Command::Place(order(10, Side::Sell, 105, 7)),
        Command::Place(order(100, Side::Buy, 110, 8)),
        Command::Place(order(101, Side::Buy, 110, 5)),
        Command::Place(order(101, Side::Sell, 100, 1)),
        Command::Cancel(OrderId::new(101)),
        Command::Cancel(OrderId::new(101)),
        Command::Place(order(101, Side::Sell, 120, 4)),
    ]
}

/// 卖出吃单方的方向镜像。
///
/// 买入 30 @95 x3
/// 买入 10 @95 x7
///
/// 卖出 100 @90 x8
///   -> 挂单 30 成交 3
///   -> 挂单 10 成交 5
///
/// 卖出 101 @90 x5
///   -> 挂单 10 成交 2
///   -> 余量卖出 101 @90 x3
///
/// 活动重复标识符时故意发送买单101@100，
/// 它本应价格交叉卖单101@90，用于验证重复标识符优先。
fn sell_taker_tape() -> Vec<Command> {
    vec![
        Command::Place(order(30, Side::Buy, 95, 3)),
        Command::Place(order(10, Side::Buy, 95, 7)),
        Command::Place(order(100, Side::Sell, 90, 8)),
        Command::Place(order(101, Side::Sell, 90, 5)),
        Command::Place(order(101, Side::Buy, 100, 1)),
        Command::Cancel(OrderId::new(101)),
        Command::Cancel(OrderId::new(101)),
        Command::Place(order(101, Side::Buy, 80, 4)),
    ]
}

#[test]
fn fixed_buy_taker_tape_is_differential_and_reproducible() {
    let tape = buy_taker_tape();

    // ------------------------------------------------------------
    // 同一固定命令序列必须在独立空簿上重复得到完全相同执行记录。
    // ------------------------------------------------------------

    let first = run_tape(&tape);
    let second = run_tape(&tape);

    assert_eq!(
        first, second,
        "same fixed Buy-taker tape must be fully reproducible"
    );

    assert_eq!(first.len(), 8);

    // ============================================================
    // 步骤 0:
    // 卖单30@105 x3
    //
    // 无成交但存在余量，因此必须挂单。
    // ============================================================

    assert_eq!(first[0].production_next_priority, 1);

    assert_eq!(first[0].book.asks.orders.len(), 1);

    assert_eq!(
        first[0].book.asks.orders[0].original.order_id,
        OrderId::new(30)
    );

    assert_eq!(first[0].book.asks.orders[0].remaining, 3);

    assert_eq!(first[0].book.asks.orders[0].priority, QueuePriority::new(0));

    // ============================================================
    // 步骤 1:
    // 卖单10@105 x7
    //
    // 同价先进先出：
    // 30 -> 10
    // ============================================================

    assert_eq!(first[1].production_next_priority, 2);

    assert_eq!(
        first[1]
            .book
            .asks
            .orders
            .iter()
            .map(|entry| entry.original.order_id)
            .collect::<Vec<_>>(),
        vec![OrderId::new(30), OrderId::new(10)]
    );

    assert_eq!(
        first[1].book.asks.depth,
        vec![ObservedDepth {
            price: Price::try_new(105).unwrap(),
            order_count: 2,
            total_qty: 10,
        }]
    );

    assert_eq!(
        first[1].book.asks.best_price,
        Some(Price::try_new(105).unwrap())
    );

    // ============================================================
    // 步骤 2:
    // 买单100@110 x8
    //
    // 30 -> 10
    // 成交 3 -> 5
    //
    // 新到订单全部成交，所以：
    //
    // - 买单100 不挂入；
    // - 优先级仍然是 2。
    // ============================================================

    let CommandOutcome::Place(Ok(trades)) = &first[2].outcome else {
        panic!("Buy100 must succeed");
    };

    assert_eq!(trades.len(), 2);

    assert_eq!(
        (
            trades[0].trade_id,
            trades[0].maker_order_id,
            trades[0].taker_order_id,
            trades[0].price,
            trades[0].qty,
            trades[0].maker_side,
        ),
        (
            TradeId::new(1),
            OrderId::new(30),
            OrderId::new(100),
            Price::try_new(105).unwrap(),
            Qty::try_new(3).unwrap(),
            Side::Sell,
        )
    );

    assert_eq!(
        (
            trades[1].trade_id,
            trades[1].maker_order_id,
            trades[1].taker_order_id,
            trades[1].price,
            trades[1].qty,
            trades[1].maker_side,
        ),
        (
            TradeId::new(2),
            OrderId::new(10),
            OrderId::new(100),
            Price::try_new(105).unwrap(),
            Qty::try_new(5).unwrap(),
            Side::Sell,
        )
    );

    // 挂单方10 原数量=7，当前剩余量=2。
    assert_eq!(first[2].book.asks.orders.len(), 1);

    assert_eq!(
        first[2].book.asks.orders[0].original.order_id,
        OrderId::new(10)
    );

    assert_eq!(
        first[2].book.asks.orders[0].original.qty,
        Qty::try_new(7).unwrap()
    );

    assert_eq!(first[2].book.asks.orders[0].remaining, 2);

    assert_eq!(first[2].book.asks.orders[0].priority, QueuePriority::new(1));

    assert!(first[2].book.bids.orders.is_empty());

    // 全部成交不允许消耗新优先级。
    assert_eq!(first[2].production_next_priority, 2);

    // ============================================================
    // 步骤 3:
    // 买单101@110 x5
    //
    // 挂单方10 剩余2：
    //   成交 2
    //
    // 吃单方剩余3：
    //   余量买单101@110 x3
    //
    // 余量载荷的数量必须是真实余量3，而不是原始5。
    // ============================================================

    let CommandOutcome::Place(Ok(trades)) = &first[3].outcome else {
        panic!("Buy101 must succeed");
    };

    assert_eq!(trades.len(), 1);

    assert_eq!(
        (
            trades[0].trade_id,
            trades[0].maker_order_id,
            trades[0].taker_order_id,
            trades[0].price,
            trades[0].qty,
            trades[0].maker_side,
        ),
        (
            TradeId::new(3),
            OrderId::new(10),
            OrderId::new(101),
            Price::try_new(105).unwrap(),
            Qty::try_new(2).unwrap(),
            Side::Sell,
        )
    );

    assert!(first[3].book.asks.orders.is_empty());

    assert_eq!(first[3].book.bids.orders.len(), 1);

    let resting_101 = &first[3].book.bids.orders[0];

    assert_eq!(resting_101.original.order_id, OrderId::new(101));

    assert_eq!(resting_101.original.side, Side::Buy);

    assert_eq!(resting_101.original.price, Price::try_new(110).unwrap());

    // 关键：载荷本身就是余量数量。
    assert_eq!(resting_101.original.qty, Qty::try_new(3).unwrap());

    assert_eq!(resting_101.remaining, 3);

    assert_eq!(resting_101.priority, QueuePriority::new(2));

    assert_eq!(
        first[3].book.bids.depth,
        vec![ObservedDepth {
            price: Price::try_new(110).unwrap(),
            order_count: 1,
            total_qty: 3,
        }]
    );

    assert_eq!(
        first[3].book.bids.best_price,
        Some(Price::try_new(110).unwrap())
    );

    assert_eq!(first[3].book.asks.best_price, None);

    assert_eq!(first[3].production_next_priority, 3);

    // ============================================================
    // 步骤 4:
    // 重复标识符 101。
    //
    // 候选本身卖出@100，本来会与买单101@110 价格交叉。
    //
    // 必须优先 DuplicateOrderId，且完整状态不变。
    // ============================================================

    assert_eq!(
        first[4].outcome,
        CommandOutcome::Place(Err(RejectReason::DuplicateOrderId))
    );

    assert_eq!(first[4].book, first[3].book);

    assert_eq!(first[4].production_next_priority, 3);

    // ============================================================
    // 步骤 5:
    // 取消订单101 成功。
    // ============================================================

    let CommandOutcome::Cancel(Ok(removed)) = &first[5].outcome else {
        panic!("Cancel101 must succeed");
    };

    assert_eq!(removed.original.order_id, OrderId::new(101));

    assert_eq!(removed.original.qty, Qty::try_new(3).unwrap());

    assert_eq!(removed.remaining, 3);

    assert_eq!(removed.priority, QueuePriority::new(2));

    assert!(first[5].book.bids.orders.is_empty());
    assert!(first[5].book.asks.orders.is_empty());

    // 取消不消耗优先级。
    assert_eq!(first[5].production_next_priority, 3);

    // ============================================================
    // 步骤 6:
    // 重复取消 -> OrderNotFound，状态不变。
    // ============================================================

    assert_eq!(
        first[6].outcome,
        CommandOutcome::Cancel(Err(RejectReason::OrderNotFound))
    );

    assert_eq!(first[6].book, first[5].book);

    assert_eq!(first[6].production_next_priority, 3);

    // ============================================================
    // 步骤 7:
    // 取消后重新使用 OrderId=101。
    // ============================================================

    let CommandOutcome::Place(Ok(trades)) = &first[7].outcome else {
        panic!("reused OrderId 101 Place must succeed");
    };

    assert!(trades.is_empty());

    assert!(first[7].book.bids.orders.is_empty());

    assert_eq!(first[7].book.asks.orders.len(), 1);

    let reused_101 = &first[7].book.asks.orders[0];

    assert_eq!(reused_101.original.order_id, OrderId::new(101));

    assert_eq!(reused_101.original.side, Side::Sell);

    assert_eq!(reused_101.original.price, Price::try_new(120).unwrap());

    assert_eq!(reused_101.original.qty, Qty::try_new(4).unwrap());

    assert_eq!(reused_101.remaining, 4);

    // 原 101 优先级=2 已被取消；
    // 标识符可以复用，但优先级不能复用。
    assert_eq!(reused_101.priority, QueuePriority::new(3));

    assert_eq!(
        first[7].book.asks.best_price,
        Some(Price::try_new(120).unwrap())
    );

    assert_eq!(first[7].production_next_priority, 4);
}

#[test]
fn fixed_sell_taker_mirror_tape_is_differential_and_reproducible() {
    let tape = sell_taker_tape();

    let first = run_tape(&tape);
    let second = run_tape(&tape);

    assert_eq!(
        first, second,
        "same fixed Sell-taker tape must be fully reproducible"
    );

    assert_eq!(first.len(), 8);

    // ============================================================
    // 初始两个买入挂单都正常挂单。
    // ============================================================

    assert_eq!(first[0].production_next_priority, 1);
    assert_eq!(first[1].production_next_priority, 2);

    assert_eq!(
        first[1]
            .book
            .bids
            .orders
            .iter()
            .map(|entry| entry.original.order_id)
            .collect::<Vec<_>>(),
        vec![OrderId::new(30), OrderId::new(10)]
    );

    assert_eq!(
        first[1].book.bids.depth,
        vec![ObservedDepth {
            price: Price::try_new(95).unwrap(),
            order_count: 2,
            total_qty: 10,
        }]
    );

    assert_eq!(
        first[1].book.bids.best_price,
        Some(Price::try_new(95).unwrap())
    );

    // ============================================================
    // 步骤 2:
    // 卖单100@90 x8
    //
    // 买单30 -> 买单10
    // 成交 3 -> 5
    // ============================================================

    let CommandOutcome::Place(Ok(trades)) = &first[2].outcome else {
        panic!("Sell100 must succeed");
    };

    assert_eq!(trades.len(), 2);

    assert_eq!(
        (
            trades[0].trade_id,
            trades[0].maker_order_id,
            trades[0].taker_order_id,
            trades[0].price,
            trades[0].qty,
            trades[0].maker_side,
        ),
        (
            TradeId::new(1),
            OrderId::new(30),
            OrderId::new(100),
            Price::try_new(95).unwrap(),
            Qty::try_new(3).unwrap(),
            Side::Buy,
        )
    );

    assert_eq!(
        (
            trades[1].trade_id,
            trades[1].maker_order_id,
            trades[1].taker_order_id,
            trades[1].price,
            trades[1].qty,
            trades[1].maker_side,
        ),
        (
            TradeId::new(2),
            OrderId::new(10),
            OrderId::new(100),
            Price::try_new(95).unwrap(),
            Qty::try_new(5).unwrap(),
            Side::Buy,
        )
    );

    assert_eq!(first[2].book.bids.orders.len(), 1);

    assert_eq!(
        first[2].book.bids.orders[0].original.order_id,
        OrderId::new(10)
    );

    assert_eq!(first[2].book.bids.orders[0].remaining, 2);

    // 全部成交不消耗优先级。
    assert_eq!(first[2].production_next_priority, 2);

    // ============================================================
    // 步骤 3:
    // 卖单101@90 x5
    //
    // 挂单方10 成交量2
    // 余量卖单101@90 x3
    // ============================================================

    let CommandOutcome::Place(Ok(trades)) = &first[3].outcome else {
        panic!("Sell101 must succeed");
    };

    assert_eq!(trades.len(), 1);

    assert_eq!(
        (
            trades[0].trade_id,
            trades[0].maker_order_id,
            trades[0].taker_order_id,
            trades[0].price,
            trades[0].qty,
            trades[0].maker_side,
        ),
        (
            TradeId::new(3),
            OrderId::new(10),
            OrderId::new(101),
            Price::try_new(95).unwrap(),
            Qty::try_new(2).unwrap(),
            Side::Buy,
        )
    );

    assert!(first[3].book.bids.orders.is_empty());

    assert_eq!(first[3].book.asks.orders.len(), 1);

    let resting_101 = &first[3].book.asks.orders[0];

    assert_eq!(resting_101.original.order_id, OrderId::new(101));

    assert_eq!(resting_101.original.side, Side::Sell);

    assert_eq!(resting_101.original.price, Price::try_new(90).unwrap());

    assert_eq!(resting_101.original.qty, Qty::try_new(3).unwrap());

    assert_eq!(resting_101.remaining, 3);

    assert_eq!(resting_101.priority, QueuePriority::new(2));

    assert_eq!(
        first[3].book.asks.depth,
        vec![ObservedDepth {
            price: Price::try_new(90).unwrap(),
            order_count: 1,
            total_qty: 3,
        }]
    );

    assert_eq!(
        first[3].book.asks.best_price,
        Some(Price::try_new(90).unwrap())
    );

    assert_eq!(first[3].production_next_priority, 3);

    // ============================================================
    // 步骤 4:
    // 重复标识符卖单101 已活动。
    //
    // 候选买单101@100 本来会价格交叉，
    // 仍必须先返回 DuplicateOrderId。
    // ============================================================

    assert_eq!(
        first[4].outcome,
        CommandOutcome::Place(Err(RejectReason::DuplicateOrderId))
    );

    assert_eq!(first[4].book, first[3].book);

    assert_eq!(first[4].production_next_priority, 3);

    // ============================================================
    // 步骤 5 取消订单101。
    // ============================================================

    let CommandOutcome::Cancel(Ok(removed)) = &first[5].outcome else {
        panic!("mirror Cancel101 must succeed");
    };

    assert_eq!(removed.original.order_id, OrderId::new(101));

    assert_eq!(removed.original.qty, Qty::try_new(3).unwrap());

    assert_eq!(removed.remaining, 3);

    assert_eq!(removed.priority, QueuePriority::new(2));

    assert!(first[5].book.bids.orders.is_empty());
    assert!(first[5].book.asks.orders.is_empty());

    assert_eq!(first[5].production_next_priority, 3);

    // ============================================================
    // 步骤 6 重复取消。
    // ============================================================

    assert_eq!(
        first[6].outcome,
        CommandOutcome::Cancel(Err(RejectReason::OrderNotFound))
    );

    assert_eq!(first[6].book, first[5].book);

    assert_eq!(first[6].production_next_priority, 3);

    // ============================================================
    // 步骤 7 标识符复用。
    // ============================================================

    let CommandOutcome::Place(Ok(trades)) = &first[7].outcome else {
        panic!("mirror reused OrderId 101 Place must succeed");
    };

    assert!(trades.is_empty());

    assert_eq!(first[7].book.bids.orders.len(), 1);

    assert!(first[7].book.asks.orders.is_empty());

    let reused_101 = &first[7].book.bids.orders[0];

    assert_eq!(reused_101.original.order_id, OrderId::new(101));

    assert_eq!(reused_101.original.side, Side::Buy);

    assert_eq!(reused_101.original.price, Price::try_new(80).unwrap());

    assert_eq!(reused_101.original.qty, Qty::try_new(4).unwrap());

    assert_eq!(reused_101.remaining, 4);

    assert_eq!(reused_101.priority, QueuePriority::new(3));

    assert_eq!(
        first[7].book.bids.best_price,
        Some(Price::try_new(80).unwrap())
    );

    assert_eq!(first[7].production_next_priority, 4);
}

/// 空命令序列和原有 1k 基础冒烟测试长度都属于允许域；重复取消不存在标识符是合法业务输入。
#[test]
fn tape_domain_accepts_empty_and_1000_smoke_length() {
    assert_eq!(validate_tape_domain(&[]), Ok(()));
    assert!(run_tape(&[]).is_empty());

    let tape: Vec<_> = (0..1000)
        .map(|_| Command::Cancel(OrderId::new(1)))
        .collect();
    assert_eq!(validate_tape_domain(&tape), Ok(()));
}

#[test]
fn tape_domain_accepts_all_field_boundaries() {
    let tape = vec![
        Command::Place(order(1, Side::Buy, 80, 1)),
        Command::Place(order(128, Side::Sell, 120, 100)),
        Command::Cancel(OrderId::new(1)),
        Command::Cancel(OrderId::new(128)),
    ];
    assert_eq!(validate_tape_domain(&tape), Ok(()));
}

#[test]
fn tape_domain_rejects_overlong_tape() {
    let tape: Vec<_> = (0..100_001)
        .map(|_| Command::Cancel(OrderId::new(1)))
        .collect();
    let error = validate_tape_domain(&tape).unwrap_err();
    assert!(error.contains("length=100001"), "{error}");
    assert!(error.contains("0..=100000"), "{error}");
}

#[test]
fn tape_domain_rejects_out_of_range_fields_with_command_index() {
    let cases = [
        (Command::Cancel(OrderId::new(0)), "OrderId=0", "1..=128"),
        (Command::Cancel(OrderId::new(129)), "OrderId=129", "1..=128"),
        (
            Command::Place(order(0, Side::Buy, 100, 1)),
            "OrderId=0",
            "1..=128",
        ),
        (
            Command::Place(order(129, Side::Buy, 100, 1)),
            "OrderId=129",
            "1..=128",
        ),
        (
            Command::Place(order(1, Side::Buy, 79, 1)),
            "price=79",
            "80..=120",
        ),
        (
            Command::Place(order(1, Side::Buy, 121, 1)),
            "price=121",
            "80..=120",
        ),
        (
            Command::Place(order(1, Side::Buy, 100, 101)),
            "qty=101",
            "1..=100",
        ),
    ];

    for (invalid, field, range) in cases {
        let tape = [Command::Cancel(OrderId::new(1)), invalid];
        let error = validate_tape_domain(&tape).unwrap_err();
        assert!(error.contains("command[1]"), "{error}");
        assert!(error.contains(field), "{error}");
        assert!(error.contains(range), "{error}");
    }
}

/// 同一条下单多字段非法时，只报告最高优先级的 OrderId 错误。
#[test]
fn tape_domain_prioritizes_order_id_over_price_and_qty() {
    let tape = [Command::Place(order(0, Side::Buy, 121, 101))];
    let error = validate_tape_domain(&tape).unwrap_err();

    assert!(error.contains("command[0]"), "{error}");
    assert!(error.contains("OrderId=0"), "{error}");
    assert!(error.contains("1..=128"), "{error}");
    assert!(!error.contains("price="), "{error}");
    assert!(!error.contains("qty="), "{error}");
}

#[test]
#[should_panic(expected = "invalid differential tape domain: command[0] Place price=121")]
fn run_tape_rejects_out_of_domain_input_before_execution() {
    let _ = run_tape(&[Command::Place(order(1, Side::Buy, 121, 1))]);
}

/// 两种包装器必须共用执行循环，且各自处理所有短命令序列命令。
#[test]
fn collected_and_streamed_short_tape_process_every_step() {
    let tape = buy_taker_tape();
    assert_eq!(run_tape(&tape).len(), tape.len());
    assert_eq!(run_tape_streaming(&tape), tape.len());
}

#[test]
fn streamed_fixed_seed_1000_tape_processes_every_step() {
    const SEED: [u8; 32] = [0x05; 32];
    let seed_hex = SEED
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>();
    println!(
        "generator={}, algorithm=ChaCha, seed=0x{}, length=1000",
        TAPE_GENERATOR_VERSION, seed_hex
    );
    let tape = generate_tape(SEED, 1000);
    assert_eq!(run_tape_streaming(&tape), 1000);
}

#[test]
fn empty_tape_streaming_returns_zero() {
    assert_eq!(run_tape_streaming(&[]), 0);
    assert!(run_tape(&[]).is_empty());
}

#[test]
fn fixed_seed_generator_is_reproducible_and_bounded() {
    let seed = [0x05; 32];
    assert!(generate_tape(seed, 0).is_empty());
    let first = generate_tape(seed, 1000);
    let second = generate_tape(seed, 1000);
    assert_eq!(first, second);
    assert_eq!(validate_tape_domain(&first), Ok(()));
}

#[test]
#[should_panic(expected = "generate_tape length=100001 outside allowed range 0..=100000")]
fn generated_tape_rejects_overlong_length() {
    let _ = generate_tape([0x05; 32], 100_001);
}

/// T05.4：新的最大长度在输入域中合法；仍只验证输入，不回放该命令序列。
#[test]
fn tape_domain_accepts_100k_boundary() {
    let tape: Vec<_> = (0..MAX_TAPE_LENGTH)
        .map(|_| Command::Cancel(OrderId::new(1)))
        .collect();
    assert_eq!(tape.len(), 100_000);
    assert_eq!(validate_tape_domain(&tape), Ok(()));
}

/// T05.4：生成器必须先通过长度门禁，之后才允许初始化 RNG / 分配 Vec。
#[test]
fn generated_tape_accepts_100k_boundary() {
    let tape = generate_tape([0x05; 32], MAX_TAPE_LENGTH);
    assert_eq!(tape.len(), 100_000);
    assert_eq!(validate_tape_domain(&tape), Ok(()));
}

/// T05.4：固定种子的 100k 单条命令序列，逐命令检查但不保留 StepRecord。
#[test]
fn streamed_fixed_seed_100k_tape_processes_every_step() {
    const SEED: [u8; 32] = [0x05; 32];
    const LENGTH: usize = 100_000;

    let seed_hex = SEED
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>();
    println!(
        "generator={}, algorithm=ChaCha, seed=0x{}, length={}",
        TAPE_GENERATOR_VERSION, seed_hex, LENGTH
    );

    let tape = generate_tape(SEED, LENGTH);
    assert_eq!(validate_tape_domain(&tape), Ok(()));
    assert_eq!(run_tape_streaming(&tape), LENGTH);
}

/// T05.5 两个具名固定种子；每个独立生成两次并完整比较输入。
/// 所有 §54 声明的覆盖范围见 assert_regular_book_invariants 注释。
/// 这不是止损订单 / 到期索引 / WAL / 摘要 / 崩溃 / 跨分片断言。
fn assert_reproducible_streamed_10k(name: &str, seed: [u8; 32]) {
    const LENGTH: usize = 10_000;
    let seed_hex = seed
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>();
    println!(
        "name={name}, generator={}, algorithm=ChaCha, seed=0x{}, length={LENGTH}",
        TAPE_GENERATOR_VERSION, seed_hex
    );
    let first = generate_tape(seed, LENGTH);
    let second = generate_tape(seed, LENGTH);
    assert_eq!(first, second, "generated tape differs for seed {name}");
    assert_eq!(validate_tape_domain(&first), Ok(()));
    assert_eq!(validate_tape_domain(&second), Ok(()));
    assert_eq!(run_tape_streaming(&first), LENGTH);
    assert_eq!(run_tape_streaming(&second), LENGTH);
}

#[test]
fn streamed_fixed_seed_10k_alpha_is_reproducible() {
    assert_reproducible_streamed_10k("alpha", [0x0a; 32]);
}

#[test]
fn streamed_fixed_seed_10k_beta_is_reproducible() {
    assert_reproducible_streamed_10k("beta", [0x0b; 32]);
}
