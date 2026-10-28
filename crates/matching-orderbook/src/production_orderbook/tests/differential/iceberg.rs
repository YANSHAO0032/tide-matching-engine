//! 冰山命令序列差分测试：向基于 Vec 的参考簿和侵入式链表生产簿
//! 输入相同外部命令；任何一方的结果都不参与另一方的执行计划。
use super::*;
use matching_domain::market_order::ProtectedMarketByQuoteExecution;
use matching_domain::{IcebergOrder, LimitImmediateExecution};

#[derive(Clone, Debug, PartialEq, Eq)]
enum IceCommand {
    Iceberg(IcebergOrder),
    IcebergPostOnly(IcebergOrder),
    Gtc(LimitOrder),
    Ioc(LimitOrder),
    Fok(LimitOrder),
    PostOnly(LimitOrder),
    ProtectedBase(MarketByBase),
    ProtectedQuote(MarketByQuote),
    Cancel(OrderId),
}

#[derive(Debug, PartialEq, Eq)]
enum IceOutcome {
    Events(Result<Vec<EngineEvent>, RejectReason>),
    Immediate(Result<LimitImmediateExecution, RejectReason>),
    PostOnly(Result<(), RejectReason>),
    Quote(Result<ProtectedMarketByQuoteExecution, RejectReason>),
    Cancel(Result<IceOrder, RejectReason>),
}

impl IceOutcome {
    fn rejected(&self) -> bool {
        match self {
            Self::Events(value) => value.is_err(),
            Self::Immediate(value) => value.is_err(),
            Self::PostOnly(value) => value.is_err(),
            Self::Quote(value) => value.is_err(),
            Self::Cancel(value) => value.is_err(),
        }
    }

    fn events(&self) -> &[EngineEvent] {
        match self {
            Self::Events(Ok(events)) => events,
            Self::Immediate(Ok(execution)) => execution.events(),
            Self::Quote(Ok(execution)) => execution.events(),
            _ => &[],
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct IceOrder {
    /// 不随成交扣减改变的原始限价订单载荷。
    original: LimitOrder,
    /// 当前可参与深度与撮合的可见基础数量。
    visible: u64,
    /// 当前尚未展示的隐藏基础数量。
    hidden: u64,
    /// 可见与隐藏数量相加得到的总剩余基础数量。
    total: u64,
    /// 冰山订单的可选单次展示峰值。
    peak: Option<Qty>,
    /// 挂单当前持有的市场本地队列优先级。
    priority: QueuePriority,
}

#[derive(Debug, PartialEq, Eq)]
struct IceState {
    /// 只含可见数量的整簿业务观察。
    visible_book: ObservedBook,
    /// 按业务顺序排列的买侧冰山订单投影。
    bids: Vec<IceOrder>,
    /// 按业务顺序排列的卖侧冰山订单投影。
    asks: Vec<IceOrder>,
    /// 下一次将分配的队列优先级数值。
    next_priority: u128,
}

fn reference_order(value: &ReferenceOrder) -> IceOrder {
    IceOrder {
        original: value.original_order().clone(),
        visible: value.visible_remaining(),
        hidden: value.hidden_remaining(),
        total: value.total_remaining(),
        peak: value.peak_size(),
        priority: value.priority(),
    }
}

fn production_order(value: &OrderNode) -> IceOrder {
    IceOrder {
        original: value.original_order().clone(),
        visible: value.visible_remaining(),
        hidden: value.hidden_remaining(),
        total: value.total_remaining(),
        peak: value.peak_size(),
        priority: value.priority(),
    }
}

fn reference_state(book: &ReferenceOrderBook, next_priority: u128) -> IceState {
    IceState {
        visible_book: observe_reference(book),
        bids: book
            .orders(Side::Buy)
            .into_iter()
            .map(reference_order)
            .collect(),
        asks: book
            .orders(Side::Sell)
            .into_iter()
            .map(reference_order)
            .collect(),
        next_priority,
    }
}

fn production_state(book: &ProductionOrderBook) -> IceState {
    let visible_book = observe_production(book);
    let project = |entries: &[ObservedOrder]| {
        entries
            .iter()
            .map(|entry| {
                production_order(
                    book.order(entry.original.order_id)
                        .expect("visible order missing from ID index"),
                )
            })
            .collect()
    };
    let bids = project(&visible_book.bids.orders);
    let asks = project(&visible_book.asks.orders);
    IceState {
        visible_book,
        bids,
        asks,
        next_priority: book.next_queue_priority(),
    }
}

fn live_order(state: &IceState, id: OrderId) -> Option<&IceOrder> {
    state
        .bids
        .iter()
        .chain(&state.asks)
        .find(|order| order.original.order_id == id)
}

fn check_state(state: &IceState) {
    assert_regular_book_invariants(&state.visible_book, state.next_priority, 0);
    for (entries, side) in [
        (&state.bids, &state.visible_book.bids),
        (&state.asks, &state.visible_book.asks),
    ] {
        assert_eq!(entries.len(), side.orders.len());
        for (detail, visible) in entries.iter().zip(&side.orders) {
            assert_eq!(detail.original, visible.original);
            assert_eq!(detail.priority, visible.priority);
            assert_eq!(detail.visible, visible.remaining);
            assert_eq!(
                detail.total,
                detail.visible.checked_add(detail.hidden).unwrap()
            );
            assert!(detail.total > 0 && detail.total <= detail.original.qty.get());
            match detail.peak {
                Some(peak) => {
                    assert!(detail.visible > 0 && detail.visible <= peak.get());
                    assert!(peak.get() < detail.original.qty.get());
                }
                None => assert_eq!(detail.hidden, 0),
            }
        }
        // 公开深度只累计当前可见片的数量。
        for level in &side.depth {
            let visible_sum: u64 = entries
                .iter()
                .filter(|entry| entry.original.price == level.price)
                .map(|entry| entry.visible)
                .try_fold(0u64, |sum, qty| sum.checked_add(qty))
                .unwrap();
            assert_eq!(level.total_qty, visible_sum);
        }
    }
}

fn ice(id: u128, side: Side, price: i64, total: u64, peak: u64) -> IcebergOrder {
    IcebergOrder::try_new(order(id, side, price, total), Qty::try_new(peak).unwrap()).unwrap()
}

fn ice_tape(seed: [u8; 32], length: usize) -> Vec<IceCommand> {
    assert!(length <= MAX_TAPE_LENGTH);
    let rng = TestRng::from_seed(RngAlgorithm::ChaCha, &seed);
    let mut runner = TestRunner::new_with_rng(Config::default(), rng);
    let strategy = (0u8..11, 1u128..=128, 85i64..=115, 2u64..=20, any::<bool>());
    (0..length)
        .map(|index| {
            let (kind, id, price, total, buy) = strategy
                .new_tree(&mut runner)
                .unwrap_or_else(|error| panic!("seed={seed:02x?} index={index}: {error:?}"))
                .current();
            let side = if buy { Side::Buy } else { Side::Sell };
            let ordinary = order(id, side, price, total);
            let iceberg = || ice(id, side, price, total, 1 + total / 3);
            match kind {
                0 | 1 => IceCommand::Iceberg(iceberg()),
                2 => IceCommand::IcebergPostOnly(iceberg()),
                3 => IceCommand::Gtc(ordinary),
                4 => IceCommand::Ioc(ordinary),
                5 => IceCommand::Fok(ordinary),
                6 => IceCommand::PostOnly(ordinary),
                7 | 8 => IceCommand::Cancel(OrderId::new(id)),
                9 => IceCommand::ProtectedBase(MarketByBase {
                    order_id: OrderId::new(id),
                    user_id: UserId::new(1),
                    side,
                    qty: Qty::try_new(total).unwrap(),
                    market_protection: Some(MarketProtection {
                        max_slippage_bps: None,
                        worst_price: Some(Price::try_new(100).unwrap()),
                    }),
                }),
                10 => IceCommand::ProtectedQuote(MarketByQuote {
                    order_id: OrderId::new(id),
                    user_id: UserId::new(1),
                    side,
                    quote_budget: QuoteBudget::try_new(u128::from(total) * 100).unwrap(),
                    market_protection: Some(MarketProtection {
                        max_slippage_bps: None,
                        worst_price: Some(Price::try_new(100).unwrap()),
                    }),
                }),
                _ => unreachable!(),
            }
        })
        .collect()
}

fn run_ice_tape(
    commands: &[IceCommand],
    seed: [u8; 32],
    mut record: impl FnMut(IceOutcome, IceState),
) {
    let mut reference = ReferenceOrderBook::new();
    let mut production = ProductionOrderBook::new(policy());
    let math = MarketMath::try_new(0, 0, 0, 1, 1).unwrap();
    let protection = CommandStartProtectionContext {
        reference_price_at_command_start: Price::try_new(100).unwrap(),
        market_band_worst: None,
    };
    let mut trade_ids = TradeIdSource::new();
    let mut next_priority = 0u128;
    let mut seen_trades = BTreeSet::new();
    for (index, command) in commands.iter().enumerate() {
        let checked = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let before = production_state(&production);
            assert_eq!(reference_state(&reference, next_priority), before);
            let bids = level_snapshot(&production.bids);
            let asks = level_snapshot(&production.asks);
            let orders = production.orders.clone();
            let arena = production.arena.test_snapshot();
            let allocator_policy = *production.queue_priority_allocator.policy();
            let supplied = trade_ids.take(64);
            let (expected, actual, incoming_id) = match command {
                IceCommand::Iceberg(value) => (
                    IceOutcome::Events(reference.place_iceberg(value.clone(), &math, &supplied)),
                    IceOutcome::Events(production.place_iceberg(value.clone(), &math, &supplied)),
                    Some(value.order().order_id),
                ),
                IceCommand::IcebergPostOnly(value) => (
                    IceOutcome::PostOnly(reference.place_iceberg_post_only(value.clone(), &math)),
                    IceOutcome::PostOnly(production.place_iceberg_post_only(value.clone(), &math)),
                    Some(value.order().order_id),
                ),
                IceCommand::Gtc(value) => (
                    IceOutcome::Events(
                        reference.place_limit_gtc_with_events(value.clone(), &supplied),
                    ),
                    IceOutcome::Events(
                        production.place_limit_gtc_with_events(value.clone(), &supplied),
                    ),
                    Some(value.order_id),
                ),
                IceCommand::Ioc(value) => (
                    IceOutcome::Immediate(reference.place_limit_ioc(
                        value.clone(),
                        &math,
                        &supplied,
                    )),
                    IceOutcome::Immediate(production.place_limit_ioc(
                        value.clone(),
                        &math,
                        &supplied,
                    )),
                    Some(value.order_id),
                ),
                IceCommand::Fok(value) => (
                    IceOutcome::Immediate(reference.place_limit_fok(
                        value.clone(),
                        &math,
                        &supplied,
                    )),
                    IceOutcome::Immediate(production.place_limit_fok(
                        value.clone(),
                        &math,
                        &supplied,
                    )),
                    Some(value.order_id),
                ),
                IceCommand::PostOnly(value) => (
                    IceOutcome::PostOnly(reference.place_limit_post_only(value.clone())),
                    IceOutcome::PostOnly(production.place_limit_post_only(value.clone())),
                    Some(value.order_id),
                ),
                IceCommand::ProtectedBase(value) => (
                    IceOutcome::Events(reference.execute_market_by_base_with_protection(
                        value.clone(),
                        &protection,
                        &math,
                        &supplied,
                    )),
                    IceOutcome::Events(production.execute_market_by_base_with_protection(
                        value.clone(),
                        &protection,
                        &math,
                        &supplied,
                    )),
                    Some(value.order_id),
                ),
                IceCommand::ProtectedQuote(value) => (
                    IceOutcome::Quote(reference.execute_market_by_quote_with_protection(
                        value.clone(),
                        &protection,
                        &math,
                        &supplied,
                    )),
                    IceOutcome::Quote(production.execute_market_by_quote_with_protection(
                        value.clone(),
                        &protection,
                        &math,
                        &supplied,
                    )),
                    Some(value.order_id),
                ),
                IceCommand::Cancel(id) => (
                    IceOutcome::Cancel(
                        reference
                            .cancel(*id)
                            .map(|removed| reference_order(&removed)),
                    ),
                    IceOutcome::Cancel(
                        production
                            .cancel(*id)
                            .map(|removed| production_order(&removed)),
                    ),
                    None,
                ),
            };
            assert_eq!(expected, actual, "raw ordered event/result mismatch");
            let after = production_state(&production);
            let refreshes = actual
                .events()
                .iter()
                .filter(|event| matches!(event, EngineEvent::IcebergRefreshed { .. }))
                .count() as u128;
            let newly_rested = incoming_id.is_some_and(|id| {
                live_order(&before, id).is_none() && live_order(&after, id).is_some()
            });
            next_priority = next_priority
                .checked_add(refreshes)
                .unwrap()
                .checked_add(u128::from(newly_rested))
                .unwrap();
            assert_eq!(after.next_priority, next_priority);
            assert_eq!(reference_state(&reference, next_priority), after);
            check_state(&after);
            if actual.rejected() {
                assert_eq!(after, before, "rejected command changed semantic state");
                assert_production_failure_state_unchanged(
                    &production,
                    &bids,
                    &asks,
                    &orders,
                    &arena,
                    before.next_priority,
                    allocator_policy,
                );
            }
            if !actual.rejected()
                && matches!(
                    command,
                    IceCommand::Ioc(_)
                        | IceCommand::Fok(_)
                        | IceCommand::ProtectedBase(_)
                        | IceCommand::ProtectedQuote(_)
                )
            {
                assert!(
                    incoming_id.is_none_or(|id| live_order(&after, id).is_none()),
                    "immediate order rested"
                );
            }
            if matches!(
                command,
                IceCommand::IcebergPostOnly(_) | IceCommand::PostOnly(_)
            ) && !actual.rejected()
            {
                for old in before.bids.iter().chain(&before.asks) {
                    assert_eq!(
                        live_order(&after, old.original.order_id),
                        Some(old),
                        "PostOnly changed a maker"
                    );
                }
                assert!(actual.events().is_empty(), "PostOnly became taker");
            }
            for event in actual.events() {
                if let EngineEvent::Trade(trade) = event {
                    assert!(trade.qty.get() > 0);
                    assert!(seen_trades.insert(trade.trade_id.get()), "TradeId reused");
                }
            }
            record(actual, after);
        }));
        if let Err(payload) = checked {
            let reason = payload
                .downcast_ref::<String>()
                .map(String::as_str)
                .or_else(|| payload.downcast_ref::<&str>().copied())
                .unwrap_or("non-string panic");
            panic!("iceberg-tape-v1 seed={seed:02x?} index={index} command={command:?}: {reason}");
        }
    }
}

#[test]
fn mixed_iceberg_fixed_seed_100k_checks_every_command() {
    let seed = [0x81; 32];
    let tape = ice_tape(seed, 100_000);
    let mut count = 0usize;
    let mut refreshes = 0usize;
    run_ice_tape(&tape, seed, |outcome, _| {
        count += 1;
        refreshes += outcome
            .events()
            .iter()
            .filter(|event| matches!(event, EngineEvent::IcebergRefreshed { .. }))
            .count();
    });
    assert_eq!(count, 100_000);
    assert!(refreshes > 0, "tape never exercised replenish");
}

#[test]
fn fixed_seed_10k_iceberg_tapes_replay_identically() {
    for seed in [[0x82; 32], [0x83; 32]] {
        let first = ice_tape(seed, 10_000);
        assert_eq!(first, ice_tape(seed, 10_000));
        let mut transcript = Vec::with_capacity(first.len());
        run_ice_tape(&first, seed, |outcome, state| {
            transcript.push((outcome, state))
        });
        let mut expected = transcript.into_iter();
        run_ice_tape(&first, seed, |outcome, state| {
            assert_eq!(expected.next(), Some((outcome, state)));
        });
        assert!(expected.next().is_none());
    }
}

#[test]
fn replenished_maker_moves_behind_same_price_peer_and_fok_uses_hidden() {
    for side in [Side::Buy, Side::Sell] {
        let maker = opposite_side(side);
        let tape = [
            IceCommand::Iceberg(ice(1, maker, 100, 6, 2)),
            IceCommand::Gtc(order(2, maker, 100, 2)),
            IceCommand::Fok(order(3, side, 100, 8)),
        ];
        let mut final_events = Vec::new();
        run_ice_tape(&tape, [0x84; 32], |outcome, _| {
            final_events = outcome.events().to_vec()
        });
        let makers: Vec<_> = final_events
            .iter()
            .filter_map(|event| {
                if let EngineEvent::Trade(trade) = event {
                    Some(trade.maker_order_id.get())
                } else {
                    None
                }
            })
            .collect();
        assert_eq!(makers, [1, 2, 1, 1]);
        assert!(final_events.iter().any(|event| matches!(event, EngineEvent::IcebergRefreshed { order_id, visible } if *order_id == OrderId::new(1) && visible.get() == 2)));
    }
}

#[test]
fn cancel_reports_all_hidden_quantity_after_partial_fill() {
    let tape = [
        IceCommand::Iceberg(ice(1, Side::Sell, 100, 9, 3)),
        IceCommand::Ioc(order(2, Side::Buy, 100, 2)),
        IceCommand::Cancel(OrderId::new(1)),
    ];
    let mut removed = None;
    run_ice_tape(&tape, [0x85; 32], |outcome, _| {
        if let IceOutcome::Cancel(Ok(order)) = outcome {
            removed = Some(order);
        }
    });
    let order = removed.unwrap();
    assert_eq!((order.visible, order.hidden, order.total), (1, 6, 7));
}

#[test]
fn replenish_aggregate_overflow_rejects_before_any_fill() {
    let tape = [
        IceCommand::Iceberg(ice(1, Side::Sell, 100, 10, 2)),
        IceCommand::Ioc(order(2, Side::Buy, 100, 1)),
        IceCommand::Gtc(order(3, Side::Sell, 100, u64::MAX - 1)),
        IceCommand::Ioc(order(4, Side::Buy, 100, 1)),
    ];
    let mut outcomes = Vec::new();
    run_ice_tape(&tape, [0x86; 32], |outcome, _| outcomes.push(outcome));
    assert_eq!(
        outcomes.last(),
        Some(&IceOutcome::Immediate(Err(
            RejectReason::ArithmeticOverflow
        )))
    );
}

#[test]
fn exhausted_priority_rejects_refresh_but_allows_partial_visible_fill() {
    let policy = crate::QueuePriorityPolicy::try_with_synthetic_ceiling(1024, Some(2)).unwrap();
    let mut book = ProductionOrderBook::new(policy);
    let math = MarketMath::try_new(0, 0, 0, 1, 1).unwrap();
    book.place_iceberg(ice(1, Side::Sell, 100, 6, 2), &math, &[])
        .unwrap();
    book.rest(order(2, Side::Sell, 100, 1)).unwrap();
    let before = production_state(&book);
    assert_eq!(before.next_priority, 2);
    assert_eq!(
        book.place_limit_fok(order(3, Side::Buy, 100, 2), &math, &[TradeId::new(1)]),
        Err(RejectReason::PrioritySpaceExhaustion)
    );
    assert_eq!(production_state(&book), before);
    let partial = book
        .place_limit_fok(order(4, Side::Buy, 100, 1), &math, &[TradeId::new(2)])
        .unwrap();
    assert_eq!(
        partial
            .events()
            .iter()
            .filter(|event| matches!(event, EngineEvent::Trade(_)))
            .count(),
        1
    );
    assert_eq!(book.next_queue_priority(), 2);
    assert_eq!(book.order(OrderId::new(1)).unwrap().visible_remaining(), 1);
}

proptest! {
    #![proptest_config(Config::with_cases(24))]
    #[test]
    fn iceberg_total_and_public_depth_hold_across_short_random_tapes(seed in any::<[u8; 32]>()) {
        let tape = ice_tape(seed, 120);
        run_ice_tape(&tape, seed, |_, state| check_state(&state));
    }

    #[test]
    fn fok_is_all_or_zero_over_replenishable_hidden(
        total in 2u64..=40, peak_seed in any::<u64>(), requested in 1u64..=50,
        buy_taker in any::<bool>(),
    ) {
        let side = if buy_taker { Side::Buy } else { Side::Sell };
        let maker = opposite_side(side);
        let peak = 1 + peak_seed % (total - 1);
        let mut reference = ReferenceOrderBook::new();
        let mut production = ProductionOrderBook::new(policy());
        let math = MarketMath::try_new(0, 0, 0, 1, 1).unwrap();
        let iceberg = ice(1, maker, 100, total, peak);
        reference.place_iceberg(iceberg.clone(), &math, &[]).unwrap();
        production.place_iceberg(iceberg, &math, &[]).unwrap();
        let before = production_state(&production);
        let incoming = order(2, side, 100, requested);
        let ids: Vec<_> = (1..=50).map(TradeId::new).collect();
        let expected = reference.place_limit_fok(incoming.clone(), &math, &ids);
        let actual = production.place_limit_fok(incoming, &math, &ids);
        assert_eq!(actual, expected);
        if requested > total {
            assert_eq!(actual, Err(RejectReason::FokInsufficientLiquidity));
            assert_eq!(production_state(&production), before);
        } else {
            let execution = actual.unwrap();
            let filled: u64 = execution.events().iter().filter_map(|event| {
                if let EngineEvent::Trade(trade) = event { Some(trade.qty.get()) } else { None }
            }).sum();
            assert_eq!(filled, requested);
            let remaining = production.order(OrderId::new(1)).map_or(0, OrderNode::total_remaining);
            assert_eq!(remaining, total - requested);
        }
        assert_eq!(production_state(&production).visible_book,
            observe_reference(&reference));
    }
}
