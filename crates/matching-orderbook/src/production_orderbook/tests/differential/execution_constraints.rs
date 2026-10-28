//! 独立执行策略的命令序列差分测试与语义性质测试。
//! 仅共享状态观察、不变量检查和既有 GTC 适配器；
//! IOC/FOK/PostOnly 分别调用两本订单簿各自独立的公开实现。
use super::*;
use matching_domain::{CancelReason, LimitImmediateExecution};

#[derive(Clone, Debug, PartialEq, Eq)]
enum ConstraintCommand {
    Gtc(LimitOrder),
    Cancel(OrderId),
    Ioc(LimitOrder),
    Fok(LimitOrder),
    PostOnly(LimitOrder),
}

#[derive(Debug, PartialEq, Eq)]
enum ConstraintOutcome {
    Gtc(Result<Vec<TradeEvent>, RejectReason>),
    Cancel(Result<ObservedOrder, RejectReason>),
    Immediate(Result<LimitImmediateExecution, RejectReason>),
    PostOnly(Result<(), RejectReason>),
}

impl ConstraintOutcome {
    fn rejected(&self) -> bool {
        match self {
            Self::Gtc(result) => result.is_err(),
            Self::Cancel(result) => result.is_err(),
            Self::Immediate(result) => result.is_err(),
            Self::PostOnly(result) => result.is_err(),
        }
    }
}

#[derive(Debug, PartialEq, Eq)]
struct ConstraintRecord {
    /// 当前执行约束命令的统一结果。
    outcome: ConstraintOutcome,
    /// 当前命令提交后的整簿业务观察。
    book: ObservedBook,
    /// 生产簿下一次将分配的队列优先级数值。
    next_priority: u128,
}

fn constraint_tape(seed: [u8; 32], length: usize) -> Vec<ConstraintCommand> {
    assert!(length <= MAX_TAPE_LENGTH);
    let rng = TestRng::from_seed(RngAlgorithm::ChaCha, &seed);
    let mut runner = TestRunner::new_with_rng(Config::default(), rng);
    // 为 GTC 设置两份生成权重以补充流动性；有限的标识符范围同时覆盖重复，
    // 以及取消、标识符复用、槽位复用和挂单部分成交。
    let strategy = (0u8..6, 1u128..=128, 80i64..=120, 1u64..=100, any::<bool>());
    (0..length)
        .map(|index| {
            let (kind, id, price, qty, buy) = strategy
                .new_tree(&mut runner)
                .unwrap_or_else(|error| panic!("seed={seed:02x?}, index={index}: {error:?}"))
                .current();
            let candidate = order(id, if buy { Side::Buy } else { Side::Sell }, price, qty);
            match kind {
                0 | 1 => ConstraintCommand::Gtc(candidate),
                2 => ConstraintCommand::Cancel(candidate.order_id),
                3 => ConstraintCommand::Ioc(candidate),
                4 => ConstraintCommand::Fok(candidate),
                5 => ConstraintCommand::PostOnly(candidate),
                _ => unreachable!(),
            }
        })
        .collect()
}

fn crossing_orders<'a>(before: &'a ObservedBook, incoming: &LimitOrder) -> Vec<&'a ObservedOrder> {
    let makers = match incoming.side {
        Side::Buy => &before.asks,
        Side::Sell => &before.bids,
    };
    makers
        .orders
        .iter()
        .filter(|maker| match incoming.side {
            Side::Buy => maker.original.price <= incoming.price,
            Side::Sell => maker.original.price >= incoming.price,
        })
        .collect()
}

/// 独立于两套撮合器验证数量守恒及原始价格优先、同价先进先出前缀，
/// 此处无需调用共享领域算术来计算期望结果。
fn assert_immediate_semantics(
    incoming: &LimitOrder,
    fok: bool,
    before: &ObservedBook,
    result: &Result<LimitImmediateExecution, RejectReason>,
) {
    let duplicate = before
        .bids
        .orders
        .iter()
        .chain(&before.asks.orders)
        .any(|entry| entry.original.order_id == incoming.order_id);
    if duplicate {
        assert_eq!(*result, Err(RejectReason::DuplicateOrderId));
        return;
    }
    let makers = crossing_orders(before, incoming);
    let available = makers
        .iter()
        .try_fold(0u64, |sum, maker| sum.checked_add(maker.remaining))
        .unwrap();
    if fok && available < incoming.qty.get() {
        assert_eq!(*result, Err(RejectReason::FokInsufficientLiquidity));
        return;
    }
    let execution = result.as_ref().expect("valid immediate order must execute");
    let mut remaining = incoming.qty.get();
    let mut trade_count = 0usize;
    for maker in makers {
        if remaining == 0 {
            break;
        }
        let qty = remaining.min(maker.remaining);
        let EngineEvent::Trade(trade) = &execution.events()[trade_count] else {
            panic!("cancellation must follow every trade");
        };
        assert_eq!(trade.maker_order_id, maker.original.order_id);
        assert_eq!(trade.taker_order_id, incoming.order_id);
        assert_eq!(trade.maker_side, opposite_side(incoming.side));
        assert_eq!(trade.price, maker.original.price);
        assert_eq!(trade.qty.get(), qty);
        remaining = remaining.checked_sub(qty).unwrap();
        trade_count = trade_count.checked_add(1).unwrap();
    }
    assert_eq!(
        execution.canceled_remaining().map(Qty::get),
        if remaining == 0 {
            None
        } else {
            Some(remaining)
        }
    );
    if remaining > 0 {
        assert!(!fok, "FOK must be all-or-zero");
        assert_eq!(
            execution.events().last(),
            Some(&EngineEvent::OrderCanceled {
                order_id: incoming.order_id,
                remaining: matching_domain::OrderRemaining::Base(Qty::try_new(remaining).unwrap()),
                reason: CancelReason::IocRemainder,
            })
        );
        trade_count = trade_count.checked_add(1).unwrap();
    }
    assert_eq!(execution.events().len(), trade_count);
}

fn execute_constraints(
    commands: &[ConstraintCommand],
    seed: [u8; 32],
    mut on_record: impl FnMut(ConstraintRecord),
) {
    let mut reference = ReferenceOrderBook::new();
    let mut production = ProductionOrderBook::new(policy());
    let math = MarketMath::try_new(0, 0, 0, 1, 1).unwrap();
    let mut ids = TradeIdSource::new();
    let mut next_priority = 0u128;
    let mut seen_trade_ids = BTreeSet::new();
    for (index, command) in commands.iter().enumerate() {
        // 即使共享不变量检查器或任一撮合实现内部触发断言，
        // 也附带可用于重放定位的种子和命令位置。
        let checked = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let before = observe_reference(&reference);
            assert_eq!(before, observe_production(&production));
            let bids = level_snapshot(&production.bids);
            let asks = level_snapshot(&production.asks);
            let orders = production.orders.clone();
            let arena = production.arena.test_snapshot();
            let allocator_policy = *production.queue_priority_allocator.policy();
            let priority_before = next_priority;
            let outcome = match command {
                ConstraintCommand::Gtc(incoming) => {
                    let supplied = ids.take(reference.orders(opposite_side(incoming.side)).len());
                    let expected = place_reference(&mut reference, incoming.clone(), &supplied);
                    let actual = production.place_limit_gtc(incoming.clone(), &supplied);
                    let expected = expected.map(|placed| {
                        if placed.rested {
                            next_priority = next_priority.checked_add(1).unwrap();
                        }
                        placed.trades
                    });
                    assert_eq!(expected, actual);
                    ConstraintOutcome::Gtc(actual)
                }
                ConstraintCommand::Cancel(id) => {
                    let expected = reference
                        .cancel(*id)
                        .map(|removed| observe_reference_order(&removed));
                    let actual = production
                        .cancel(*id)
                        .map(|removed| observe_production_order(&removed));
                    assert_eq!(expected, actual);
                    ConstraintOutcome::Cancel(actual)
                }
                ConstraintCommand::Ioc(incoming) | ConstraintCommand::Fok(incoming) => {
                    let supplied = ids.take(reference.orders(opposite_side(incoming.side)).len());
                    let fok = matches!(command, ConstraintCommand::Fok(_));
                    let (expected, actual) = if fok {
                        (
                            reference.place_limit_fok(incoming.clone(), &math, &supplied),
                            production.place_limit_fok(incoming.clone(), &math, &supplied),
                        )
                    } else {
                        (
                            reference.place_limit_ioc(incoming.clone(), &math, &supplied),
                            production.place_limit_ioc(incoming.clone(), &math, &supplied),
                        )
                    };
                    assert_eq!(expected, actual, "ordered events/result mismatch");
                    assert_immediate_semantics(incoming, fok, &before, &actual);
                    if actual.is_ok() {
                        assert!(!reference_contains_order(&reference, incoming.order_id));
                        assert!(production.order(incoming.order_id).is_none());
                    }
                    ConstraintOutcome::Immediate(actual)
                }
                ConstraintCommand::PostOnly(incoming) => {
                    let duplicate = reference_contains_order(&reference, incoming.order_id);
                    let crosses = !crossing_orders(&before, incoming).is_empty();
                    let contract = if duplicate {
                        Err(RejectReason::DuplicateOrderId)
                    } else if crosses {
                        Err(RejectReason::PostOnlyWouldCross)
                    } else {
                        Ok(())
                    };
                    let expected = reference.place_limit_post_only(incoming.clone());
                    let actual = production.place_limit_post_only(incoming.clone());
                    assert_eq!(expected, actual);
                    assert_eq!(actual, contract);
                    if actual.is_ok() {
                        next_priority = next_priority.checked_add(1).unwrap();
                        let after = observe_production(&production);
                        // 所有既有挂单的各项状态均保持不变；
                        // 即使 PostOnly 接口返回空元组，也不能隐藏任何成交行为。
                        for entry in before.bids.orders.iter().chain(&before.asks.orders) {
                            let observed = after
                                .bids
                                .orders
                                .iter()
                                .chain(&after.asks.orders)
                                .find(|live| live.original.order_id == entry.original.order_id);
                            assert_eq!(observed, Some(entry));
                        }
                        let rested = production.order(incoming.order_id).unwrap();
                        assert_eq!(rested.remaining(), incoming.qty.get());
                        assert_eq!(rested.priority().get(), priority_before);
                    }
                    ConstraintOutcome::PostOnly(actual)
                }
            };
            let after = observe_production(&production);
            assert_eq!(observe_reference(&reference), after);
            if outcome.rejected() {
                assert_eq!(after, before, "rejection changed semantic state");
                assert_production_failure_state_unchanged(
                    &production,
                    &bids,
                    &asks,
                    &orders,
                    &arena,
                    priority_before,
                    allocator_policy,
                );
            }
            assert_eq!(production.next_queue_priority(), next_priority);
            assert_regular_book_invariants(&after, next_priority, index);
            let trades: Vec<&TradeEvent> = match &outcome {
                ConstraintOutcome::Gtc(Ok(trades)) => trades.iter().collect(),
                ConstraintOutcome::Immediate(Ok(execution)) => execution
                    .events()
                    .iter()
                    .filter_map(|event| {
                        if let EngineEvent::Trade(trade) = event {
                            Some(trade)
                        } else {
                            None
                        }
                    })
                    .collect(),
                _ => Vec::new(),
            };
            for trade in trades {
                assert!(trade.qty.get() > 0);
                assert!(
                    seen_trade_ids.insert(trade.trade_id.get()),
                    "reused TradeId"
                );
            }
            on_record(ConstraintRecord {
                outcome,
                book: after,
                next_priority,
            });
        }));
        if let Err(payload) = checked {
            let reason = payload
                .downcast_ref::<String>()
                .map(String::as_str)
                .or_else(|| payload.downcast_ref::<&str>().copied())
                .unwrap_or("non-string panic");
            panic!(
                "constraint-tape-v1 algorithm=ChaCha seed={seed:02x?} index={index} command={command:?}: {reason}"
            );
        }
    }
}

#[test]
fn mixed_constraints_fixed_seed_100k_checks_every_command() {
    let seed = [0x71; 32];
    let tape = constraint_tape(seed, 100_000);
    let mut count = 0usize;
    let mut branches = [[0usize; 2]; 5];
    execute_constraints(&tape, seed, |record| {
        let kind = match &tape[count] {
            ConstraintCommand::Gtc(_) => 0,
            ConstraintCommand::Cancel(_) => 1,
            ConstraintCommand::Ioc(_) => 2,
            ConstraintCommand::Fok(_) => 3,
            ConstraintCommand::PostOnly(_) => 4,
        };
        let rejected = usize::from(record.outcome.rejected());
        branches[kind][rejected] = branches[kind][rejected].checked_add(1).unwrap();
        count = count.checked_add(1).unwrap();
    });
    assert_eq!(count, 100_000);
    assert!(
        branches.iter().flatten().all(|count| *count > 0),
        "missing success/rejection branch: {branches:?}"
    );
}

#[test]
fn mirrored_fifo_price_boundary_and_post_only_later_maker_tape() {
    for side in [Side::Buy, Side::Sell] {
        let maker_side = opposite_side(side);
        let farther = if side == Side::Buy { 101 } else { 99 };
        let tape = [
            ConstraintCommand::Gtc(order(30, maker_side, 100, 3)),
            ConstraintCommand::Gtc(order(10, maker_side, 100, 7)),
            ConstraintCommand::Gtc(order(20, maker_side, farther, 4)),
            // 订单簿总量足够，但价格范围内可成交量只有 10。
            ConstraintCommand::Fok(order(90, side, 100, 11)),
            ConstraintCommand::PostOnly(order(91, side, 100, 1)),
            ConstraintCommand::Ioc(order(92, side, 100, 8)),
            // 第二笔挂单现已部分成交；恰好满足总量的 FOK 将跨越两个价格。
            ConstraintCommand::Fok(order(93, side, farther, 6)),
            ConstraintCommand::PostOnly(order(94, maker_side, 100, 6)),
            ConstraintCommand::Ioc(order(95, side, 100, 8)),
        ];
        let mut records = Vec::new();
        execute_constraints(&tape, [0x77; 32], |record| records.push(record));
        assert_eq!(
            records[3].outcome,
            ConstraintOutcome::Immediate(Err(RejectReason::FokInsufficientLiquidity))
        );
        assert_eq!(
            records[4].outcome,
            ConstraintOutcome::PostOnly(Err(RejectReason::PostOnlyWouldCross))
        );
        let ConstraintOutcome::Immediate(Ok(last)) = &records[8].outcome else {
            panic!("IOC must succeed");
        };
        assert_eq!(last.canceled_remaining().map(Qty::get), Some(2));
        assert!(
            matches!(&last.events()[0], EngineEvent::Trade(trade) if trade.maker_order_id == OrderId::new(94) && trade.qty.get() == 6)
        );
        assert_eq!(records[8].next_priority, 4);
        assert!(records[8].book.bids.orders.is_empty());
        assert!(records[8].book.asks.orders.is_empty());
    }
}

#[test]
fn mixed_constraints_two_fixed_seed_10k_tapes_repeat_exact_transcripts() {
    for seed in [[0x72; 32], [0x73; 32]] {
        let first = constraint_tape(seed, 10_000);
        let second = constraint_tape(seed, 10_000);
        assert_eq!(first, second);
        let mut transcript = Vec::with_capacity(first.len());
        execute_constraints(&first, seed, |record| transcript.push(record));
        let mut prior = transcript.into_iter();
        execute_constraints(&second, seed, |record| {
            assert_eq!(Some(record), prior.next())
        });
        assert!(prior.next().is_none());
    }
}

fn property_tape(
    makers: &[(i64, u64)],
    buy: bool,
    limit: i64,
    qty: u64,
    kind: u8,
) -> Vec<ConstraintCommand> {
    let side = if buy { Side::Buy } else { Side::Sell };
    let mut tape: Vec<_> = makers
        .iter()
        .enumerate()
        .map(|(index, (price, qty))| {
            let id = u128::try_from(index).unwrap().checked_add(1).unwrap();
            ConstraintCommand::Gtc(order(id, opposite_side(side), *price, *qty))
        })
        .collect();
    let candidate = order(1000, side, limit, qty);
    tape.push(match kind {
        0 => ConstraintCommand::Ioc(candidate),
        1 => ConstraintCommand::Fok(candidate),
        2 => ConstraintCommand::PostOnly(candidate),
        _ => unreachable!(),
    });
    tape
}

proptest! {
    #![proptest_config(Config { cases: 256, rng_seed: proptest::test_runner::RngSeed::Fixed(0x710cf0), ..Config::default() })]

    #[test]
    fn ioc_quantity_conservation_and_never_rests(
        makers in prop::collection::vec((80i64..=120, 1u64..=100), 0..32),
        buy in any::<bool>(), limit in 80i64..=120, qty in 1u64..=1000,
    ) {
        execute_constraints(&property_tape(&makers, buy, limit, qty, 0), [0x74; 32], |_| {});
    }

    #[test]
    fn fok_all_or_zero_at_executable_liquidity_boundary(
        makers in prop::collection::vec((80i64..=120, 1u64..=100), 0..32),
        buy in any::<bool>(), limit in 80i64..=120, qty in 1u64..=1000,
    ) {
        let available = makers.iter().filter(|(price, _)| if buy { *price <= limit } else { *price >= limit })
            .try_fold(0u64, |sum, (_, qty)| sum.checked_add(*qty)).unwrap();
        let quantities = [qty, available.max(1), available.checked_add(1).unwrap()];
        for requested in quantities {
            execute_constraints(&property_tape(&makers, buy, limit, requested, 1), [0x75; 32], |_| {});
        }
    }

    #[test]
    fn post_only_never_taker_and_preserves_every_existing_maker(
        makers in prop::collection::vec((80i64..=120, 1u64..=100), 0..32),
        buy in any::<bool>(), limit in 80i64..=120, qty in 1u64..=1000,
    ) {
        execute_constraints(&property_tape(&makers, buy, limit, qty, 2), [0x76; 32], |_| {});
    }
}

#[test]
fn immediate_empty_book_is_canceled_or_rejected_without_resting() {
    let mut reference = ReferenceOrderBook::new();
    let mut production = ProductionOrderBook::new(policy());
    let math = MarketMath::try_new(0, 0, 0, 1, 1).unwrap();
    let incoming = order(1, Side::Buy, 100, 5);
    let actual = production
        .place_limit_ioc(incoming.clone(), &math, &[])
        .unwrap();
    assert_eq!(
        reference.place_limit_ioc(incoming.clone(), &math, &[]),
        Ok(actual.clone())
    );
    assert_eq!(
        actual.events(),
        &[EngineEvent::OrderCanceled {
            order_id: incoming.order_id,
            remaining: matching_domain::OrderRemaining::Base(incoming.qty),
            reason: CancelReason::IocRemainder,
        }]
    );
    assert_eq!(
        production.place_limit_fok(incoming.clone(), &math, &[]),
        Err(RejectReason::FokInsufficientLiquidity)
    );
    assert_eq!(
        reference.place_limit_fok(incoming, &math, &[]),
        Err(RejectReason::FokInsufficientLiquidity)
    );
    assert_eq!(
        observe_reference(&reference),
        observe_production(&production)
    );
    assert_eq!(production.next_queue_priority(), 0);
}
