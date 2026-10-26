//! 常规改单的独立双模型命令、原序事件、时效及失败原子性验证。

use super::*;
use matching_domain::{
    AmendOrder, AmendedQuantity, AssetId, AssetReserve, CommandId, ExpireReason, ExpiredOrder,
    LogicalClock, LogicalTime, MarketId, ReservationAdjustmentToken, ReservationId, TimeInForce,
};
use matching_risk::ReservationState;

fn shadow(input: &LimitGtcOrder) -> ReservationState {
    ReservationState {
        reservation_id: ReservationId::new(input.order_id.get()),
        order_id: input.order_id,
        user_id: input.user_id,
        market_id: MarketId::new(7),
        version: 11,
        reserves: vec![AssetReserve {
            asset: AssetId::new(3),
            atoms: 17,
        }],
    }
}

fn amend(input: &LimitGtcOrder) -> AmendOrder {
    AmendOrder {
        market_id: MarketId::new(7),
        user_id: input.user_id,
        order_id: input.order_id,
        new_price: None,
        new_qty: None,
        new_quote_budget: None,
        new_visible_qty: None,
        new_stop_price: None,
        new_trigger_reference: None,
        new_trigger_expiry: None,
        new_tif: None,
        new_market_protection: None,
        reservation_adjustment: None,
    }
}

/// 常规改单专用命令域；历史 GTC 随机生成器继续保持原来的可复现输入。
#[derive(Clone, Debug)]
enum Command {
    Rest(LimitGtcOrder, TimeInForce, bool),
    Place(LimitGtcOrder, TimeInForce),
    Amend(Box<AmendOrder>),
    Cancel(OrderId),
    AdvanceTime(LogicalTime),
    EndSession(u64),
}

#[derive(Debug, PartialEq, Eq)]
enum Outcome {
    Unit(Result<(), RejectReason>),
    Events(Result<Vec<EngineEvent>, RejectReason>),
    Expired(Result<Vec<ExpiredOrder>, RejectReason>),
}

impl Outcome {
    fn failed(&self) -> bool {
        match self {
            Self::Unit(result) => result.is_err(),
            Self::Events(result) => result.is_err(),
            Self::Expired(result) => result.is_err(),
        }
    }
}

#[derive(Debug, PartialEq, Eq)]
struct State {
    book: ObservedBook,
    /// 与业务价序及 FIFO 投影保持相同原序，包含完整预留内容和版本。
    timed_shadows: Vec<(OrderId, TimeInForce, Option<ReservationState>)>,
    next: u128,
    stop_next: u128,
}

#[derive(Debug, PartialEq, Eq)]
struct Record {
    outcome: Outcome,
    state: State,
}

fn reference_state(book: &ReferenceOrderBook) -> State {
    State {
        book: observe_reference(book),
        timed_shadows: [Side::Buy, Side::Sell]
            .into_iter()
            .flat_map(|side| book.orders(side))
            .map(|entry| {
                let id = entry.original_order().order_id;
                (
                    id,
                    entry.time_in_force(),
                    book.reservation_state(id).cloned(),
                )
            })
            .collect(),
        next: book.test_deterministic_state().1,
        stop_next: book.next_stop_priority(),
    }
}

fn production_state(book: &ProductionOrderBook) -> State {
    assert_eq!(book.validate_regular_expiry(), Ok(()));
    assert_eq!(book.validate_stop_book(), Ok(()));
    let observed = observe_production(book);
    let timed_shadows = observed
        .bids
        .orders
        .iter()
        .chain(&observed.asks.orders)
        .map(|entry| {
            let id = entry.original.order_id;
            (
                id,
                book.order(id).unwrap().time_in_force(),
                book.reservation_state(id).cloned(),
            )
        })
        .collect();
    State {
        book: observed,
        timed_shadows,
        next: book.next_queue_priority(),
        stop_next: book.next_stop_priority(),
    }
}

/// 失败时额外比较物理槽位、空闲链、索引及完整预留映射，不能只比较语义终态。
fn production_snapshot(book: &ProductionOrderBook) -> String {
    let mut index = book.orders.iter().collect::<Vec<_>>();
    index.sort_unstable_by_key(|(id, _)| **id);
    let mut reservations = book.reservations.iter().collect::<Vec<_>>();
    reservations.sort_unstable_by_key(|(id, _)| **id);
    format!(
        "{:?}",
        (
            level_snapshot(&book.bids),
            level_snapshot(&book.asks),
            book.arena.test_snapshot(),
            index,
            reservations,
            &book.regular_expiry,
            book.next_queue_priority(),
            book.next_stop_priority(),
            [book.stop_orders(Side::Buy), book.stop_orders(Side::Sell)],
            &book.reference_prices
        )
    )
}

struct Harness {
    reference: ReferenceOrderBook,
    production: ProductionOrderBook,
    clock: LogicalClock,
    ids: TradeIdSource,
    known_ids: BTreeSet<OrderId>,
    step: usize,
}

impl Harness {
    fn new() -> Self {
        Self {
            reference: ReferenceOrderBook::new(),
            production: ProductionOrderBook::new(policy()),
            clock: LogicalClock::new(LogicalTime::new(1)),
            ids: TradeIdSource::new(),
            known_ids: BTreeSet::new(),
            step: 0,
        }
    }

    fn run(&mut self, command: Command) -> Record {
        assert_eq!(
            reference_state(&self.reference),
            production_state(&self.production)
        );
        let reference_before = self.reference.test_deterministic_state();
        let production_before = production_snapshot(&self.production);
        let now = self.clock.now();
        // 本命令域只有普通 maker，每张最多一笔成交，固定容量先于调用方状态变更验证。
        assert!(
            self.reference.orders(Side::Buy).len() + self.reference.orders(Side::Sell).len() <= 128
        );
        match &command {
            Command::Rest(input, _, _) | Command::Place(input, _) => {
                self.known_ids.insert(input.order_id);
            }
            Command::Amend(input) => {
                self.known_ids.insert(input.order_id);
            }
            Command::Cancel(id) => {
                self.known_ids.insert(*id);
            }
            Command::AdvanceTime(_) | Command::EndSession(_) => {}
        }
        let ids = self.ids.take(128);
        let (reference, production) = match &command {
            Command::Rest(input, tif, true) => (
                Outcome::Unit(self.reference.rest_timed_with_reservation(
                    input.clone(),
                    *tif,
                    now,
                    9,
                    shadow(input),
                )),
                Outcome::Unit(self.production.rest_timed_with_reservation(
                    input.clone(),
                    *tif,
                    now,
                    9,
                    shadow(input),
                )),
            ),
            Command::Rest(input, tif, false) => (
                Outcome::Unit(self.reference.rest_timed(input.clone(), *tif, now, 9)),
                Outcome::Unit(self.production.rest_timed(input.clone(), *tif, now, 9)),
            ),
            Command::Place(input, tif) => (
                Outcome::Events(self.reference.place_limit_timed_with_events(
                    input.clone(),
                    *tif,
                    now,
                    9,
                    &ids,
                )),
                Outcome::Events(self.production.place_limit_timed_with_events(
                    input.clone(),
                    *tif,
                    now,
                    9,
                    &ids,
                )),
            ),
            Command::Amend(input) => (
                Outcome::Events(
                    self.reference
                        .amend_regular_with_events(input, now, 9, &ids),
                ),
                Outcome::Events(
                    self.production
                        .amend_regular_with_events(input, now, 9, &ids),
                ),
            ),
            Command::Cancel(id) => (
                Outcome::Unit(self.reference.cancel(*id).map(|_| ())),
                Outcome::Unit(self.production.cancel(*id).map(|_| ())),
            ),
            Command::AdvanceTime(at) => {
                self.clock.advance_to(*at).unwrap();
                (
                    Outcome::Expired(Ok(self.reference.expire_gtd_through(*at))),
                    Outcome::Expired(Ok(self.production.expire_gtd_through(*at))),
                )
            }
            Command::EndSession(session) => (
                Outcome::Expired(self.reference.end_session(*session, 9)),
                Outcome::Expired(self.production.end_session(*session, 9)),
            ),
        };
        assert_eq!(
            reference, production,
            "step={}, command={command:?}",
            self.step
        );
        if production.failed() {
            assert_eq!(
                self.reference.test_deterministic_state(),
                reference_before,
                "参考拒绝改变状态：step={}, command={command:?}",
                self.step
            );
            assert_eq!(
                production_snapshot(&self.production),
                production_before,
                "生产拒绝改变状态：step={}, command={command:?}",
                self.step
            );
        }
        let reference_state = reference_state(&self.reference);
        let state = production_state(&self.production);
        assert_eq!(
            reference_state, state,
            "step={}, command={command:?}",
            self.step
        );
        assert_regular_book_invariants(&state.book, state.next, self.step);
        // 身份序只用于完整映射查询；事件、订单及深度均保留各自的业务原序。
        for id in &self.known_ids {
            let expected = self.production.reservation_state(*id);
            assert_eq!(self.reference.reservation_state(*id), expected);
            match (self.production.order(*id), expected) {
                (Some(node), Some(reservation)) => {
                    assert_eq!(reservation, &shadow(node.original_order()))
                }
                (None, _) => assert!(expected.is_none(), "终结订单残留影子预留：{id:?}"),
                (Some(_), None) => {}
            }
        }
        self.step += 1;
        Record {
            outcome: production,
            state,
        }
    }

    fn rest(&mut self, input: LimitGtcOrder, tif: TimeInForce) {
        assert_eq!(
            self.run(Command::Rest(input, tif, true)).outcome,
            Outcome::Unit(Ok(()))
        );
    }
}

fn events(record: &Record) -> &[EngineEvent] {
    match &record.outcome {
        Outcome::Events(Ok(events)) => events,
        outcome => panic!("预期成功事件，实际为 {outcome:?}"),
    }
}

fn gtd(at: u64) -> TimeInForce {
    TimeInForce::Gtd {
        expire_at: LogicalTime::new(at),
    }
}

fn expected_amended(
    input: &LimitGtcOrder,
    price: i64,
    old: u64,
    new: u64,
    retained: bool,
) -> EngineEvent {
    EngineEvent::OrderAmended {
        order_id: input.order_id,
        old_price: Some(input.price),
        new_price: Some(Price::try_new(price).unwrap()),
        old_quantity: AmendedQuantity::BaseRemaining(Qty::try_new(old).unwrap()),
        new_quantity: AmendedQuantity::BaseRemaining(Qty::try_new(new).unwrap()),
        priority_retained: retained,
    }
}

#[test]
fn amend_regular_differential_reprice_target_aggregate_overflow_is_precommit() {
    for side in [Side::Buy, Side::Sell] {
        let mut harness = Harness::new();
        let input = order(1, side, 90, 2);
        for item in [input.clone(), order(2, side, 100, u64::MAX - 1)] {
            harness.rest(item, gtd(20));
        }
        let mut command = amend(&input);
        command.new_price = Some(Price::try_new(100).unwrap());
        command.new_tif = Some(TimeInForce::Day { session_id: 9 });
        assert_eq!(
            harness.run(Command::Amend(command.into())).outcome,
            Outcome::Events(Err(RejectReason::ArithmeticOverflow))
        );
    }
}

#[test]
fn amend_regular_differential_same_price_aggregate_overflow_is_precommit() {
    let input = order(1, Side::Buy, 100, 1);
    let mut harness = Harness::new();
    for item in [input.clone(), order(2, Side::Buy, 100, u64::MAX - 1)] {
        harness.rest(item, TimeInForce::Gtc);
    }
    let mut command = amend(&input);
    command.new_qty = Some(Qty::try_new(2).unwrap());
    assert_eq!(
        harness.run(Command::Amend(command.into())).outcome,
        Outcome::Events(Err(RejectReason::ArithmeticOverflow))
    );
}

#[test]
fn amend_regular_differential_partial_fill_absolute_targets_and_fifo() {
    for side in [Side::Buy, Side::Sell] {
        let mut harness = Harness::new();
        let input = order(1, side, 100, 10);
        harness.rest(input.clone(), gtd(20));
        harness.rest(order(2, side, 100, 4), TimeInForce::Gtc);
        harness.run(Command::Place(
            order(3, opposite_side(side), 100, 3),
            TimeInForce::Gtc,
        ));
        let original_priority = harness.production.order(input.order_id).unwrap().priority();
        let mut reduce = amend(&input);
        reduce.new_qty = Some(Qty::try_new(8).unwrap());
        let record = harness.run(Command::Amend(reduce.into()));
        assert_eq!(
            events(&record),
            &[expected_amended(&input, 100, 7, 5, true)]
        );
        let entry = harness.production.order(input.order_id).unwrap();
        assert_eq!(
            (
                entry.original_order().qty.get(),
                entry.remaining(),
                entry.priority()
            ),
            (8, 5, original_priority)
        );
        for invalid in [2, 3] {
            let mut command = amend(&input);
            command.new_qty = Some(Qty::try_new(invalid).unwrap());
            assert_eq!(
                harness.run(Command::Amend(command.into())).outcome,
                Outcome::Events(Err(RejectReason::InvalidQty))
            );
        }
        let mut increase = amend(&input);
        increase.new_qty = Some(Qty::try_new(12).unwrap());
        let record = harness.run(Command::Amend(increase.into()));
        assert_eq!(
            events(&record),
            &[expected_amended(&input, 100, 5, 9, false)]
        );
        let ordered = match side {
            Side::Buy => &record.state.book.bids,
            Side::Sell => &record.state.book.asks,
        };
        assert_eq!(
            ordered
                .orders
                .iter()
                .map(|entry| entry.original.order_id)
                .collect::<Vec<_>>(),
            vec![OrderId::new(2), input.order_id]
        );
        assert_eq!(
            harness.production.reservation_state(input.order_id),
            Some(&shadow(&input))
        );
        // 未给数量时必须沿用最新绝对总量，不能把余量重新当作原始总量。
        let mut price_only = amend(&input);
        price_only.new_price = Some(Price::try_new(95).unwrap());
        let record = harness.run(Command::Amend(price_only.into()));
        assert_eq!(
            events(&record),
            &[expected_amended(&input, 95, 9, 9, false)]
        );
        assert_eq!(
            harness
                .production
                .order(input.order_id)
                .unwrap()
                .original_order()
                .qty
                .get(),
            12
        );
    }
}

#[test]
fn amend_regular_differential_reprice_residual_joins_existing_target_tail() {
    for side in [Side::Buy, Side::Sell] {
        let mut harness = Harness::new();
        let input = order(1, side, 90, 8);
        harness.rest(input.clone(), gtd(20));
        harness.rest(order(2, side, 100, 3), TimeInForce::Gtc);
        harness.rest(order(3, side, 100, 4), TimeInForce::Gtc);
        let mut command = amend(&input);
        command.new_price = Some(Price::try_new(100).unwrap());
        command.new_tif = Some(gtd(30));
        let record = harness.run(Command::Amend(command.into()));
        assert_eq!(
            events(&record),
            &[expected_amended(&input, 100, 8, 8, false)]
        );
        let observed = match side {
            Side::Buy => &record.state.book.bids,
            Side::Sell => &record.state.book.asks,
        };
        assert_eq!(
            observed
                .orders
                .iter()
                .map(|entry| (entry.original.order_id.get(), entry.priority.get()))
                .collect::<Vec<_>>(),
            vec![(2, 1), (3, 2), (1, 3)]
        );
        assert_eq!(
            (observed.depth[0].order_count, observed.depth[0].total_qty),
            (3, 15)
        );
        assert_eq!(
            harness
                .run(Command::AdvanceTime(LogicalTime::new(20)))
                .outcome,
            Outcome::Expired(Ok(vec![]))
        );
        assert_eq!(
            harness
                .run(Command::AdvanceTime(LogicalTime::new(30)))
                .outcome,
            Outcome::Expired(Ok(vec![ExpiredOrder {
                order_id: input.order_id,
                reason: ExpireReason::GtdReached
            }]))
        );
        assert_eq!(harness.production.reservation_state(input.order_id), None);
    }
}

#[test]
fn amend_regular_differential_reprice_events_keep_candidate_then_trade_then_lifecycle_order() {
    for side in [Side::Buy, Side::Sell] {
        for total in [5, 8] {
            let mut harness = Harness::new();
            let (old_price, next_price) = match side {
                Side::Buy => (90, 101),
                Side::Sell => (110, 99),
            };
            let input = order(1, side, old_price, total);
            harness.rest(input.clone(), gtd(20));
            harness.rest(order(9, opposite_side(side), 100, 2), gtd(20));
            harness.rest(
                order(2, opposite_side(side), next_price, 3),
                TimeInForce::Day { session_id: 9 },
            );
            let mut command = amend(&input);
            command.new_price = Some(Price::try_new(next_price).unwrap());
            command.new_tif = Some(gtd(30));
            let record = harness.run(Command::Amend(command.into()));
            let mut expected = vec![expected_amended(&input, next_price, total, total, false)];
            for (offset, maker, price, qty) in [(0, 9, 100, 2), (1, 2, next_price, 3)] {
                expected.push(EngineEvent::Trade(TradeEvent {
                    trade_id: TradeId::new(385 + offset),
                    maker_order_id: OrderId::new(maker),
                    taker_order_id: input.order_id,
                    price: Price::try_new(price).unwrap(),
                    qty: Qty::try_new(qty).unwrap(),
                    maker_side: opposite_side(side),
                }));
            }
            expected.push(if total == 5 {
                EngineEvent::OrderFilled {
                    order_id: input.order_id,
                }
            } else {
                EngineEvent::OrderPartiallyFilled {
                    order_id: input.order_id,
                    remaining: Qty::try_new(3).unwrap(),
                }
            });
            assert_eq!(events(&record), expected);
            assert_eq!(record.state.next, if total == 5 { 3 } else { 4 });
            assert_eq!(
                harness
                    .production
                    .reservation_state(input.order_id)
                    .cloned(),
                if total == 5 {
                    None
                } else {
                    Some(shadow(&input))
                }
            );
            assert_eq!(
                harness
                    .run(Command::AdvanceTime(LogicalTime::new(20)))
                    .outcome,
                Outcome::Expired(Ok(vec![]))
            );
            assert_eq!(
                harness.run(Command::EndSession(9)).outcome,
                Outcome::Expired(Ok(vec![]))
            );
            let expected = if total == 5 {
                vec![]
            } else {
                vec![ExpiredOrder {
                    order_id: input.order_id,
                    reason: ExpireReason::GtdReached,
                }]
            };
            assert_eq!(
                harness
                    .run(Command::AdvanceTime(LogicalTime::new(30)))
                    .outcome,
                Outcome::Expired(Ok(expected))
            );
        }
    }
}

#[test]
fn amend_regular_differential_all_tif_transitions_preserve_fifo_and_move_references() {
    for side in [Side::Buy, Side::Sell] {
        for source in [
            TimeInForce::Gtc,
            gtd(20),
            TimeInForce::Day { session_id: 9 },
        ] {
            for target in [
                TimeInForce::Gtc,
                gtd(30),
                TimeInForce::Day { session_id: 9 },
            ] {
                let mut harness = Harness::new();
                let input = order(1, side, 100, 3);
                harness.rest(input.clone(), source);
                harness.rest(order(2, side, 100, 4), TimeInForce::Gtc);
                let before = production_state(&harness.production);
                let mut command = amend(&input);
                command.new_tif = Some(target);
                let record = harness.run(Command::Amend(command.into()));
                assert_eq!(
                    events(&record),
                    &[expected_amended(&input, 100, 3, 3, true)]
                );
                assert_eq!(record.state.book, before.book);
                assert_eq!(record.state.next, before.next);
                assert_eq!(
                    record.state.timed_shadows[0],
                    (input.order_id, target, Some(shadow(&input)))
                );
                assert_eq!(
                    harness
                        .run(Command::AdvanceTime(LogicalTime::new(20)))
                        .outcome,
                    Outcome::Expired(Ok(vec![]))
                );
                let expected = if matches!(target, TimeInForce::Day { .. }) {
                    vec![ExpiredOrder {
                        order_id: input.order_id,
                        reason: ExpireReason::SessionEnded,
                    }]
                } else {
                    vec![]
                };
                assert_eq!(
                    harness.run(Command::EndSession(9)).outcome,
                    Outcome::Expired(Ok(expected))
                );
                let expected = if matches!(target, TimeInForce::Gtd { .. }) {
                    vec![ExpiredOrder {
                        order_id: input.order_id,
                        reason: ExpireReason::GtdReached,
                    }]
                } else {
                    vec![]
                };
                assert_eq!(
                    harness
                        .run(Command::AdvanceTime(LogicalTime::new(30)))
                        .outcome,
                    Outcome::Expired(Ok(expected))
                );
            }
        }
    }
}

#[test]
fn amend_regular_differential_gtd_to_earlier_time_and_canonical_expiry_order() {
    let mut harness = Harness::new();
    for id in [9, 2, 1] {
        harness.rest(order(id, Side::Buy, 100, 3), gtd(30));
    }
    let mut command = amend(&order(9, Side::Buy, 100, 3));
    command.new_tif = Some(gtd(20));
    harness.run(Command::Amend(command.into()));
    assert_eq!(
        harness
            .run(Command::AdvanceTime(LogicalTime::new(30)))
            .outcome,
        Outcome::Expired(Ok(vec![9, 1, 2]
            .into_iter()
            .map(|id| ExpiredOrder {
                order_id: OrderId::new(id),
                reason: ExpireReason::GtdReached
            })
            .collect()))
    );
}

#[test]
fn amend_regular_differential_rejections_leave_both_full_states_unchanged() {
    let mut harness = Harness::new();
    let input = order(1, Side::Buy, 90, 10);
    harness.rest(input.clone(), gtd(20));
    harness.rest(
        order(2, Side::Buy, 90, 4),
        TimeInForce::Day { session_id: 9 },
    );
    harness.rest(order(3, Side::Sell, 110, 6), gtd(30));
    let mut cases = Vec::new();
    for (tif, reason) in [
        (gtd(1), RejectReason::InvalidExpireTime),
        (gtd(0), RejectReason::InvalidExpireTime),
        (
            TimeInForce::Day { session_id: 8 },
            RejectReason::InvalidSession,
        ),
        (TimeInForce::Ioc, RejectReason::InvalidOrderCombination),
        (TimeInForce::Fok, RejectReason::InvalidOrderCombination),
    ] {
        let mut command = amend(&input);
        command.new_tif = Some(tif);
        command.new_price = Some(Price::try_new(110).unwrap());
        command.new_qty = Some(Qty::try_new(12).unwrap());
        cases.push((command, reason));
    }
    let mut command = amend(&input);
    command.new_quote_budget = Some(matching_domain::QuoteAmount::new(9));
    cases.push((command, RejectReason::InvalidOrderCombination));
    let mut command = amend(&input);
    command.market_id = MarketId::new(8);
    cases.push((command, RejectReason::InvalidReservation));
    let mut command = amend(&input);
    command.user_id = UserId::new(100);
    cases.push((command, RejectReason::InvalidReservation));
    let mut command = amend(&input);
    command.new_qty = Some(Qty::try_new(11).unwrap());
    command.reservation_adjustment = Some(ReservationAdjustmentToken {
        token_id: 1,
        command_id: CommandId::new(1),
        reservation_id: ReservationId::new(1),
        order_id: input.order_id,
        user_id: input.user_id,
        market_id: MarketId::new(7),
        expected_reservation_version: 11,
        target_reserves: Default::default(),
        fee_schedule_version: 1,
        fee_tier_id: 0,
        risk_key_id: 1,
        signature: [0; 64],
    });
    cases.push((command, RejectReason::InvalidReservation));
    for (command, reason) in cases {
        assert_eq!(
            harness.run(Command::Amend(command.into())).outcome,
            Outcome::Events(Err(reason))
        );
    }
    assert_eq!(
        harness.run(Command::EndSession(8)).outcome,
        Outcome::Expired(Err(RejectReason::InvalidSession))
    );
    let unreserved = order(4, Side::Buy, 80, 3);
    harness.run(Command::Rest(unreserved.clone(), TimeInForce::Gtc, false));
    assert_eq!(
        harness
            .run(Command::Amend(amend(&unreserved).into()))
            .outcome,
        Outcome::Events(Err(RejectReason::InvalidReservation))
    );
    harness.run(Command::Cancel(input.order_id));
    assert_eq!(harness.production.reservation_state(input.order_id), None);
    assert_eq!(
        harness.run(Command::Amend(amend(&input).into())).outcome,
        Outcome::Events(Err(RejectReason::OrderNotFound))
    );
}

fn regular_tape() -> Vec<Command> {
    let input = order(1, Side::Buy, 90, 10);
    let mut commands = vec![
        Command::Rest(input.clone(), gtd(20), true),
        Command::Rest(
            order(2, Side::Buy, 90, 3),
            TimeInForce::Day { session_id: 9 },
            true,
        ),
        Command::Rest(order(3, Side::Sell, 100, 4), gtd(20), true),
    ];
    for (qty, price, tif) in [
        (8, 90, gtd(30)),
        (12, 90, gtd(30)),
        (12, 100, TimeInForce::Day { session_id: 9 }),
    ] {
        let mut command = amend(&input);
        command.new_qty = Some(Qty::try_new(qty).unwrap());
        command.new_price = Some(Price::try_new(price).unwrap());
        command.new_tif = Some(tif);
        commands.push(Command::Amend(command.into()));
    }
    commands.extend([
        Command::AdvanceTime(LogicalTime::new(20)),
        Command::EndSession(9),
        Command::Cancel(OrderId::new(1)),
    ]);
    commands
}

#[test]
fn amend_regular_differential_repeated_tape_has_identical_original_order_transcript() {
    let run = || {
        let mut harness = Harness::new();
        regular_tape()
            .into_iter()
            .map(|command| harness.run(command))
            .collect::<Vec<_>>()
    };
    assert_eq!(run(), run());
}

proptest! {
    #[test]
    fn amend_regular_differential_generated_commands_compare_every_step(
        changes in proptest::collection::vec((1u128..=8, 1u64..30, 80i64..121, 0u8..6), 1..40)
    ) {
        let mut harness = Harness::new();
        for id in 1..=8 {
            let side = if id % 2 == 0 { Side::Buy } else { Side::Sell };
            harness.rest(order(id, side, if side == Side::Buy { 90 } else { 110 }, 10), gtd(30));
        }
        for (id, qty, price, kind) in changes {
            let mut command = amend(&order(id, Side::Buy, 90, 10));
            command.new_qty = Some(Qty::try_new(qty).unwrap());
            command.new_price = Some(Price::try_new(price).unwrap());
            command.new_tif = match kind {
                0 => Some(TimeInForce::Gtc), 1 => Some(gtd(20)), 2 => Some(gtd(30)),
                3 => Some(TimeInForce::Day { session_id: 9 }), 4 => Some(gtd(1)), _ => None,
            };
            harness.run(Command::Amend(command.into()));
        }
        harness.run(Command::AdvanceTime(LogicalTime::new(20)));
        harness.run(Command::EndSession(9));
        harness.run(Command::AdvanceTime(LogicalTime::new(30)));
    }
}
