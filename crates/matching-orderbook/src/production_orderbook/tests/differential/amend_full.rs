//! 完整改单命令域的独立双模型回放、原序事件及逐命令全业务投影。

use super::*;
use crate::AmendExecutionContext;
use ed25519_dalek::{Signer, SigningKey};
use matching_domain::{
    AmendOrder, AmendedQuantity, AssetId, AssetReserve, CommandId, ExpiredOrder, IcebergOrder,
    LogicalClock, LogicalTime, MarketId, MarketProtectionPatch, PriceReference, PriceReferenceKind,
    PriceReferenceState, QuoteAmount, ReservationAdjustmentToken, ReservationId, StopLimitOrder,
    StopMarketOrder, StopMarketQuantity, StopOrder, StopPriority, TimeInForce, TriggerExpiry,
    TriggerExpiryPatch,
};
use matching_risk::{ReservationState, RiskKeySet};

mod scenarios;
mod tapes;

const SESSION: u64 = 9;
const MAX_IDS: u128 = 24;
const TRADE_CAPACITY: usize = 1024;

fn math() -> MarketMath {
    MarketMath::try_new(0, 0, 0, 1, 1).unwrap()
}

fn shadow(input: &LimitOrder) -> ReservationState {
    ReservationState {
        reservation_id: ReservationId::new(input.order_id.get()),
        order_id: input.order_id,
        user_id: input.user_id,
        market_id: MarketId::new(7),
        origin_session_id: matching_domain::OriginSessionId::new(1),
        version: 11,
        status: matching_risk::ReservationStatus::Active,
        fee_schedule_version: 5,
        fee_tier_id: 1,
        reserves: reserves(10_000),
    }
}

fn reserves(atoms: u128) -> Vec<AssetReserve> {
    vec![
        AssetReserve {
            asset: AssetId::new(3),
            atoms,
        },
        AssetReserve {
            asset: AssetId::new(8),
            atoms: 200,
        },
    ]
}

fn patch(id: u128) -> AmendOrder {
    AmendOrder {
        market_id: MarketId::new(7),
        user_id: UserId::new(id as u64),
        order_id: OrderId::new(id),
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

fn signed(patch: &mut AmendOrder, command_id: u128, version: u64) {
    let signing = SigningKey::from_bytes(&[7; 32]);
    let mut token = ReservationAdjustmentToken {
        token_id: command_id,
        command_id: CommandId::new(command_id),
        reservation_id: ReservationId::new(patch.order_id.get()),
        order_id: patch.order_id,
        user_id: patch.user_id,
        market_id: patch.market_id,
        expected_reservation_version: version,
        target_reserves: reserves(12_000).into(),
        fee_schedule_version: 5,
        fee_tier_id: 1,
        risk_key_id: 31,
        signature: [0; 64],
    };
    token.signature = signing
        .sign(&token.canonical_signing_payload().unwrap())
        .to_bytes();
    patch.reservation_adjustment = Some(token);
}

/// 当前受控市场的本地触发核验：分别使用各模型的命令开始预留与冻结费率。
fn trigger_r1(
    order: &StopOrder,
    states: &[(OrderId, ReservationState)],
) -> Result<(), RejectReason> {
    let (user, fee) = match order {
        StopOrder::Limit(o) => (o.user_id, o.fee_schedule_version),
        StopOrder::Market(o) => (o.user_id, o.fee_schedule_version),
    };
    let state = states
        .iter()
        .find(|(id, _)| *id == order.order_id())
        .map(|(_, state)| state)
        .ok_or(RejectReason::InvalidReservation)?;
    if fee != 5
        || state.reservation_id != order.reservation_id()
        || state.user_id != user
        || state.market_id != MarketId::new(7)
    {
        return Err(RejectReason::InvalidReservation);
    }
    if reserves(9000).iter().any(|required| {
        !state
            .reserves
            .iter()
            .any(|r| r.asset == required.asset && r.atoms >= required.atoms)
    }) {
        return Err(RejectReason::InsufficientReservation);
    }
    Ok(())
}

fn gtd(at: u64) -> TimeInForce {
    TimeInForce::Gtd {
        expire_at: LogicalTime::new(at),
    }
}

fn stop(id: u128, kind: u8, side: Side, expiry: TriggerExpiry) -> StopOrder {
    let ordinary = order(id, side, if side == Side::Buy { 90 } else { 110 }, 12);
    let reference = PriceReferenceKind::LastTrade;
    let trigger = Price::try_new(if side == Side::Buy { 105 } else { 95 }).unwrap();
    if kind == 2 {
        StopOrder::Limit(StopLimitOrder {
            order_id: ordinary.order_id,
            user_id: ordinary.user_id,
            reservation_id: ReservationId::new(id),
            fee_schedule_version: 5,
            trigger_reference: reference,
            side,
            stop_price: trigger,
            price: ordinary.price,
            qty: ordinary.qty,
            child_tif: TimeInForce::Gtc,
            trigger_expiry: expiry,
        })
    } else {
        StopOrder::Market(StopMarketOrder {
            order_id: ordinary.order_id,
            user_id: ordinary.user_id,
            reservation_id: ReservationId::new(id),
            fee_schedule_version: 5,
            trigger_reference: reference,
            side,
            stop_price: trigger,
            quantity: if kind == 3 {
                StopMarketQuantity::Quote(QuoteBudget::try_new(1200).unwrap())
            } else {
                StopMarketQuantity::Base(ordinary.qty)
            },
            child_tif: TimeInForce::Ioc,
            trigger_expiry: expiry,
            market_protection: None,
        })
    }
}

/// 所有命令和预留需求均在回放前构造，不从任一模型的执行结果生成另一方输入。
#[derive(Clone, Debug, PartialEq, Eq)]
enum Command {
    Rest(LimitOrder, TimeInForce),
    Iceberg(IcebergOrder),
    Stop(StopOrder),
    ReservedPlace(LimitOrder),
    Place(LimitOrder, TimeInForce),
    Amend {
        patch: Box<AmendOrder>,
        command_id: u128,
        required: u128,
    },
    Cancel(OrderId),
    AdvanceTime(LogicalTime),
    EndSession(u64),
    Reference(PriceReference),
}

fn amend(patch: AmendOrder) -> Command {
    Command::Amend {
        patch: patch.into(),
        command_id: 80,
        required: 9000,
    }
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
    fn events(&self) -> &[EngineEvent] {
        match self {
            Self::Events(Ok(events)) => events,
            _ => &[],
        }
    }
}

#[derive(Debug, PartialEq, Eq)]
struct Regular {
    /// 订单创建时的完整普通限价载荷。
    original: LimitOrder,
    /// 当前可见剩余基础数量。
    visible: u64,
    /// 当前隐藏剩余基础数量。
    hidden: u64,
    /// 当前全部剩余基础数量。
    total: u64,
    /// 冰山订单的单片峰值；普通订单为空。
    peak: Option<Qty>,
    /// 当前订单有效期。
    tif: TimeInForce,
    /// 当前队列优先级。
    priority: QueuePriority,
}

#[derive(Debug, PartialEq, Eq)]
pub(super) struct State {
    /// 双模型共享的可观察订单簿、深度与事件状态。
    book: ObservedBook,
    /// 普通订单完整运行态的有序投影。
    regular: Vec<Regular>,
    /// 买卖两侧未触发止损订单及其优先级。
    stops: [Vec<(StopOrder, StopPriority)>; 2],
    /// 身份排序只用于查询完整映射；业务队列、成交和事件始终保留原序。
    reservations: Vec<(OrderId, ReservationState)>,
    /// 参考模型从自身载荷派生，生产读取实际二级到期桶。
    expiry: Vec<(OrderId, TimeInForce)>,
    /// 未触发止损订单的到期、触发来源和价格投影。
    stop_expiry: Vec<(OrderId, TriggerExpiry, PriceReferenceKind, Side, Price)>,
    /// 下一普通队列优先级计数。
    next: u128,
    /// 下一止损优先级计数。
    stop_next: u128,
    /// 当前各类参考价及其序列状态。
    sources: PriceReferenceState,
    /// 已执行命令后的逻辑时间。
    now: LogicalTime,
    /// 当前交易时段身份。
    session: u64,
}

impl State {
    fn regular(&self, id: OrderId) -> Option<&Regular> {
        self.regular.iter().find(|o| o.original.order_id == id)
    }
    fn stop(&self, id: OrderId) -> Option<&(StopOrder, StopPriority)> {
        self.stops
            .iter()
            .flatten()
            .find(|(o, _)| o.order_id() == id)
    }
    fn reservation(&self, id: OrderId) -> Option<&ReservationState> {
        self.reservations
            .iter()
            .find(|(key, _)| *key == id)
            .map(|(_, state)| state)
    }
}

pub(super) fn reference_state(book: &ReferenceOrderBook, now: LogicalTime) -> State {
    let regular = [Side::Buy, Side::Sell]
        .into_iter()
        .flat_map(|side| book.orders(side))
        .map(|o| Regular {
            original: o.original_order().clone(),
            visible: o.visible_remaining(),
            hidden: o.hidden_remaining(),
            total: o.total_remaining(),
            peak: o.peak_size(),
            tif: o.time_in_force(),
            priority: o.priority(),
        })
        .collect::<Vec<_>>();
    let mut expiry = regular
        .iter()
        .filter(|o| o.tif != TimeInForce::Gtc)
        .map(|o| (o.original.order_id, o.tif))
        .collect::<Vec<_>>();
    expiry.sort_unstable_by_key(|(id, _)| *id);
    let mut stop_expiry = [Side::Buy, Side::Sell]
        .into_iter()
        .flat_map(|side| book.stop_orders(side))
        .filter(|(o, _)| o.trigger_expiry() != TriggerExpiry::Gtc)
        .map(|(o, _)| {
            (
                o.order_id(),
                o.trigger_expiry(),
                o.trigger_reference(),
                o.side(),
                o.stop_price(),
            )
        })
        .collect::<Vec<_>>();
    stop_expiry.sort_unstable_by_key(|(id, ..)| *id);
    State {
        book: observe_reference(book),
        regular,
        stops: [Side::Buy, Side::Sell].map(|side| {
            book.stop_orders(side)
                .into_iter()
                .map(|(o, priority)| (o.clone(), priority))
                .collect()
        }),
        reservations: book.test_reservations(),
        expiry,
        stop_expiry,
        next: book.test_next_queue_priority(),
        stop_next: book.next_stop_priority(),
        sources: book.test_reference_state().clone(),
        now,
        session: SESSION,
    }
}

pub(super) fn production_state(book: &ProductionOrderBook, now: LogicalTime) -> State {
    assert_eq!(validate_book(book), Ok(()));
    assert_eq!(book.validate_regular_expiry(), Ok(()));
    assert_eq!(book.validate_stop_book(), Ok(()));
    let observed = observe_production(book);
    let regular = observed
        .bids
        .orders
        .iter()
        .chain(&observed.asks.orders)
        .map(|o| {
            let n = book.order(o.original.order_id).unwrap();
            Regular {
                original: n.original_order().clone(),
                visible: n.visible_remaining(),
                hidden: n.hidden_remaining(),
                total: n.total_remaining(),
                peak: n.peak_size(),
                tif: n.time_in_force(),
                priority: n.priority(),
            }
        })
        .collect::<Vec<_>>();
    let expiry = book.regular_expiry.test_entries();
    let mut reservations = book
        .reservations
        .iter()
        .map(|(id, s)| (*id, s.clone()))
        .collect::<Vec<_>>();
    reservations.sort_unstable_by_key(|(id, _)| *id);
    State {
        book: observed,
        regular,
        stops: [Side::Buy, Side::Sell].map(|side| {
            book.stop_orders(side)
                .into_iter()
                .map(|(o, priority)| (o.clone(), priority))
                .collect()
        }),
        reservations,
        expiry,
        stop_expiry: book.stop_book.test_expiry_entries(),
        next: book.next_queue_priority(),
        stop_next: book.next_stop_priority(),
        sources: book.reference_prices.clone(),
        now,
        session: SESSION,
    }
}

pub(super) fn invariants(state: &State, step: usize) {
    assert_regular_book_invariants(&state.book, state.next, step);
    let mut ids = BTreeSet::new();
    for o in &state.regular {
        assert!(ids.insert(o.original.order_id));
        assert_eq!(o.visible.checked_add(o.hidden), Some(o.total));
        assert!(o.visible > 0 && o.total <= o.original.qty.get());
        assert!(o.priority.get() < state.next);
        if let Some(peak) = o.peak {
            assert!(o.visible <= peak.get());
        } else {
            assert_eq!(o.hidden, 0);
        }
    }
    for stops in &state.stops {
        for (o, priority) in stops {
            assert!(ids.insert(o.order_id()));
            assert!(priority.get() < state.stop_next);
        }
        for pair in stops.windows(2) {
            if pair[0].0.stop_price() == pair[1].0.stop_price()
                && pair[0].0.trigger_reference() == pair[1].0.trigger_reference()
            {
                assert!(pair[0].1 < pair[1].1);
            }
        }
    }
    for (id, shadow) in &state.reservations {
        assert!(ids.contains(id));
        assert_eq!(*id, shadow.order_id);
    }
}

#[derive(Debug, PartialEq, Eq)]
struct Record {
    /// 单条命令在两个独立模型中的可观察结果。
    outcome: Outcome,
    /// 单条命令后的完整状态投影。
    state: State,
}

struct Harness {
    /// 独立参考订单簿。
    r: ReferenceOrderBook,
    /// 待差分比较的生产订单簿。
    p: ProductionOrderBook,
    /// 每条时间命令显式推进的逻辑时钟。
    clock: LogicalClock,
    /// 验证签名调整令牌的确定性公钥集合。
    keys: RiskKeySet,
    /// 为成交预先分配的单调身份来源。
    trades: TradeIdSource,
    /// 当前磁带中的零基命令序号。
    step: usize,
}

impl Harness {
    fn new() -> Self {
        let signing = SigningKey::from_bytes(&[7; 32]);
        Self {
            r: ReferenceOrderBook::new(),
            p: ProductionOrderBook::new(policy()),
            clock: LogicalClock::new(LogicalTime::new(1)),
            keys: RiskKeySet::new(2, [(31, signing.verifying_key())]).unwrap(),
            trades: TradeIdSource::new(),
            step: 0,
        }
    }
    fn run(&mut self, command: &Command) -> Record {
        let now = self.clock.now();
        let before = production_state(&self.p, now);
        assert_eq!(reference_state(&self.r, now), before);
        invariants(&before, self.step);
        let ps = super::super::amend_stop::production_snapshot(&self.p);
        let rs = self.r.test_deterministic_state();
        // 活动身份最多 24；每单余量至多 30、峰值至少 1，整条 child 级联至多 720 片。
        assert!(before.regular.len() + before.stops.iter().map(Vec::len).sum::<usize>() <= 24);
        assert!(before.regular.iter().all(|o| o.total <= 30));
        let trades = self.trades.take(TRADE_CAPACITY);
        let math = math();
        let (expected, actual) = match command {
            Command::Rest(o, tif) => (
                Outcome::Unit(self.r.rest_timed_with_reservation(
                    o.clone(),
                    *tif,
                    now,
                    SESSION,
                    shadow(o),
                )),
                Outcome::Unit(self.p.rest_timed_with_reservation(
                    o.clone(),
                    *tif,
                    now,
                    SESSION,
                    shadow(o),
                )),
            ),
            Command::Iceberg(ice) => (
                Outcome::Unit(self.r.place_iceberg_post_only_with_reservation(
                    ice.clone(),
                    &math,
                    shadow(ice.order()),
                )),
                Outcome::Unit(self.p.place_iceberg_post_only_with_reservation(
                    ice.clone(),
                    &math,
                    shadow(ice.order()),
                )),
            ),
            Command::Stop(o) => {
                let ordinary = order(o.order_id().get(), o.side(), 100, 12);
                (
                    Outcome::Unit(
                        self.r
                            .place_stop_with_reservation(o.clone(), now, SESSION, shadow(&ordinary))
                            .map(|_| ()),
                    ),
                    Outcome::Unit(
                        self.p
                            .place_stop_with_reservation(o.clone(), now, SESSION, shadow(&ordinary))
                            .map(|_| ()),
                    ),
                )
            }
            Command::Place(o, tif) => (
                Outcome::Events(self.r.place_limit_timed_with_events(
                    o.clone(),
                    *tif,
                    now,
                    SESSION,
                    &trades,
                )),
                Outcome::Events(self.p.place_limit_timed_with_events(
                    o.clone(),
                    *tif,
                    now,
                    SESSION,
                    &trades,
                )),
            ),
            Command::ReservedPlace(o) => (
                Outcome::Events(self.r.place_limit_gtc_with_reservation(
                    o.clone(),
                    shadow(o),
                    &trades,
                )),
                Outcome::Events(self.p.place_limit_gtc_with_reservation(
                    o.clone(),
                    shadow(o),
                    &trades,
                )),
            ),
            Command::Amend {
                patch,
                command_id,
                required,
            } => {
                // 两资产 fixture 的需求独立于签名目标；9000 覆盖受控域的本金、费用与安全量。
                let context = AmendExecutionContext {
                    market_math: math,
                    command_id: CommandId::new(*command_id),
                    fee_schedule_version: 5,
                    fee_tier_id: 1,
                    risk_keys: &self.keys,
                    required_reserves: reserves(*required),
                };
                (
                    Outcome::Events(
                        self.r
                            .amend_with_context(patch, now, SESSION, &trades, &context),
                    ),
                    Outcome::Events(
                        self.p
                            .amend_with_context(patch, now, SESSION, &trades, &context),
                    ),
                )
            }
            Command::Cancel(id) => {
                let r = if self.r.stop_order(*id).is_some() {
                    self.r
                        .cancel_stop(*id)
                        .map(|_| ())
                        .ok_or(RejectReason::OrderNotFound)
                } else {
                    self.r.cancel(*id).map(|_| ())
                };
                let p = if self.p.stop_order(*id).is_some() {
                    self.p
                        .cancel_stop(*id)
                        .map(|_| ())
                        .ok_or(RejectReason::OrderNotFound)
                } else {
                    self.p.cancel(*id).map(|_| ())
                };
                (Outcome::Unit(r), Outcome::Unit(p))
            }
            Command::AdvanceTime(at) => {
                self.clock.advance_to(*at).unwrap();
                (
                    Outcome::Expired(Ok(self.r.expire_gtd_through(*at))),
                    Outcome::Expired(Ok(self.p.expire_gtd_through(*at))),
                )
            }
            Command::EndSession(session) => (
                Outcome::Expired(self.r.end_session(*session, SESSION)),
                Outcome::Expired(self.p.end_session(*session, SESSION)),
            ),
            Command::Reference(reference) => {
                let rstates = self.r.test_reservations();
                let pstates = self
                    .p
                    .reservations
                    .iter()
                    .map(|(id, state)| (*id, state.clone()))
                    .collect::<Vec<_>>();
                (
                    Outcome::Events(self.r.trigger_stops(
                        *reference,
                        now,
                        SESSION,
                        &math,
                        &trades,
                        |order| trigger_r1(order, &rstates),
                    )),
                    Outcome::Events(self.p.trigger_stops(
                        *reference,
                        now,
                        SESSION,
                        &math,
                        &trades,
                        |order| trigger_r1(order, &pstates),
                    )),
                )
            }
        };
        assert_eq!(expected, actual, "step={} command={command:?}", self.step);
        if actual.failed() {
            assert_eq!(
                self.r.test_deterministic_state(),
                rs,
                "参考拒绝改变完整状态：{command:?}"
            );
            assert_eq!(
                super::super::amend_stop::production_snapshot(&self.p),
                ps,
                "生产拒绝改变完整状态：{command:?}"
            );
        }
        let state = production_state(&self.p, self.clock.now());
        let expected_state = reference_state(&self.r, self.clock.now());
        assert_eq!(
            expected_state.reservations, state.reservations,
            "预留：step={} command={command:?}",
            self.step
        );
        assert_eq!(
            expected_state.sources, state.sources,
            "来源：step={} command={command:?}",
            self.step
        );
        assert_eq!(
            expected_state.regular, state.regular,
            "挂单：step={} command={command:?}",
            self.step
        );
        assert_eq!(
            expected_state, state,
            "step={} command={command:?}",
            self.step
        );
        invariants(&state, self.step);
        success_properties(command, &before, &actual, &state);
        self.step += 1;
        Record {
            outcome: actual,
            state,
        }
    }
}

/// 独立按输入与前态推导成功契约，不以模型返回的 retained 位作为期望答案。
fn success_properties(command: &Command, before: &State, outcome: &Outcome, after: &State) {
    let Command::Amend { patch, .. } = command else {
        return;
    };
    if outcome.failed() {
        return;
    }
    let id = patch.order_id;
    let (old_price, new_price, old_quantity, new_quantity, retained) =
        if let Some(old) = before.regular(id) {
            let price = patch.new_price.unwrap_or(old.original.price);
            let filled = old.original.qty.get().checked_sub(old.total).unwrap();
            let remaining = patch
                .new_qty
                .unwrap_or(old.original.qty)
                .get()
                .checked_sub(filled)
                .unwrap();
            let retained = price == old.original.price
                && remaining <= old.total
                && old
                    .peak
                    .is_none_or(|peak| patch.new_visible_qty.unwrap_or(peak) <= peak);
            let refreshes = outcome
                .events()
                .iter()
                .filter(|event| matches!(event, EngineEvent::IcebergRefreshed { .. }))
                .count() as u128;
            let residual_allocations = u128::from(!retained && after.regular(id).is_some());
            assert_eq!(
                after.next,
                before
                    .next
                    .checked_add(refreshes)
                    .unwrap()
                    .checked_add(residual_allocations)
                    .unwrap()
            );
            assert_eq!(after.stop_next, before.stop_next);
            if let Some(new) = after.regular(id) {
                assert_eq!(new.original.order_id, old.original.order_id);
                assert_eq!(
                    new.original.qty.get(),
                    filled.checked_add(remaining).unwrap()
                );
                if retained {
                    assert_eq!(new.priority, old.priority);
                    assert_eq!(after.next, before.next);
                } else {
                    assert!(new.priority > old.priority);
                    assert!(new.priority.get() >= before.next);
                }
            } else {
                assert!(!retained);
                // 无冰山 maker 的完全成交不需要补量，吃单方也不得分配挂单优先级。
                if before.regular.iter().all(|o| o.peak.is_none()) {
                    assert_eq!(after.next, before.next);
                }
            }
            (
                Some(old.original.price),
                Some(price),
                AmendedQuantity::BaseRemaining(Qty::try_new(old.total).unwrap()),
                AmendedQuantity::BaseRemaining(Qty::try_new(remaining).unwrap()),
                retained,
            )
        } else {
            let (old, priority) = before.stop(id).unwrap();
            let (new, next_priority) = after.stop(id).unwrap();
            let retained = patch.new_stop_price.is_none_or(|p| p == old.stop_price())
                && patch
                    .new_trigger_reference
                    .is_none_or(|r| r == old.trigger_reference());
            assert_eq!(new.order_id(), old.order_id());
            assert_eq!(after.next, before.next);
            if retained {
                assert_eq!(next_priority, priority);
                assert_eq!(after.stop_next, before.stop_next);
            } else {
                assert_eq!(next_priority.get(), before.stop_next);
                assert_eq!(after.stop_next, before.stop_next.checked_add(1).unwrap());
            }
            let quantities = |o: &StopOrder| match o {
                StopOrder::Limit(o) => (Some(o.price), AmendedQuantity::BaseRemaining(o.qty)),
                StopOrder::Market(o) => (
                    None,
                    match o.quantity {
                        StopMarketQuantity::Base(qty) => AmendedQuantity::BaseRemaining(qty),
                        StopMarketQuantity::Quote(budget) => {
                            AmendedQuantity::QuoteBudget(QuoteAmount::new(budget.get()))
                        }
                    },
                ),
            };
            let (op, oq) = quantities(old);
            let (np, nq) = quantities(new);
            assert_eq!(std::mem::discriminant(&oq), std::mem::discriminant(&nq));
            (op, np, oq, nq, retained)
        };
    assert_eq!(
        outcome.events().first(),
        Some(&EngineEvent::OrderAmended {
            order_id: id,
            old_price,
            new_price,
            old_quantity,
            new_quantity,
            priority_retained: retained,
        })
    );
    let mut lifecycle_started = false;
    for event in &outcome.events()[1..] {
        if matches!(event, EngineEvent::Trade(_)) {
            assert!(!lifecycle_started);
        } else {
            lifecycle_started = true;
        }
    }
    if let Some(after_shadow) = after.reservation(id) {
        let old_shadow = before.reservation(id).unwrap();
        if let Some(token) = &patch.reservation_adjustment {
            assert_eq!(
                after_shadow.version,
                old_shadow.version.checked_add(1).unwrap()
            );
            assert_eq!(
                after_shadow.reserves.as_slice(),
                token.target_reserves.as_slice()
            );
        } else {
            assert_eq!(after_shadow, old_shadow);
        }
    }
    for (other_id, state) in &after.reservations {
        if *other_id != id {
            assert_eq!(before.reservation(*other_id), Some(state));
        }
    }
}
