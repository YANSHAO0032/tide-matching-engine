//! 撤单替换的独立双模型命令适配、完整归属历史及逐命令原序差分。

use super::amend_full::{State, invariants, production_state, reference_state};
use super::*;
use crate::AmendExecutionContext;
use crate::production_orderbook::ProductionCancelReplaceContext;
use crate::reference_orderbook::ReferencePlaceContext;
use ed25519_dalek::{Signer, SigningKey};
use matching_domain::{
    AmendOrder, AssetId, AssetReserve, CancelReplaceOrder, CancelReplaceRejection, CommandId,
    ExpiredOrder, IcebergOrder, LogicalClock, LogicalTime, MarketId, OrderFlags, PriceReference,
    PriceReferenceKind, ReplacementOrder, ReplacementOrderKind, ReservationAdjustmentToken,
    ReservationId, ReservationToken, ReservationTransition, StopLimitOrder, StopOrder, TimeInForce,
    TriggerExpiry,
};
use matching_risk::{
    ExpectedReplacementReservation, ReservationRegistry, ReservationState, ReservationStatus,
    RiskKeySet,
};

mod properties;
mod scenarios;
mod tapes;

const SESSION: u64 = 9;
const TRADE_CAPACITY: usize = 1024;

fn math() -> MarketMath {
    MarketMath::try_new(0, 0, 0, 1, 1).unwrap()
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

fn input(id: u128, side: Side, price: i64, qty: u64) -> LimitGtcOrder {
    let mut o = order(id, side, price, qty);
    o.user_id = UserId::new(5);
    o
}

fn shadow(id: u128) -> ReservationState {
    ReservationState {
        reservation_id: ReservationId::new(id),
        order_id: OrderId::new(id),
        user_id: UserId::new(5),
        market_id: MarketId::new(7),
        version: 3,
        status: ReservationStatus::Active,
        fee_schedule_version: 5,
        fee_tier_id: 1,
        reserves: reserves(10_000),
    }
}

fn flags(post_only: bool) -> OrderFlags {
    OrderFlags {
        post_only,
        reduce_only: false,
    }
}

fn limit(tif: TimeInForce, post_only: bool) -> ReplacementOrderKind {
    ReplacementOrderKind::Limit {
        tif,
        flags: flags(post_only),
    }
}

fn gtd(at: u64) -> TimeInForce {
    TimeInForce::Gtd {
        expire_at: LogicalTime::new(at),
    }
}

/// 三种授权都从预先确定的命令和可信需求构造；不使用任何模型返回值签名。
fn replacement(
    identity: (u128, u128, u128, u64),
    command_id: u128,
    transition: u8,
    side: Side,
    price: i64,
    qty: u64,
    kind: ReplacementOrderKind,
) -> Command {
    let (old, new, reservation, version) = identity;
    let expected = ExpectedReplacementReservation {
        command_id: CommandId::new(command_id),
        old_reservation_id: ReservationId::new(reservation),
        old_order_id: OrderId::new(old),
        new_order_id: OrderId::new(new),
        user_id: UserId::new(5),
        market_id: MarketId::new(7),
        expected_version: version,
        fee_schedule_version: 5,
        fee_tier_id: 1,
        required_reserves: reserves(9000),
    };
    let reservation_transition = match transition {
        0 => ReservationTransition::ReuseOld { adjustment: None },
        1 => {
            let signing = SigningKey::from_bytes(&[7; 32]);
            let mut token = ReservationAdjustmentToken {
                token_id: command_id,
                command_id: expected.command_id,
                reservation_id: expected.old_reservation_id,
                order_id: expected.old_order_id,
                user_id: expected.user_id,
                market_id: expected.market_id,
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
            ReservationTransition::ReuseOld {
                adjustment: Some(token),
            }
        }
        2 => {
            let signing = SigningKey::from_bytes(&[7; 32]);
            let mut token = ReservationToken {
                token_id: command_id,
                command_id: expected.command_id,
                reservation_id: ReservationId::new(new),
                order_id: expected.new_order_id,
                user_id: expected.user_id,
                market_id: expected.market_id,
                reserves: reserves(12_000).into(),
                fee_schedule_version: 5,
                fee_tier_id: 1,
                risk_key_id: 31,
                signature: [0; 64],
            };
            token.signature = signing
                .sign(&token.canonical_signing_payload().unwrap())
                .to_bytes();
            ReservationTransition::ReplaceWithNew {
                new_reservation: token,
            }
        }
        _ => unreachable!(),
    };
    Command::CancelReplace {
        request: Box::new(CancelReplaceOrder {
            market_id: expected.market_id,
            user_id: expected.user_id,
            old_order_id: expected.old_order_id,
            replacement: ReplacementOrder {
                market_id: expected.market_id,
                user_id: expected.user_id,
                order_id: expected.new_order_id,
                side,
                price: Price::try_new(price).unwrap(),
                qty: Qty::try_new(qty).unwrap(),
                kind,
            },
            reservation_transition,
        }),
        expected: Box::new(expected),
    }
}

/// 所有命令均在回放之前生成，身份、签名及可信预留需求属于输入磁带。
#[derive(Clone, Debug, PartialEq, Eq)]
enum Command {
    Rest(LimitGtcOrder, TimeInForce),
    Iceberg(IcebergOrder),
    Stop(StopOrder),
    Place(LimitGtcOrder),
    CancelReplace {
        request: Box<CancelReplaceOrder>,
        expected: Box<ExpectedReplacementReservation>,
    },
    Amend(Box<AmendOrder>),
    Cancel(OrderId),
    AdvanceTime(LogicalTime),
    EndSession(u64),
    Reference(PriceReference),
}

#[derive(Debug, PartialEq, Eq)]
enum Outcome {
    Unit(Result<(), RejectReason>),
    Events(Result<Vec<EngineEvent>, RejectReason>),
    CancelReplace(Result<Vec<EngineEvent>, CancelReplaceRejection>),
    Expired(Result<Vec<ExpiredOrder>, RejectReason>),
}

impl Outcome {
    fn failed(&self) -> bool {
        match self {
            Self::Unit(r) => r.is_err(),
            Self::Events(r) => r.is_err(),
            Self::CancelReplace(r) => r.is_err(),
            Self::Expired(r) => r.is_err(),
        }
    }
    fn events(&self) -> &[EngineEvent] {
        match self {
            Self::Events(Ok(events)) | Self::CancelReplace(Ok(events)) => events,
            _ => &[],
        }
    }
}

#[derive(Debug, PartialEq, Eq)]
struct Record {
    /// 单条命令在两个独立模型中的可观察执行结果。
    outcome: Outcome,
    /// 单条命令后的完整订单簿与事件状态。
    state: State,
    /// 完整金融快照与私有归属的结构相等包含所有 PendingR2 历史。
    registry: ReservationRegistry,
}

/// 旧入口没有统一登记表接管接口；测试驱动仅登记独立输入中的新预留。
/// 已登记身份和全部历史不可覆盖；CancelReplace 永远不经过此适配。
fn admit(
    registry: &mut ReservationRegistry,
    known: &mut BTreeSet<ReservationId>,
    state: ReservationState,
) {
    assert!(!known.contains(&state.reservation_id));
    let states = known
        .iter()
        .map(|id| registry.get(*id).unwrap().clone())
        .chain(std::iter::once(state.clone()))
        .collect::<Vec<_>>();
    let pending = known
        .iter()
        .copied()
        .filter(|id| registry.is_pending_r2(*id))
        .collect::<Vec<_>>();
    *registry = ReservationRegistry::new_with_pending_r2(states, pending).unwrap();
    assert!(known.insert(state.reservation_id));
}

/// 旧取消、到期及普通下单仍使用真实批量归属计划，绝不删除金融历史或伪造释放。
fn terminalize(
    registry: &mut ReservationRegistry,
    before: &[(OrderId, ReservationState)],
    after: &[(OrderId, ReservationState)],
    incoming: Option<&ReservationState>,
) {
    let terminal = before
        .iter()
        .map(|(_, s)| s)
        .chain(incoming)
        .filter(|s| {
            !after
                .iter()
                .any(|(_, active)| active.reservation_id == s.reservation_id)
        })
        .collect::<Vec<_>>();
    if !terminal.is_empty() {
        let plan = registry.plan_binding_transitions(terminal, []).unwrap();
        registry.apply_binding_transition(plan).unwrap();
    }
    registry
        .validate_order_bindings(after.iter().map(|(id, s)| (*id, s)))
        .unwrap();
}

struct Harness {
    /// 独立参考订单簿。
    r: ReferenceOrderBook,
    /// 待差分比较的生产订单簿。
    p: ProductionOrderBook,
    /// 参考订单簿使用的金融预留登记表。
    rr: ReservationRegistry,
    /// 生产订单簿使用的金融预留登记表。
    pr: ReservationRegistry,
    /// 已登记到参考侧的全部预留身份。
    known_r: BTreeSet<ReservationId>,
    /// 已登记到生产侧的全部预留身份。
    known_p: BTreeSet<ReservationId>,
    /// 每条命令显式推进的逻辑时钟。
    clock: LogicalClock,
    /// 签发测试资金授权的确定性验签公钥集合。
    keys: RiskKeySet,
    /// 为每条成交预先分配的单调成交身份来源。
    trades: TradeIdSource,
    /// 当前磁带中的零基命令序号。
    step: usize,
}

impl Harness {
    fn new() -> Self {
        Self {
            r: ReferenceOrderBook::new(),
            p: ProductionOrderBook::new(policy()),
            rr: ReservationRegistry::new([]).unwrap(),
            pr: ReservationRegistry::new([]).unwrap(),
            known_r: BTreeSet::new(),
            known_p: BTreeSet::new(),
            clock: LogicalClock::new(LogicalTime::new(1)),
            keys: RiskKeySet::new(2, [(31, SigningKey::from_bytes(&[7; 32]).verifying_key())])
                .unwrap(),
            trades: TradeIdSource::new(),
            step: 0,
        }
    }

    fn observe(&self) -> State {
        let r = reference_state(&self.r, self.clock.now());
        let p = production_state(&self.p, self.clock.now());
        assert_eq!(r, p, "step={} 全业务投影", self.step);
        assert_eq!(self.rr, self.pr, "step={} 完整金融及归属历史", self.step);
        assert_eq!(self.known_r, self.known_p);
        let shadows = self.r.test_reservations();
        self.rr
            .validate_order_bindings(shadows.iter().map(|(id, s)| (*id, s)))
            .unwrap();
        self.pr
            .validate_order_bindings(self.p.reservations.iter().map(|(id, s)| (*id, s)))
            .unwrap();
        invariants(&p, self.step);
        p
    }

    fn run(&mut self, command: &Command) -> Record {
        self.observe();
        let before_r = self.r.clone();
        let before_p = super::super::amend_stop::production_snapshot(&self.p);
        let before_rr = self.rr.clone();
        let before_pr = self.pr.clone();
        let rshadows = self.r.test_reservations();
        let pshadows = self
            .p
            .reservations
            .iter()
            .map(|(id, s)| (*id, s.clone()))
            .collect::<Vec<_>>();
        let now = self.clock.now();
        let math = math();
        let regular = [Side::Buy, Side::Sell]
            .into_iter()
            .flat_map(|side| self.r.orders(side))
            .collect::<Vec<_>>();
        assert!(
            regular.len()
                + self.r.stop_orders(Side::Buy).len()
                + self.r.stop_orders(Side::Sell).len()
                <= 24
        );
        assert!(regular.iter().all(|o| o.total_remaining() <= 30));
        // 单笔成交至少一原子；活动挂单总量不超过 720，因此预分配容量覆盖完整补量序列。
        let ids = self.trades.take(TRADE_CAPACITY);
        let incoming = match command {
            Command::Rest(o, _) | Command::Place(o) => Some(shadow(o.order_id.get())),
            Command::Iceberg(o) => Some(shadow(o.order().order_id.get())),
            Command::Stop(o) => Some(shadow(o.order_id().get())),
            _ => None,
        };
        let (expected, actual) = match command {
            Command::Rest(o, tif) => (
                Outcome::Unit(self.r.rest_timed_with_reservation(
                    o.clone(),
                    *tif,
                    now,
                    SESSION,
                    incoming.clone().unwrap(),
                )),
                Outcome::Unit(self.p.rest_timed_with_reservation(
                    o.clone(),
                    *tif,
                    now,
                    SESSION,
                    incoming.clone().unwrap(),
                )),
            ),
            Command::Iceberg(o) => (
                Outcome::Unit(self.r.place_iceberg_post_only_with_reservation(
                    o.clone(),
                    &math,
                    incoming.clone().unwrap(),
                )),
                Outcome::Unit(self.p.place_iceberg_post_only_with_reservation(
                    o.clone(),
                    &math,
                    incoming.clone().unwrap(),
                )),
            ),
            Command::Stop(o) => (
                Outcome::Unit(
                    self.r
                        .place_stop_with_reservation(
                            o.clone(),
                            now,
                            SESSION,
                            incoming.clone().unwrap(),
                        )
                        .map(|_| ()),
                ),
                Outcome::Unit(
                    self.p
                        .place_stop_with_reservation(
                            o.clone(),
                            now,
                            SESSION,
                            incoming.clone().unwrap(),
                        )
                        .map(|_| ()),
                ),
            ),
            Command::Place(o) => (
                Outcome::Events(self.r.place_limit_gtc_with_reservation(
                    o.clone(),
                    incoming.clone().unwrap(),
                    &ids,
                )),
                Outcome::Events(self.p.place_limit_gtc_with_reservation(
                    o.clone(),
                    incoming.clone().unwrap(),
                    &ids,
                )),
            ),
            Command::CancelReplace { request, expected } => {
                let rplan = self.r.plan_cancel_replace(
                    request,
                    expected,
                    &self.rr,
                    &self.keys,
                    ReferencePlaceContext {
                        math: &math,
                        now,
                        current_session_id: SESSION,
                        trade_ids: &ids,
                    },
                );
                let pplan = self.p.plan_cancel_replace(
                    request,
                    expected,
                    &self.pr,
                    &self.keys,
                    ProductionCancelReplaceContext {
                        math: &math,
                        now,
                        current_session_id: SESSION,
                        trade_ids: &ids,
                    },
                );
                assert_eq!(self.r, before_r);
                assert_eq!(
                    super::super::amend_stop::production_snapshot(&self.p),
                    before_p
                );
                assert_eq!(self.rr, before_rr);
                assert_eq!(self.pr, before_pr);
                let r = rplan.map(|plan| self.r.apply_cancel_replace(&mut self.rr, plan).unwrap());
                let p = pplan.map(|plan| self.p.apply_cancel_replace(&mut self.pr, plan).unwrap());
                if r.is_ok() {
                    self.known_r.insert(
                        self.r
                            .reservation_state(request.replacement.order_id)
                            .map_or_else(
                                || match &request.reservation_transition {
                                    ReservationTransition::ReuseOld { .. } => {
                                        expected.old_reservation_id
                                    }
                                    ReservationTransition::ReplaceWithNew { new_reservation } => {
                                        new_reservation.reservation_id
                                    }
                                },
                                |s| s.reservation_id,
                            ),
                    );
                }
                if p.is_ok() {
                    self.known_p.insert(match &request.reservation_transition {
                        ReservationTransition::ReuseOld { .. } => expected.old_reservation_id,
                        ReservationTransition::ReplaceWithNew { new_reservation } => {
                            new_reservation.reservation_id
                        }
                    });
                }
                (Outcome::CancelReplace(r), Outcome::CancelReplace(p))
            }
            Command::Amend(patch) => {
                let context = AmendExecutionContext {
                    market_math: math,
                    command_id: CommandId::new(88),
                    fee_schedule_version: 5,
                    fee_tier_id: 1,
                    risk_keys: &self.keys,
                    required_reserves: reserves(9000),
                };
                (
                    Outcome::Events(
                        self.r
                            .amend_with_context(patch, now, SESSION, &ids, &context),
                    ),
                    Outcome::Events(
                        self.p
                            .amend_with_context(patch, now, SESSION, &ids, &context),
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
            Command::Reference(source) => (
                Outcome::Unit(self.r.collect_triggered_stops(*source).map(|removed| {
                    assert!(removed.is_empty());
                })),
                Outcome::Unit(self.p.collect_triggered_stops(*source).map(|removed| {
                    assert!(removed.is_empty());
                })),
            ),
        };
        assert_eq!(expected, actual, "step={} command={command:?}", self.step);
        if actual.failed() {
            assert_eq!(self.r, before_r, "参考拒绝改变状态：{command:?}");
            assert_eq!(
                super::super::amend_stop::production_snapshot(&self.p),
                before_p,
                "生产拒绝改变状态：{command:?}"
            );
            assert_eq!(self.rr, before_rr);
            assert_eq!(self.pr, before_pr);
        } else if !matches!(command, Command::CancelReplace { .. }) {
            if let Some(state) = incoming.clone() {
                admit(&mut self.rr, &mut self.known_r, state.clone());
                admit(&mut self.pr, &mut self.known_p, state);
            }
            let after_r = self.r.test_reservations();
            let after_p = self
                .p
                .reservations
                .iter()
                .map(|(id, s)| (*id, s.clone()))
                .collect::<Vec<_>>();
            terminalize(&mut self.rr, &rshadows, &after_r, incoming.as_ref());
            terminalize(&mut self.pr, &pshadows, &after_p, incoming.as_ref());
        }
        let state = self.observe();
        self.step += 1;
        Record {
            outcome: actual,
            state,
            registry: self.pr.clone(),
        }
    }
}
