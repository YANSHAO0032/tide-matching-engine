//! 单命令拒绝、重复纯计划和完整状态屏障的独立双模型证据。

use super::*;
use crate::ReferenceOrderBook;
use crate::reference_orderbook::ReferencePlaceContext;
use matching_domain::ReservationAdjustmentToken;
use proptest::prelude::*;

struct Fixture {
    /// 独立参考订单簿前态。
    reference: ReferenceOrderBook,
    /// 待与参考模型比较的生产订单簿前态。
    production: ProductionOrderBook,
    /// 与两份订单簿共同对应的金融预留登记表。
    registry: ReservationRegistry,
}

impl Fixture {
    fn new(side: Side, qty: u64, old_kind: u8) -> Self {
        let math = MarketMath::try_new(0, 0, 0, 1, 1).unwrap();
        let mut reference = ReferenceOrderBook::new();
        let mut production = ProductionOrderBook::new(policy());
        let mut old = input(1, side, qty);
        old.price = Price::try_new(if side == Side::Buy { 90 } else { 110 }).unwrap();
        let timed = TimeInForce::Gtd {
            expire_at: LogicalTime::new(100),
        };
        match old_kind {
            0 => {
                reference
                    .rest_timed_with_reservation(
                        old.clone(),
                        timed,
                        LogicalTime::new(1),
                        7,
                        reserve(1),
                    )
                    .unwrap();
                production
                    .rest_timed_with_reservation(old, timed, LogicalTime::new(1), 7, reserve(1))
                    .unwrap();
            }
            1 => {
                let iceberg = IcebergOrder::try_new(old, Qty::try_new(1).unwrap()).unwrap();
                reference
                    .place_iceberg_post_only_with_reservation(iceberg.clone(), &math, reserve(1))
                    .unwrap();
                production
                    .place_iceberg_post_only_with_reservation(iceberg, &math, reserve(1))
                    .unwrap();
            }
            2 => {
                let stop = stop(1, side, qty);
                reference
                    .place_stop_with_reservation(stop.clone(), LogicalTime::new(1), 7, reserve(1))
                    .unwrap();
                production
                    .place_stop_with_reservation(stop, LogicalTime::new(1), 7, reserve(1))
                    .unwrap();
            }
            _ => unreachable!(),
        }
        for (id, order_side, price) in [(2, side, old_price(side)), (3, opposite(side), 100)] {
            let mut order = input(id, order_side, qty);
            order.price = Price::try_new(price).unwrap();
            reference
                .rest_with_reservation(order.clone(), reserve(id))
                .unwrap();
            production
                .rest_with_reservation(order, reserve(id))
                .unwrap();
        }
        let stop = stop(4, side, qty);
        reference
            .place_stop_with_reservation(stop.clone(), LogicalTime::new(1), 7, reserve(4))
            .unwrap();
        production
            .place_stop_with_reservation(stop, LogicalTime::new(1), 7, reserve(4))
            .unwrap();
        // 留下两个真实空槽，防止只比较活动订单而遗漏空闲栈污染。
        for id in [80, 81] {
            let mut order = input(id, side, 1);
            order.price = Price::try_new(old_price(side)).unwrap();
            reference.rest(order.clone()).unwrap();
            production.rest(order).unwrap();
        }
        for id in [80, 81] {
            reference.cancel(OrderId::new(id)).unwrap();
            production.cancel(OrderId::new(id)).unwrap();
        }
        for kind in [
            matching_domain::PriceReferenceKind::LastTrade,
            matching_domain::PriceReferenceKind::MarkPrice,
        ] {
            let reference_price = matching_domain::PriceReference {
                kind,
                source_seq: 5,
                price: Price::try_new(100).unwrap(),
            };
            reference.collect_triggered_stops(reference_price).unwrap();
            production.collect_triggered_stops(reference_price).unwrap();
        }
        Self {
            reference,
            production,
            registry: ReservationRegistry::new([reserve(1), reserve(2), reserve(3), reserve(4)])
                .unwrap(),
        }
    }

    fn check(
        &self,
        production: bool,
        r: &CancelReplaceOrder,
        e: &ExpectedReplacementReservation,
        math: &MarketMath,
        reason: Option<RejectReason>,
    ) {
        let before = (
            self.reference.clone(),
            production_snapshot(&self.production),
            self.registry.clone(),
        );
        let ids = [TradeId::new(50); 64];
        if production {
            let first = self.production.plan_cancel_replace(
                r,
                e,
                &self.registry,
                &keys(),
                context(math, &ids),
            );
            let second = self.production.plan_cancel_replace(
                r,
                e,
                &self.registry,
                &keys(),
                context(math, &ids),
            );
            assert_eq!(first, second);
            match reason {
                Some(reason) => assert_eq!(first.unwrap_err(), r.rejection(reason)),
                None => {
                    first.unwrap();
                }
            }
        } else {
            let plan = || {
                self.reference.plan_cancel_replace(
                    r,
                    e,
                    &self.registry,
                    &keys(),
                    ReferencePlaceContext {
                        math,
                        now: LogicalTime::new(10),
                        current_session_id: 7,
                        trade_ids: &ids,
                    },
                )
            };
            let first = plan();
            assert_eq!(first, plan());
            match reason {
                Some(reason) => assert_eq!(first.unwrap_err(), r.rejection(reason)),
                None => {
                    first.unwrap();
                }
            }
        }
        assert_eq!(
            (
                self.reference.clone(),
                production_snapshot(&self.production),
                self.registry.clone()
            ),
            before
        );
    }
}

fn opposite(side: Side) -> Side {
    if side == Side::Buy {
        Side::Sell
    } else {
        Side::Buy
    }
}
fn old_price(side: Side) -> i64 {
    if side == Side::Buy { 90 } else { 110 }
}

fn stop(id: u128, side: Side, qty: u64) -> StopOrder {
    StopOrder::Limit(StopLimitOrder {
        order_id: OrderId::new(id),
        user_id: UserId::new(5),
        reservation_id: reserve(id).reservation_id,
        fee_schedule_version: 1,
        side,
        stop_price: Price::try_new(if side == Side::Buy { 120 } else { 80 }).unwrap(),
        price: Price::try_new(100).unwrap(),
        qty: Qty::try_new(qty).unwrap(),
        child_tif: TimeInForce::Gtc,
        trigger_reference: matching_domain::PriceReferenceKind::LastTrade,
        trigger_expiry: TriggerExpiry::Gtd {
            expire_at: LogicalTime::new(100),
        },
    })
}

fn adjustment() -> ReservationAdjustmentToken {
    let e = expected();
    let mut token = ReservationAdjustmentToken {
        token_id: 70,
        command_id: e.command_id,
        reservation_id: e.old_reservation_id,
        order_id: e.old_order_id,
        user_id: e.user_id,
        market_id: e.market_id,
        expected_reservation_version: e.expected_version,
        target_reserves: e.required_reserves.into(),
        fee_schedule_version: 1,
        fee_tier_id: 2,
        risk_key_id: 31,
        signature: [0; 64],
    };
    sign_adjustment(&mut token);
    token
}

fn sign_adjustment(token: &mut ReservationAdjustmentToken) {
    if let Ok(payload) = token.canonical_signing_payload() {
        token.signature = SigningKey::from_bytes(&[7; 32]).sign(&payload).to_bytes();
    }
}

fn plan_case(production: bool, case: u8, side: Side, qty: u64, old_kind: u8, new: bool) {
    let f = Fixture::new(side, qty, old_kind);
    let mut r = request(new, side, qty, TimeInForce::Gtc);
    let mut e = expected();
    let mut math = MarketMath::try_new(0, 0, 0, 1, 1).unwrap();
    let reason = match case {
        0 => {
            r.old_order_id = OrderId::new(99);
            RejectReason::OrderNotFound
        }
        1 => {
            r.user_id = UserId::new(99);
            RejectReason::OrderNotFound
        }
        2 => {
            r.market_id = MarketId::new(99);
            RejectReason::OrderNotFound
        }
        3 => {
            r.replacement.user_id = UserId::new(99);
            RejectReason::InvalidOrderCombination
        }
        4 => {
            r.replacement.market_id = MarketId::new(99);
            RejectReason::InvalidOrderCombination
        }
        5..=7 => {
            r.replacement.order_id = OrderId::new([1, 2, 4][usize::from(case - 5)]);
            RejectReason::DuplicateOrderId
        }
        8 => {
            if let ReplacementOrderKind::Limit { flags, .. } = &mut r.replacement.kind {
                flags.reduce_only = true;
            }
            RejectReason::InvalidOrderCombination
        }
        9 => {
            r.replacement.kind = ReplacementOrderKind::Limit {
                tif: TimeInForce::Ioc,
                flags: OrderFlags {
                    post_only: true,
                    reduce_only: false,
                },
            };
            RejectReason::InvalidOrderCombination
        }
        10 => {
            r.replacement.kind = ReplacementOrderKind::Iceberg {
                peak_size: Qty::try_new(1).unwrap(),
                tif: TimeInForce::Ioc,
                flags: OrderFlags {
                    post_only: false,
                    reduce_only: false,
                },
            };
            RejectReason::InvalidOrderCombination
        }
        11 => {
            r.replacement.kind = ReplacementOrderKind::Iceberg {
                peak_size: r.replacement.qty,
                tif: TimeInForce::Gtc,
                flags: OrderFlags {
                    post_only: false,
                    reduce_only: false,
                },
            };
            RejectReason::InvalidIceberg
        }
        12 => {
            r.replacement.kind = ReplacementOrderKind::Limit {
                tif: TimeInForce::Gtd {
                    expire_at: LogicalTime::new(10),
                },
                flags: OrderFlags {
                    post_only: false,
                    reduce_only: false,
                },
            };
            RejectReason::InvalidExpireTime
        }
        13 => {
            r.replacement.kind = ReplacementOrderKind::Limit {
                tif: TimeInForce::Day { session_id: 8 },
                flags: OrderFlags {
                    post_only: false,
                    reduce_only: false,
                },
            };
            RejectReason::InvalidSession
        }
        14 => {
            math = MarketMath::try_new(0, 0, 0, 3, 1).unwrap();
            RejectReason::InvalidTick
        }
        15 => {
            r.replacement.qty = Qty::try_new(3).unwrap();
            math = MarketMath::try_new(0, 0, 0, 1, 2).unwrap();
            RejectReason::InvalidLot
        }
        16 => {
            math = MarketMath::try_new(0, 0, 36, 1, 1).unwrap();
            r.replacement.price = Price::try_new(10_000).unwrap();
            RejectReason::ArithmeticOverflow
        }
        17 => {
            r.replacement.qty = Qty::try_new(qty + 1).unwrap();
            r.replacement.kind = ReplacementOrderKind::Limit {
                tif: TimeInForce::Fok,
                flags: OrderFlags {
                    post_only: false,
                    reduce_only: false,
                },
            };
            RejectReason::FokInsufficientLiquidity
        }
        18 => {
            if let ReplacementOrderKind::Limit { flags, .. } = &mut r.replacement.kind {
                flags.post_only = true;
            }
            RejectReason::PostOnlyWouldCross
        }
        19 => {
            e.old_order_id = OrderId::new(99);
            RejectReason::InvalidReservation
        }
        20 => {
            e.new_order_id = OrderId::new(99);
            RejectReason::InvalidReservation
        }
        21 => {
            e.user_id = UserId::new(99);
            RejectReason::InvalidReservation
        }
        22 => {
            e.old_reservation_id = ReservationId::new(99);
            RejectReason::InvalidReservation
        }
        23 => {
            e.expected_version = 4;
            RejectReason::InvalidReservation
        }
        24 => {
            e.fee_schedule_version = 9;
            RejectReason::InvalidReservation
        }
        25 => {
            e.fee_tier_id = 9;
            RejectReason::InvalidReservation
        }
        _ => unreachable!(),
    };
    f.check(production, &r, &e, &math, Some(reason));
}

fn token_case(production: bool, case: u8, side: Side, qty: u64, old_kind: u8, new: bool) {
    let f = Fixture::new(side, qty, old_kind);
    let mut r = request(new, side, qty, TimeInForce::Gtc);
    if !new {
        r.reservation_transition = ReservationTransition::ReuseOld {
            adjustment: Some(adjustment()),
        };
    }
    let e = expected();
    let reason = if case == 13 || case == 14 {
        RejectReason::InsufficientReservation
    } else {
        RejectReason::InvalidReservation
    };
    // 两种令牌分别签名真实载荷；身份不匹配用有效签名证明不能只验签而跳过绑定。
    match &mut r.reservation_transition {
        ReservationTransition::ReplaceWithNew { new_reservation: t } => {
            match case {
                0 => {
                    t.signature[0] ^= 1;
                }
                1 => t.command_id = CommandId::new(99),
                2 => t.order_id = OrderId::new(99),
                3 => t.user_id = UserId::new(99),
                4 => t.market_id = MarketId::new(99),
                5 => t.reservation_id = e.old_reservation_id,
                6 => t.fee_schedule_version = 9,
                7 => t.fee_tier_id = 9,
                8 => t.risk_key_id = 99,
                9 => t.reserves.push(t.reserves[0]),
                10 => {
                    t.reserves.push(AssetReserve {
                        asset: AssetId::new(2),
                        atoms: 1,
                    });
                    t.reserves.swap(0, 1);
                }
                11 => t.reserves[0].atoms = 0,
                12 => t.reserves.clear(),
                13 => t.reserves[0].atoms = 499,
                14 => t.reserves[0].asset = AssetId::new(2),
                15 => t.reserves.push(AssetReserve {
                    asset: AssetId::new(2),
                    atoms: 1,
                }),
                _ => unreachable!(),
            }
            if case != 0
                && let Ok(payload) = t.canonical_signing_payload()
            {
                t.signature = SigningKey::from_bytes(&[7; 32]).sign(&payload).to_bytes();
            }
        }
        ReservationTransition::ReuseOld {
            adjustment: Some(t),
        } => {
            match case {
                0 => {
                    t.signature[0] ^= 1;
                }
                1 => t.command_id = CommandId::new(99),
                2 => t.order_id = OrderId::new(9),
                3 => t.user_id = UserId::new(99),
                4 => t.market_id = MarketId::new(99),
                5 => t.reservation_id = ReservationId::new(99),
                6 => t.fee_schedule_version = 9,
                7 => t.fee_tier_id = 9,
                8 => t.risk_key_id = 99,
                9 => t.target_reserves.push(t.target_reserves[0]),
                10 => {
                    t.target_reserves.push(AssetReserve {
                        asset: AssetId::new(2),
                        atoms: 1,
                    });
                    t.target_reserves.swap(0, 1);
                }
                11 => t.target_reserves[0].atoms = 0,
                12 => t.target_reserves.clear(),
                13 => t.target_reserves[0].atoms = 499,
                14 => t.target_reserves[0].asset = AssetId::new(2),
                15 => t.target_reserves.push(AssetReserve {
                    asset: AssetId::new(2),
                    atoms: 1,
                }),
                16 => t.expected_reservation_version = 4,
                _ => unreachable!(),
            }
            if case != 0 {
                sign_adjustment(t);
            }
        }
        _ => unreachable!(),
    }
    f.check(
        production,
        &r,
        &e,
        &MarketMath::try_new(0, 0, 0, 1, 1).unwrap(),
        Some(reason),
    );
}

#[test]
fn cancel_replace_atomicity_all_rejection_classes_are_repeatable_and_pure() {
    for production in [false, true] {
        for old_kind in 0..3 {
            for new in [false, true] {
                for case in 0..26 {
                    plan_case(production, case, Side::Buy, 5, old_kind, new);
                }
                for case in 0..if new { 16 } else { 17 } {
                    token_case(production, case, Side::Sell, 5, old_kind, new);
                }
            }
        }
    }
}

#[test]
fn cancel_replace_atomicity_real_signed_tokens_have_success_controls() {
    let math = MarketMath::try_new(0, 0, 0, 1, 1).unwrap();
    for production in [false, true] {
        for old_kind in 0..3 {
            for new in [false, true] {
                let f = Fixture::new(Side::Buy, 5, old_kind);
                let mut r = request(new, Side::Buy, 5, TimeInForce::Gtc);
                if !new {
                    r.reservation_transition = ReservationTransition::ReuseOld {
                        adjustment: Some(adjustment()),
                    };
                }
                f.check(production, &r, &expected(), &math, None);
            }
        }
    }
}

#[test]
fn cancel_replace_atomicity_registry_history_and_invalid_trusted_assets_are_pure_rejects() {
    let math = MarketMath::try_new(0, 0, 0, 1, 1).unwrap();
    for production in [false, true] {
        for scenario in 0..6 {
            let mut f = Fixture::new(Side::Buy, 5, 0);
            let r = request(true, Side::Buy, 5, TimeInForce::Gtc);
            let mut e = expected();
            match scenario {
                0 | 1 => {
                    let mut history = reserve(9);
                    if scenario == 0 {
                        history.status = ReservationStatus::Released;
                    }
                    f.registry = ReservationRegistry::new_with_pending_r2(
                        [reserve(1), reserve(2), reserve(3), reserve(4), history],
                        if scenario == 1 {
                            vec![ReservationId::new(109)]
                        } else {
                            vec![]
                        },
                    )
                    .unwrap();
                }
                2 => e.required_reserves.clear(),
                3 => e.required_reserves[0].atoms = 0,
                4 => e.required_reserves.push(e.required_reserves[0]),
                5 => {
                    e.required_reserves.push(AssetReserve {
                        asset: AssetId::new(2),
                        atoms: 1,
                    });
                    e.required_reserves.swap(0, 1);
                }
                _ => unreachable!(),
            }
            f.check(
                production,
                &r,
                &e,
                &math,
                Some(RejectReason::InvalidReservation),
            );
        }
    }
}

fn production_apply_drift(case: u8, side: Side, qty: u64, new: bool) {
    let mut f = Fixture::new(side, qty, 0);
    let math = MarketMath::try_new(0, 0, 0, 1, 1).unwrap();
    let r = request(
        new,
        side,
        qty + 1,
        TimeInForce::Gtd {
            expire_at: LogicalTime::new(50),
        },
    );
    let plan = f
        .production
        .plan_cancel_replace(
            &r,
            &expected(),
            &f.registry,
            &keys(),
            context(&math, &[TradeId::new(1)]),
        )
        .unwrap();
    match case {
        0 => {
            f.production.queue_priority_allocator.allocate().unwrap();
        }
        1 => {
            f.production.stop_priority_allocator.allocate().unwrap();
        }
        2 => {
            f.production
                .reservations
                .get_mut(&OrderId::new(2))
                .unwrap()
                .version += 1;
        }
        3 => {
            f.production
                .reference_prices
                .advance(matching_domain::PriceReference {
                    kind: matching_domain::PriceReferenceKind::MarkPrice,
                    source_seq: 6,
                    price: Price::try_new(101).unwrap(),
                })
                .unwrap();
        }
        4 => {
            f.production
                .regular_expiry
                .insert(OrderId::new(2), TimeInForce::Day { session_id: 7 });
        }
        5 => {
            f.production
                .orders
                .insert(OrderId::new(2), f.production.orders[&OrderId::new(1)]);
        }
        6 => {
            let index = f.production.orders[&OrderId::new(2)];
            f.production
                .arena
                .get_mut(index)
                .unwrap()
                .commit_partial_remaining(qty - 1);
        }
        7 => {
            f.production
                .arena
                .insert(OrderNode::new(input(99, side, 1), QueuePriority::new(80)))
                .unwrap();
        }
        8 => {
            f.production.stop_book.remove(OrderId::new(4)).unwrap();
        }
        9 => {
            if side == Side::Buy {
                f.production.bids.clear();
            } else {
                f.production.asks.clear();
            }
        }
        10 => {
            let binding = f
                .registry
                .plan_binding_transitions([&reserve(2)], [])
                .unwrap();
            f.registry.apply_binding_transition(binding).unwrap();
        }
        11 => {
            let mut history = reserve(99);
            history.status = ReservationStatus::Released;
            f.registry =
                ReservationRegistry::new([reserve(1), reserve(2), reserve(3), reserve(4), history])
                    .unwrap();
        }
        _ => unreachable!(),
    }
    let before = (production_snapshot(&f.production), f.registry.clone());
    assert_eq!(
        f.production
            .apply_cancel_replace(&mut f.registry, plan.clone()),
        Err(ReservationApplyError::StateChanged)
    );
    assert_eq!(
        (production_snapshot(&f.production), f.registry.clone()),
        before
    );
    assert_eq!(
        f.production.apply_cancel_replace(&mut f.registry, plan),
        Err(ReservationApplyError::StateChanged)
    );
    assert_eq!((production_snapshot(&f.production), f.registry), before);
}

#[test]
fn cancel_replace_atomicity_production_apply_rejects_every_physical_drift_before_writes() {
    for case in 0..12 {
        for side in [Side::Buy, Side::Sell] {
            for new in [false, true] {
                production_apply_drift(case, side, 5, new);
            }
        }
    }
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(64))]

    #[test]
    fn cancel_replace_atomicity_generated_rejects_preserve_complete_state(case in 0u8..26, buy in any::<bool>(), qty in 2u64..32, old_kind in 0u8..3, new in any::<bool>(), production in any::<bool>()) {
        plan_case(production, case, if buy {Side::Buy} else {Side::Sell}, qty, old_kind, new);
    }

    #[test]
    fn cancel_replace_atomicity_generated_authorization_rejects_are_zero_mutation(case in 0u8..16, buy in any::<bool>(), qty in 2u64..32, old_kind in 0u8..3, new in any::<bool>(), production in any::<bool>()) {
        token_case(production, case, if buy {Side::Buy} else {Side::Sell}, qty, old_kind, new);
    }

    #[test]
    fn cancel_replace_atomicity_generated_success_plans_are_equal_and_pure(buy in any::<bool>(), qty in 2u64..32, old_kind in 0u8..3, new in any::<bool>(), production in any::<bool>(), tif in 0u8..4) {
        let side = if buy {Side::Buy} else {Side::Sell};
        let f = Fixture::new(side, qty, old_kind);
        let tif = match tif {0 => TimeInForce::Gtc, 1 => TimeInForce::Ioc, 2 => TimeInForce::Fok, _ => TimeInForce::Gtd {expire_at: LogicalTime::new(50)}};
        f.check(production, &request(new, side, qty, tif), &expected(), &MarketMath::try_new(0,0,0,1,1).unwrap(), None);
    }

    #[test]
    fn cancel_replace_atomicity_generated_production_apply_drift_is_fail_closed(case in 0u8..12, buy in any::<bool>(), qty in 2u64..32, new in any::<bool>()) {
        production_apply_drift(case, if buy {Side::Buy} else {Side::Sell}, qty, new);
    }

    #[test]
    fn cancel_replace_atomicity_generated_production_priority_only_blocks_actual_rest(buy in any::<bool>(), qty in 2u64..32, old_kind in 0u8..3, new in any::<bool>()) {
        let side=if buy {Side::Buy} else {Side::Sell};
        let mut f=Fixture::new(side,qty,old_kind);
        let next=f.production.next_queue_priority();
        f.production.queue_priority_allocator=crate::QueuePriorityAllocator::try_restore(QueuePriorityPolicy::try_with_synthetic_ceiling(1024,Some(next)).unwrap(),next).unwrap();
        let math=MarketMath::try_new(0,0,0,1,1).unwrap();
        f.check(true,&request(new,side,qty+1,TimeInForce::Gtc),&expected(),&math,Some(RejectReason::PrioritySpaceExhaustion));
        f.check(true,&request(new,side,qty+1,TimeInForce::Ioc),&expected(),&math,None);
    }
}
