//! 使用真实止损簿验证版本化预留复核发生在 child 撮合前。
use matching_domain::{
    AssetId, AssetReserve, EngineEvent, FeeAssetPolicy, FeeRounding, FeeSchedule, FeeScheduleBook,
    FeeTier, LogicalTime, MarketId, MarketMath, OrderId, OriginSessionId, Price, PriceReference,
    PriceReferenceKind, Qty, RejectReason, ReservationId, Side, StopLimitOrder, StopOrder, StpKey,
    StpMode, TimeInForce, TriggerExpiry, UserId,
};
use matching_orderbook::{
    QueuePriorityPolicy, TriggerRegistryContext, production_orderbook::ProductionOrderBook,
};
use matching_risk::{
    LimitReservePolicy, ReservationRegistry, ReservationState, ReservationStatus,
    check_stop_limit_reservation,
};

#[test]
fn trigger_rejection_releases_actual_reserve_atomically() {
    use matching_domain::ReservationReleaseReason;
    use matching_risk::{SettlementAsset, TradeSettlementContext};
    let math = MarketMath::try_new(0, 0, 0, 1, 1).unwrap();
    let policy = LimitReservePolicy::new((AssetId(1), 0), (AssetId(2), 0), math, 1, 1, 0).unwrap();
    let fees = FeeScheduleBook::new(
        FeeSchedule::new(
            1,
            FeeAssetPolicy::Quote,
            [(
                1,
                FeeTier {
                    maker_bps: 0,
                    taker_bps: 1_000,
                    rounding: FeeRounding::Ceiling,
                },
            )],
        )
        .unwrap(),
    );
    let state = ReservationState {
        reservation_id: ReservationId(3),
        order_id: OrderId(1),
        user_id: UserId(2),
        market_id: MarketId(7),
        origin_session_id: OriginSessionId(1),
        version: 0,
        reserves: vec![AssetReserve {
            asset: AssetId(2),
            atoms: 21,
        }],
        status: ReservationStatus::Active,
        fee_schedule_version: 1,
        fee_tier_id: 1,
    };
    let stop = StopOrder::Limit(StopLimitOrder {
        trigger_reference: PriceReferenceKind::LastTrade,
        order_id: OrderId(1),
        user_id: UserId(2),
        stp_key: StpKey::new(0),
        stp_mode: StpMode::None,
        reservation_id: ReservationId(3),
        fee_schedule_version: 1,
        side: Side::Buy,
        stop_price: Price::try_new(10).unwrap(),
        price: Price::try_new(10).unwrap(),
        qty: Qty::try_new(2).unwrap(),
        child_tif: TimeInForce::Gtc,
        trigger_expiry: TriggerExpiry::Gtc,
    });
    let mut book = ProductionOrderBook::new(QueuePriorityPolicy::try_new(1024).unwrap());
    book.bind_market(MarketId(7)).unwrap();
    book.place_stop_with_reservation(stop, LogicalTime::new(0), 1, state.clone())
        .unwrap();
    let mut registry = ReservationRegistry::new([state]).unwrap();
    let trigger = || TriggerRegistryContext {
        now: LogicalTime::new(0),
        current_session_id: 1,
        math: &math,
        trade_ids: &[],
    };
    let settlement = || TradeSettlementContext {
        market_id: MarketId(7),
        math,
        base: SettlementAsset {
            asset: AssetId(1),
            scale: 0,
        },
        quote: SettlementAsset {
            asset: AssetId(2),
            scale: 0,
        },
        fees: &fees,
    };
    let reference = |seq, price| PriceReference {
        kind: PriceReferenceKind::LastTrade,
        source_seq: seq,
        price: Price::try_new(price).unwrap(),
    };
    let plan = book
        .plan_trigger_stops_with_registry(reference(1, 9), trigger(), &registry, |_| Ok(()))
        .unwrap();
    book.apply_trigger_stops_with_registry(&mut registry, plan)
        .unwrap();
    let before = (book.clone(), registry.clone());
    let make_plan = || {
        book.plan_trigger_stops_with_r2(
            reference(2, 11),
            trigger(),
            settlement(),
            &registry,
            |order| match order {
                StopOrder::Limit(child) => {
                    check_stop_limit_reservation(&policy, &fees, &registry, MarketId(7), child)
                }
                _ => Err(RejectReason::InvalidOrderCombination),
            },
        )
        .unwrap()
    };
    let plan = make_plan();
    let cursor = matching_domain::TradeIdCursor::new(
        matching_domain::ClusterId(1),
        matching_domain::ShardId(1),
        1,
    );
    let (cursor_plan, after_cursor) = book
        .plan_trigger_stops_with_r2_and_cursor(
            reference(2, 11),
            trigger(),
            settlement(),
            &registry,
            &cursor,
            |order| match order {
                StopOrder::Limit(child) => {
                    check_stop_limit_reservation(&policy, &fees, &registry, MarketId(7), child)
                }
                _ => Err(RejectReason::InvalidOrderCombination),
            },
        )
        .unwrap();
    let (checked_plan, checked_cursor) = book
        .plan_trigger_stops_with_r2_and_cursor_checked(
            reference(2, 11),
            trigger(),
            settlement(),
            &registry,
            &cursor,
            |order, current| {
                assert_eq!(current.best_price(Side::Sell), None);
                match order {
                    StopOrder::Limit(child) => {
                        check_stop_limit_reservation(&policy, &fees, &registry, MarketId(7), child)
                    }
                    _ => Err(RejectReason::InvalidOrderCombination),
                }
            },
        )
        .unwrap();
    assert_eq!(checked_plan, plan);
    assert_eq!(checked_cursor, cursor);
    assert_eq!(cursor_plan, plan);
    assert_eq!(after_cursor, cursor);
    assert_eq!(plan, make_plan());
    assert_eq!((book.clone(), registry.clone()), before);
    let mut drift_registry = registry.clone();
    let changed = drift_registry
        .plan_r2(
            &[matching_risk::SettlementDebit {
                reservation_id: ReservationId(3),
                asset: AssetId(2),
                atoms: 1,
            }],
            &[],
        )
        .unwrap();
    drift_registry.apply_r2(changed).unwrap();
    let drift_before = drift_registry.clone();
    let mut drift_book = book.clone();
    assert_eq!(
        drift_book.apply_trigger_stops_with_registry(&mut drift_registry, plan.clone()),
        Err(matching_risk::ReservationApplyError::StateChanged)
    );
    assert_eq!(drift_book, book);
    assert_eq!(drift_registry, drift_before);
    let events = book
        .apply_trigger_stops_with_registry(&mut registry, plan)
        .unwrap();
    assert!(
        matches!(events.as_slice(),[EngineEvent::StopTriggered {..},EngineEvent::TriggeredOrderRejected { reason:RejectReason::InsufficientReservation,..},
        EngineEvent::ReservationReleased { reason:ReservationReleaseReason::TriggerRejected,released:Some(assets),..}] if assets[0].atoms==21)
    );
    assert_eq!(
        registry.get(ReservationId(3)).unwrap().status,
        ReservationStatus::Released
    );
}

#[test]
fn trigger_trade_settles_both_sides_and_failure_keeps_original_state() {
    use matching_domain::{LimitOrder, ReservationReleaseReason, TradeId};
    use matching_risk::{SettlementAsset, TradeSettlementContext};
    for (maker_atoms, succeeds, stp) in [(1, false, false), (3, true, false), (3, true, true)] {
        let math = MarketMath::try_new(0, 0, 0, 1, 1).unwrap();
        let policy =
            LimitReservePolicy::new((AssetId(1), 0), (AssetId(2), 0), math, 1, 1, 0).unwrap();
        let fees = FeeScheduleBook::new(
            FeeSchedule::new(
                1,
                FeeAssetPolicy::Quote,
                [(
                    1,
                    FeeTier {
                        maker_bps: 0,
                        taker_bps: 1_000,
                        rounding: FeeRounding::Ceiling,
                    },
                )],
            )
            .unwrap(),
        );
        let state = |id, user, asset, atoms| ReservationState {
            reservation_id: ReservationId(id),
            order_id: OrderId(id),
            user_id: UserId(user),
            market_id: MarketId(7),
            origin_session_id: OriginSessionId(1),
            version: 0,
            reserves: vec![AssetReserve { asset, atoms }],
            status: ReservationStatus::Active,
            fee_schedule_version: 1,
            fee_tier_id: 1,
        };
        let maker = state(4, 4, AssetId(1), maker_atoms);
        let child = state(1, if stp { 4 } else { 2 }, AssetId(2), 25);
        let mut book = ProductionOrderBook::new(QueuePriorityPolicy::try_new(1024).unwrap());
        book.bind_market(MarketId(7)).unwrap();
        book.rest_with_reservation(
            LimitOrder {
                order_id: OrderId(4),
                user_id: UserId(4),
                stp_key: StpKey::new(0),
                stp_mode: StpMode::None,
                side: Side::Sell,
                price: Price::try_new(10).unwrap(),
                qty: Qty::try_new(2).unwrap(),
            },
            maker.clone(),
        )
        .unwrap();
        let stop = StopOrder::Limit(StopLimitOrder {
            trigger_reference: PriceReferenceKind::LastTrade,
            order_id: OrderId(1),
            user_id: UserId(if stp { 4 } else { 2 }),
            stp_key: StpKey::new(0),
            stp_mode: if stp {
                StpMode::CancelMaker
            } else {
                StpMode::None
            },
            reservation_id: ReservationId(1),
            fee_schedule_version: 1,
            side: Side::Buy,
            stop_price: Price::try_new(10).unwrap(),
            price: Price::try_new(10).unwrap(),
            qty: Qty::try_new(2).unwrap(),
            child_tif: TimeInForce::Gtc,
            trigger_expiry: TriggerExpiry::Gtc,
        });
        book.place_stop_with_reservation(stop, LogicalTime::new(0), 1, child.clone())
            .unwrap();
        let mut registry = ReservationRegistry::new([maker, child]).unwrap();
        let trade_ids = [TradeId(1)];
        let context = || TriggerRegistryContext {
            now: LogicalTime::new(0),
            current_session_id: 1,
            math: &math,
            trade_ids: &trade_ids,
        };
        let settlement = || TradeSettlementContext {
            market_id: MarketId(7),
            math,
            base: SettlementAsset {
                asset: AssetId(1),
                scale: 0,
            },
            quote: SettlementAsset {
                asset: AssetId(2),
                scale: 0,
            },
            fees: &fees,
        };
        let reference = |seq, price| PriceReference {
            kind: PriceReferenceKind::LastTrade,
            source_seq: seq,
            price: Price::try_new(price).unwrap(),
        };
        let baseline = book
            .plan_trigger_stops_with_registry(reference(1, 9), context(), &registry, |_| Ok(()))
            .unwrap();
        book.apply_trigger_stops_with_registry(&mut registry, baseline)
            .unwrap();
        let before = (book.clone(), registry.clone());
        let result = book.plan_trigger_stops_with_r2(
            reference(2, 11),
            context(),
            settlement(),
            &registry,
            |order| match order {
                StopOrder::Limit(child) => {
                    check_stop_limit_reservation(&policy, &fees, &registry, MarketId(7), child)
                }
                _ => Err(RejectReason::InvalidOrderCombination),
            },
        );
        let cursor = matching_domain::TradeIdCursor::with_next_index(
            matching_domain::ClusterId(0),
            matching_domain::ShardId(0),
            0,
            1,
        )
        .unwrap();
        let cursor_result = book.plan_trigger_stops_with_r2_and_cursor(
            reference(2, 11),
            context(),
            settlement(),
            &registry,
            &cursor,
            |order| match order {
                StopOrder::Limit(child) => {
                    check_stop_limit_reservation(&policy, &fees, &registry, MarketId(7), child)
                }
                _ => Err(RejectReason::InvalidOrderCombination),
            },
        );
        if !succeeds {
            assert_eq!(result.unwrap_err(), RejectReason::InsufficientReservation);
            assert_eq!(
                cursor_result.unwrap_err(),
                RejectReason::InsufficientReservation
            );
            assert_eq!((book, registry), before);
            continue;
        }
        let plan = result.unwrap();
        let (cursor_plan, after_cursor) = cursor_result.unwrap();
        assert_eq!(cursor_plan, plan);
        assert_eq!(after_cursor.next_index(), if stp { 1 } else { 2 });
        if !stp {
            let exhausted = matching_domain::TradeIdCursor::with_next_index(
                matching_domain::ClusterId(0),
                matching_domain::ShardId(0),
                0,
                u64::from(u32::MAX) + 1,
            )
            .unwrap();
            let failed = book.plan_trigger_stops_with_r2_and_cursor(
                reference(2, 11),
                context(),
                settlement(),
                &registry,
                &exhausted,
                |order| match order {
                    StopOrder::Limit(child) => {
                        check_stop_limit_reservation(&policy, &fees, &registry, MarketId(7), child)
                    }
                    _ => Err(RejectReason::InvalidOrderCombination),
                },
            );
            assert_eq!(failed.unwrap_err(), RejectReason::ArithmeticOverflow);
            assert_eq!((book.clone(), registry.clone()), before);
        }
        assert_eq!((book.clone(), registry.clone()), before);
        let events = book
            .apply_trigger_stops_with_registry(&mut registry, plan)
            .unwrap();
        if stp {
            assert!(
                !events
                    .iter()
                    .any(|e| matches!(e, EngineEvent::Trade(_) | EngineEvent::SettlementDelta(_)))
            );
            assert!(events.iter().any(|e| matches!(e, EngineEvent::ReservationReleased {
                order_id:OrderId(4), reason:ReservationReleaseReason::SelfTradePrevention, released:Some(assets), ..
            } if assets[0].atoms==3)));
            assert_eq!(
                registry.get(ReservationId(4)).unwrap().status,
                ReservationStatus::Released
            );
            assert!(book.order(OrderId(1)).is_some());
            continue;
        }
        assert_eq!(
            events
                .iter()
                .filter(|e| matches!(e, EngineEvent::Trade(_)))
                .count(),
            1
        );
        assert!(events.iter().any(
            |e| matches!(e,EngineEvent::SettlementDelta(delta) if delta.taker_fee.amount.get()==2)
        ));
        assert!(events.iter().any(|e| matches!(e,EngineEvent::ReservationReleased {order_id:OrderId(4),reason:ReservationReleaseReason::FilledRemainder,released:Some(assets),..} if assets[0].atoms==1)));
        assert!(events.iter().any(|e| matches!(e,EngineEvent::ReservationReleased {order_id:OrderId(1),reason:ReservationReleaseReason::FilledRemainder,released:Some(assets),..} if assets[0].atoms==3)));
        assert_eq!(
            registry.get(ReservationId(1)).unwrap().status,
            ReservationStatus::Released
        );
    }
}

#[test]
fn stop_limit_rechecks_fee_coverage_before_child_can_rest() {
    for (atoms, accepted) in [(21, false), (22, true)] {
        let math = MarketMath::try_new(0, 0, 0, 1, 1).unwrap();
        let policy =
            LimitReservePolicy::new((AssetId(1), 0), (AssetId(2), 0), math, 1, 1, 0).unwrap();
        let fees = FeeScheduleBook::new(
            FeeSchedule::new(
                1,
                FeeAssetPolicy::Quote,
                [(
                    1,
                    FeeTier {
                        maker_bps: 0,
                        taker_bps: 1_000,
                        rounding: FeeRounding::Ceiling,
                    },
                )],
            )
            .unwrap(),
        );
        let state = ReservationState {
            reservation_id: ReservationId(3),
            order_id: OrderId(1),
            user_id: UserId(2),
            market_id: MarketId(7),
            origin_session_id: OriginSessionId(1),
            version: 0,
            reserves: vec![AssetReserve {
                asset: AssetId(2),
                atoms,
            }],
            status: ReservationStatus::Active,
            fee_schedule_version: 1,
            fee_tier_id: 1,
        };
        let stop = StopOrder::Limit(StopLimitOrder {
            trigger_reference: PriceReferenceKind::LastTrade,
            order_id: OrderId(1),
            user_id: UserId(2),
            stp_key: StpKey::new(0),
            stp_mode: StpMode::None,
            reservation_id: ReservationId(3),
            fee_schedule_version: 1,
            side: Side::Buy,
            stop_price: Price::try_new(10).unwrap(),
            price: Price::try_new(10).unwrap(),
            qty: Qty::try_new(2).unwrap(),
            child_tif: TimeInForce::Gtc,
            trigger_expiry: TriggerExpiry::Gtc,
        });
        let mut book = ProductionOrderBook::new(QueuePriorityPolicy::try_new(1024).unwrap());
        book.bind_market(MarketId(7)).unwrap();
        book.place_stop_with_reservation(stop, LogicalTime::new(0), 1, state.clone())
            .unwrap();
        let mut registry = ReservationRegistry::new([state]).unwrap();
        let context = || TriggerRegistryContext {
            now: LogicalTime::new(0),
            current_session_id: 1,
            math: &math,
            trade_ids: &[],
        };
        let reference = |seq, price| PriceReference {
            kind: PriceReferenceKind::LastTrade,
            source_seq: seq,
            price: Price::try_new(price).unwrap(),
        };
        let plan = book
            .plan_trigger_stops_with_registry(reference(1, 9), context(), &registry, |_| Ok(()))
            .unwrap();
        book.apply_trigger_stops_with_registry(&mut registry, plan)
            .unwrap();
        let plan = book
            .plan_trigger_stops_with_registry(reference(2, 11), context(), &registry, |order| {
                match order {
                    StopOrder::Limit(order) => {
                        check_stop_limit_reservation(&policy, &fees, &registry, MarketId(7), order)
                    }
                    _ => Err(RejectReason::InvalidOrderCombination),
                }
            })
            .unwrap();
        let events = book
            .apply_trigger_stops_with_registry(&mut registry, plan)
            .unwrap();
        assert_eq!(book.order(OrderId(1)).is_some(), accepted);
        assert_eq!(
            events.iter().any(|event| matches!(
                event,
                EngineEvent::TriggeredOrderRejected {
                    reason: RejectReason::InsufficientReservation,
                    ..
                }
            )),
            !accepted
        );
        assert!(!events.iter().any(|event| matches!(
            event,
            EngineEvent::Trade(_) | EngineEvent::SettlementDelta(_)
        )));
    }
}

#[test]
fn stop_market_quote_checks_budget_coverage_before_execution() {
    use matching_domain::{
        QuoteBudget, ReservationReleaseReason, StopMarketOrder, StopMarketQuantity,
    };
    use matching_risk::{SettlementAsset, TradeSettlementContext, check_stop_market_reservation};
    for (atoms, accepted) in [(39, false), (40, true)] {
        let math = MarketMath::try_new(0, 0, 0, 1, 1).unwrap();
        let policy =
            LimitReservePolicy::new((AssetId(1), 0), (AssetId(2), 0), math, 1, 1, 0).unwrap();
        let fees = FeeScheduleBook::new(
            FeeSchedule::new(
                1,
                FeeAssetPolicy::Quote,
                [(
                    1,
                    FeeTier {
                        maker_bps: 0,
                        taker_bps: 1_000,
                        rounding: FeeRounding::Ceiling,
                    },
                )],
            )
            .unwrap(),
        );
        let state = ReservationState {
            reservation_id: ReservationId(3),
            order_id: OrderId(1),
            user_id: UserId(2),
            market_id: MarketId(7),
            origin_session_id: OriginSessionId(1),
            version: 0,
            reserves: vec![AssetReserve {
                asset: AssetId(2),
                atoms,
            }],
            status: ReservationStatus::Active,
            fee_schedule_version: 1,
            fee_tier_id: 1,
        };
        let stop = StopOrder::Market(StopMarketOrder {
            trigger_reference: PriceReferenceKind::LastTrade,
            market_protection: None,
            order_id: OrderId(1),
            user_id: UserId(2),
            stp_key: StpKey::new(0),
            stp_mode: StpMode::None,
            reservation_id: ReservationId(3),
            fee_schedule_version: 1,
            side: Side::Buy,
            stop_price: Price::try_new(10).unwrap(),
            quantity: StopMarketQuantity::Quote(QuoteBudget::try_new(20).unwrap()),
            child_tif: TimeInForce::Ioc,
            trigger_expiry: TriggerExpiry::Gtc,
        });
        let mut book = ProductionOrderBook::new(QueuePriorityPolicy::try_new(1024).unwrap());
        book.bind_market(MarketId(7)).unwrap();
        book.place_stop_with_reservation(stop, LogicalTime::new(0), 1, state.clone())
            .unwrap();
        let mut registry = ReservationRegistry::new([state]).unwrap();
        let context = || TriggerRegistryContext {
            now: LogicalTime::new(0),
            current_session_id: 1,
            math: &math,
            trade_ids: &[],
        };
        let reference = |seq, price| PriceReference {
            kind: PriceReferenceKind::LastTrade,
            source_seq: seq,
            price: Price::try_new(price).unwrap(),
        };
        let plan = book
            .plan_trigger_stops_with_registry(reference(1, 9), context(), &registry, |_| Ok(()))
            .unwrap();
        book.apply_trigger_stops_with_registry(&mut registry, plan)
            .unwrap();
        let plan = book
            .plan_trigger_stops_with_r2(
                reference(2, 11),
                context(),
                TradeSettlementContext {
                    market_id: MarketId(7),
                    math,
                    base: SettlementAsset {
                        asset: AssetId(1),
                        scale: 0,
                    },
                    quote: SettlementAsset {
                        asset: AssetId(2),
                        scale: 0,
                    },
                    fees: &fees,
                },
                &registry,
                |order| match order {
                    StopOrder::Market(order) => check_stop_market_reservation(
                        &policy,
                        &fees,
                        &registry,
                        MarketId(7),
                        order,
                        None,
                    ),
                    _ => Err(RejectReason::InvalidOrderCombination),
                },
            )
            .unwrap();
        let events = book
            .apply_trigger_stops_with_registry(&mut registry, plan)
            .unwrap();
        assert_eq!(
            events.iter().any(|event| matches!(
                event,
                EngineEvent::TriggeredOrderRejected {
                    reason: RejectReason::InsufficientReservation,
                    ..
                }
            )),
            !accepted
        );
        assert!(!events.iter().any(|event| matches!(
            event,
            EngineEvent::Trade(_) | EngineEvent::SettlementDelta(_)
        )));
        let reason = if accepted {
            ReservationReleaseReason::IocRemainder
        } else {
            ReservationReleaseReason::TriggerRejected
        };
        assert!(events.iter().any(|event| matches!(event,EngineEvent::ReservationReleased {reason:r,released:Some(assets),..} if *r==reason && assets[0].atoms==atoms)));
        assert_eq!(
            registry.get(ReservationId(3)).unwrap().status,
            ReservationStatus::Released
        );
    }
}

#[test]
fn multiple_empty_market_children_release_in_trigger_priority_order() {
    use matching_domain::{
        QuoteBudget, ReservationReleaseReason, StopMarketOrder, StopMarketQuantity,
    };
    use matching_risk::{SettlementAsset, TradeSettlementContext, check_stop_market_reservation};
    let math = MarketMath::try_new(0, 0, 0, 1, 1).unwrap();
    let policy = LimitReservePolicy::new((AssetId(1), 0), (AssetId(2), 0), math, 1, 1, 0).unwrap();
    let fees = FeeScheduleBook::new(
        FeeSchedule::new(
            1,
            FeeAssetPolicy::Quote,
            [(
                1,
                FeeTier {
                    maker_bps: 0,
                    taker_bps: 0,
                    rounding: FeeRounding::Floor,
                },
            )],
        )
        .unwrap(),
    );
    let mut book = ProductionOrderBook::new(QueuePriorityPolicy::try_new(1024).unwrap());
    book.bind_market(MarketId(7)).unwrap();
    let mut states = Vec::new();
    for id in [2_u128, 1] {
        let state = ReservationState {
            reservation_id: ReservationId(id),
            order_id: OrderId(id),
            user_id: UserId(id as u64),
            market_id: MarketId(7),
            origin_session_id: OriginSessionId(1),
            version: 0,
            reserves: vec![AssetReserve {
                asset: AssetId(2),
                atoms: 20,
            }],
            status: ReservationStatus::Active,
            fee_schedule_version: 1,
            fee_tier_id: 1,
        };
        let stop = StopOrder::Market(StopMarketOrder {
            trigger_reference: PriceReferenceKind::LastTrade,
            market_protection: None,
            order_id: OrderId(id),
            user_id: UserId(id as u64),
            stp_key: StpKey::new(0),
            stp_mode: StpMode::None,
            reservation_id: ReservationId(id),
            fee_schedule_version: 1,
            side: Side::Buy,
            stop_price: Price::try_new(10).unwrap(),
            quantity: StopMarketQuantity::Quote(QuoteBudget::try_new(20).unwrap()),
            child_tif: TimeInForce::Ioc,
            trigger_expiry: TriggerExpiry::Gtc,
        });
        book.place_stop_with_reservation(stop, LogicalTime::new(0), 1, state.clone())
            .unwrap();
        states.push(state);
    }
    let mut registry = ReservationRegistry::new(states).unwrap();
    let context = || TriggerRegistryContext {
        now: LogicalTime::new(0),
        current_session_id: 1,
        math: &math,
        trade_ids: &[],
    };
    let reference = |seq, price| PriceReference {
        kind: PriceReferenceKind::LastTrade,
        source_seq: seq,
        price: Price::try_new(price).unwrap(),
    };
    let baseline = book
        .plan_trigger_stops_with_registry(reference(1, 9), context(), &registry, |_| Ok(()))
        .unwrap();
    book.apply_trigger_stops_with_registry(&mut registry, baseline)
        .unwrap();
    let plan = book
        .plan_trigger_stops_with_r2(
            reference(2, 11),
            context(),
            TradeSettlementContext {
                market_id: MarketId(7),
                math,
                base: SettlementAsset {
                    asset: AssetId(1),
                    scale: 0,
                },
                quote: SettlementAsset {
                    asset: AssetId(2),
                    scale: 0,
                },
                fees: &fees,
            },
            &registry,
            |stop| match stop {
                StopOrder::Market(stop) => check_stop_market_reservation(
                    &policy,
                    &fees,
                    &registry,
                    MarketId(7),
                    stop,
                    None,
                ),
                _ => Err(RejectReason::InvalidOrderCombination),
            },
        )
        .unwrap();
    let events = book
        .apply_trigger_stops_with_registry(&mut registry, plan)
        .unwrap();
    assert!(matches!(
        events.as_slice(),
        [
            EngineEvent::StopTriggered {
                order_id: OrderId(2),
                ..
            },
            EngineEvent::ReservationReleased {
                order_id: OrderId(2),
                reason: ReservationReleaseReason::IocRemainder,
                ..
            },
            EngineEvent::StopTriggered {
                order_id: OrderId(1),
                ..
            },
            EngineEvent::ReservationReleased {
                order_id: OrderId(1),
                reason: ReservationReleaseReason::IocRemainder,
                ..
            }
        ]
    ));
}

#[test]
fn triggered_child_settles_multiple_makers_without_early_release() {
    triggered_multi_maker_case(false, false);
    triggered_multi_maker_case(true, false);
    triggered_multi_maker_case(false, true);
}

fn triggered_multi_maker_case(invalid_second: bool, duplicate_id: bool) {
    use matching_domain::snapshot_wire::{SnapshotDecoder, SnapshotEncoder};
    use matching_domain::{LimitOrder, TradeId};
    use matching_risk::{SettlementAsset, TradeSettlementContext};
    let math = MarketMath::try_new(0, 0, 0, 1, 1).unwrap();
    let policy = LimitReservePolicy::new((AssetId(1), 0), (AssetId(2), 0), math, 1, 1, 0).unwrap();
    let mut fees = FeeScheduleBook::new(
        FeeSchedule::new(
            1,
            FeeAssetPolicy::Quote,
            [(
                1,
                FeeTier {
                    maker_bps: 0,
                    taker_bps: 0,
                    rounding: FeeRounding::Floor,
                },
            )],
        )
        .unwrap(),
    );
    fees.activate(
        FeeSchedule::new(
            2,
            FeeAssetPolicy::Quote,
            [(
                1,
                FeeTier {
                    maker_bps: 1_000,
                    taker_bps: 0,
                    rounding: FeeRounding::Floor,
                },
            )],
        )
        .unwrap(),
    )
    .unwrap();
    let state = |id, asset, atoms| ReservationState {
        reservation_id: ReservationId(id),
        order_id: OrderId(id),
        user_id: UserId(id as u64),
        market_id: MarketId(7),
        origin_session_id: OriginSessionId(1),
        version: 0,
        reserves: vec![AssetReserve { asset, atoms }],
        status: ReservationStatus::Active,
        fee_schedule_version: 1,
        fee_tier_id: 1,
    };
    let mut book = ProductionOrderBook::new(QueuePriorityPolicy::try_new(1024).unwrap());
    book.bind_market(MarketId(7)).unwrap();
    let mut states = Vec::new();
    for id in [4_u128, 5] {
        let mut s = state(
            id,
            AssetId(1),
            if id == 5 && invalid_second { 1 } else { 2 },
        );
        if id == 5 {
            s.fee_schedule_version = 2;
        }
        book.rest_with_reservation(
            LimitOrder {
                order_id: OrderId(id),
                user_id: UserId(id as u64),
                stp_key: StpKey::new(0),
                stp_mode: StpMode::None,
                side: Side::Sell,
                price: Price::try_new(10).unwrap(),
                qty: Qty::try_new(2).unwrap(),
            },
            s.clone(),
        )
        .unwrap();
        states.push(s);
    }
    let stop = StopOrder::Limit(StopLimitOrder {
        trigger_reference: PriceReferenceKind::LastTrade,
        order_id: OrderId(1),
        user_id: UserId(1),
        stp_key: StpKey::new(0),
        stp_mode: StpMode::None,
        reservation_id: ReservationId(1),
        fee_schedule_version: 1,
        side: Side::Buy,
        stop_price: Price::try_new(10).unwrap(),
        price: Price::try_new(10).unwrap(),
        qty: Qty::try_new(3).unwrap(),
        child_tif: TimeInForce::Gtc,
        trigger_expiry: TriggerExpiry::Gtc,
    });
    let child = state(1, AssetId(2), 35);
    book.place_stop_with_reservation(stop, LogicalTime::new(0), 1, child.clone())
        .unwrap();
    states.push(child);
    let mut registry = ReservationRegistry::new(states).unwrap();
    let ids = [TradeId(1), TradeId(if duplicate_id { 1 } else { 2 })];
    let context = || TriggerRegistryContext {
        now: LogicalTime::new(0),
        current_session_id: 1,
        math: &math,
        trade_ids: &ids,
    };
    let reference = |seq, price| PriceReference {
        kind: PriceReferenceKind::LastTrade,
        source_seq: seq,
        price: Price::try_new(price).unwrap(),
    };
    let baseline = book
        .plan_trigger_stops_with_registry(reference(1, 9), context(), &registry, |_| Ok(()))
        .unwrap();
    book.apply_trigger_stops_with_registry(&mut registry, baseline)
        .unwrap();
    let mut snapshot = SnapshotEncoder::new(1_000_000);
    book.write_snapshot(&mut snapshot).unwrap();
    registry.write_snapshot(&mut snapshot).unwrap();
    let bytes = snapshot.finish();
    let mut reader = SnapshotDecoder::new(&bytes, 1_000_000).unwrap();
    let mut restored_book = ProductionOrderBook::read_snapshot(&mut reader).unwrap();
    let mut restored_registry = ReservationRegistry::read_snapshot(&mut reader).unwrap();
    reader.finish().unwrap();
    let before = (book.clone(), registry.clone());
    let result = book.plan_trigger_stops_with_r2(
        reference(2, 11),
        context(),
        TradeSettlementContext {
            market_id: MarketId(7),
            math,
            base: SettlementAsset {
                asset: AssetId(1),
                scale: 0,
            },
            quote: SettlementAsset {
                asset: AssetId(2),
                scale: 0,
            },
            fees: &fees,
        },
        &registry,
        |order| match order {
            StopOrder::Limit(order) => {
                check_stop_limit_reservation(&policy, &fees, &registry, MarketId(7), order)
            }
            _ => Err(RejectReason::InvalidOrderCombination),
        },
    );
    if duplicate_id {
        assert_eq!(result.unwrap_err(), RejectReason::InvalidOrderCombination);
        assert_eq!((book, registry), before);
        return;
    }
    let cursor = matching_domain::TradeIdCursor::with_next_index(
        matching_domain::ClusterId(0),
        matching_domain::ShardId(0),
        0,
        1,
    )
    .unwrap();
    let cursor_result = book.plan_trigger_stops_with_r2_and_cursor(
        reference(2, 11),
        context(),
        TradeSettlementContext {
            market_id: MarketId(7),
            math,
            base: SettlementAsset {
                asset: AssetId(1),
                scale: 0,
            },
            quote: SettlementAsset {
                asset: AssetId(2),
                scale: 0,
            },
            fees: &fees,
        },
        &registry,
        &cursor,
        |order| match order {
            StopOrder::Limit(order) => {
                check_stop_limit_reservation(&policy, &fees, &registry, MarketId(7), order)
            }
            _ => Err(RejectReason::InvalidOrderCombination),
        },
    );
    if invalid_second {
        assert_eq!(result.unwrap_err(), RejectReason::InsufficientReservation);
        assert_eq!(
            cursor_result.unwrap_err(),
            RejectReason::InsufficientReservation
        );
        assert_eq!((book, registry), before);
        return;
    }
    let plan = result.unwrap();
    let (cursor_plan, after_cursor) = cursor_result.unwrap();
    assert_eq!(cursor_plan, plan);
    assert_eq!(after_cursor.next_index(), 3);
    for limits in [(0, 10), (10, 1)] {
        let halted = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            book.plan_trigger_stops_with_r2_and_cursor_limited(
                reference(2, 11),
                context(),
                TradeSettlementContext {
                    market_id: MarketId(7),
                    math,
                    base: SettlementAsset {
                        asset: AssetId(1),
                        scale: 0,
                    },
                    quote: SettlementAsset {
                        asset: AssetId(2),
                        scale: 0,
                    },
                    fees: &fees,
                },
                &registry,
                (&cursor, limits),
                |order, _| match order {
                    StopOrder::Limit(order) => {
                        check_stop_limit_reservation(&policy, &fees, &registry, MarketId(7), order)
                    }
                    _ => Err(RejectReason::InvalidOrderCombination),
                },
            )
        }));
        assert!(halted.is_err());
        assert_eq!((book.clone(), registry.clone()), before);
        assert_eq!(cursor.next_index(), 1);
    }
    let events = book
        .apply_trigger_stops_with_registry(&mut registry, plan)
        .unwrap();
    let restored_plan = restored_book
        .plan_trigger_stops_with_r2(
            reference(2, 11),
            context(),
            TradeSettlementContext {
                market_id: MarketId(7),
                math,
                base: SettlementAsset {
                    asset: AssetId(1),
                    scale: 0,
                },
                quote: SettlementAsset {
                    asset: AssetId(2),
                    scale: 0,
                },
                fees: &fees,
            },
            &restored_registry,
            |order| match order {
                StopOrder::Limit(order) => check_stop_limit_reservation(
                    &policy,
                    &fees,
                    &restored_registry,
                    MarketId(7),
                    order,
                ),
                _ => Err(RejectReason::InvalidOrderCombination),
            },
        )
        .unwrap();
    let restored_events = restored_book
        .apply_trigger_stops_with_registry(&mut restored_registry, restored_plan)
        .unwrap();
    assert_eq!(events, restored_events);
    assert_eq!(book, restored_book);
    assert_eq!(registry, restored_registry);
    assert_eq!(
        events
            .iter()
            .filter_map(|e| match e {
                EngineEvent::Trade(t) => Some(t.maker_order_id),
                _ => None,
            })
            .collect::<Vec<_>>(),
        vec![OrderId(4), OrderId(5)]
    );
    assert_eq!(
        events
            .iter()
            .filter(|e| matches!(e, EngineEvent::SettlementDelta(_)))
            .count(),
        2
    );
    assert!(events.iter().any(|e|matches!(e,EngineEvent::SettlementDelta(delta) if delta.trade_id==TradeId(2)
        && delta.maker_fee.fee_schedule_version==2 && delta.taker_fee.fee_schedule_version==1 && delta.maker_fee.amount.get()==1)));
    let second = events
        .iter()
        .position(|e| matches!(e,EngineEvent::Trade(t) if t.maker_order_id==OrderId(5)))
        .unwrap();
    let release = events
        .iter()
        .position(|e| {
            matches!(
                e,
                EngineEvent::ReservationReleased {
                    order_id: OrderId(1),
                    ..
                }
            )
        })
        .unwrap();
    assert!(release > second);
    assert_eq!(
        book.reservation_state(OrderId(5)),
        registry.get(ReservationId(5))
    );
}

#[test]
fn triggered_stp_cancel_precedes_later_real_settlement() {
    use matching_domain::{LimitOrder, ReservationReleaseReason, TradeId};
    use matching_risk::{SettlementAsset, TradeSettlementContext};
    let math = MarketMath::try_new(0, 0, 0, 1, 1).unwrap();
    let policy = LimitReservePolicy::new((AssetId(1), 0), (AssetId(2), 0), math, 1, 1, 0).unwrap();
    let fees = FeeScheduleBook::new(
        FeeSchedule::new(
            1,
            FeeAssetPolicy::Quote,
            [(
                1,
                FeeTier {
                    maker_bps: 0,
                    taker_bps: 0,
                    rounding: FeeRounding::Floor,
                },
            )],
        )
        .unwrap(),
    );
    let state = |id, user, asset, atoms| ReservationState {
        reservation_id: ReservationId(id),
        order_id: OrderId(id),
        user_id: UserId(user),
        market_id: MarketId(7),
        origin_session_id: OriginSessionId(1),
        version: 0,
        reserves: vec![AssetReserve { asset, atoms }],
        status: ReservationStatus::Active,
        fee_schedule_version: 1,
        fee_tier_id: 1,
    };
    let mut book = ProductionOrderBook::new(QueuePriorityPolicy::try_new(1024).unwrap());
    book.bind_market(MarketId(7)).unwrap();
    let mut states = Vec::new();
    for (id, user) in [(4_u128, 1_u64), (5, 5)] {
        let s = state(id, user, AssetId(1), 2);
        book.rest_with_reservation(
            LimitOrder {
                order_id: OrderId(id),
                user_id: UserId(user),
                stp_key: StpKey::new(0),
                stp_mode: StpMode::None,
                side: Side::Sell,
                price: Price::try_new(10).unwrap(),
                qty: Qty::try_new(2).unwrap(),
            },
            s.clone(),
        )
        .unwrap();
        states.push(s);
    }
    let stop = StopOrder::Limit(StopLimitOrder {
        trigger_reference: PriceReferenceKind::LastTrade,
        order_id: OrderId(1),
        user_id: UserId(1),
        stp_key: StpKey::new(0),
        stp_mode: StpMode::CancelMaker,
        reservation_id: ReservationId(1),
        fee_schedule_version: 1,
        side: Side::Buy,
        stop_price: Price::try_new(10).unwrap(),
        price: Price::try_new(10).unwrap(),
        qty: Qty::try_new(3).unwrap(),
        child_tif: TimeInForce::Gtc,
        trigger_expiry: TriggerExpiry::Gtc,
    });
    let child = state(1, 1, AssetId(2), 35);
    book.place_stop_with_reservation(stop, LogicalTime::new(0), 1, child.clone())
        .unwrap();
    states.push(child);
    let mut registry = ReservationRegistry::new(states).unwrap();
    let ids = [TradeId(1)];
    let context = || TriggerRegistryContext {
        now: LogicalTime::new(0),
        current_session_id: 1,
        math: &math,
        trade_ids: &ids,
    };
    let reference = |seq, price| PriceReference {
        kind: PriceReferenceKind::LastTrade,
        source_seq: seq,
        price: Price::try_new(price).unwrap(),
    };
    let baseline = book
        .plan_trigger_stops_with_registry(reference(1, 9), context(), &registry, |_| Ok(()))
        .unwrap();
    book.apply_trigger_stops_with_registry(&mut registry, baseline)
        .unwrap();
    let plan = book
        .plan_trigger_stops_with_r2(
            reference(2, 11),
            context(),
            TradeSettlementContext {
                market_id: MarketId(7),
                math,
                base: SettlementAsset {
                    asset: AssetId(1),
                    scale: 0,
                },
                quote: SettlementAsset {
                    asset: AssetId(2),
                    scale: 0,
                },
                fees: &fees,
            },
            &registry,
            |order| match order {
                StopOrder::Limit(order) => {
                    check_stop_limit_reservation(&policy, &fees, &registry, MarketId(7), order)
                }
                _ => Err(RejectReason::InvalidOrderCombination),
            },
        )
        .unwrap();
    let events = book
        .apply_trigger_stops_with_registry(&mut registry, plan)
        .unwrap();
    let released = events
        .iter()
        .position(|e| {
            matches!(
                e,
                EngineEvent::ReservationReleased {
                    order_id: OrderId(4),
                    reason: ReservationReleaseReason::SelfTradePrevention,
                    ..
                }
            )
        })
        .unwrap();
    let trade = events
        .iter()
        .position(|e| matches!(e,EngineEvent::Trade(t) if t.maker_order_id==OrderId(5)))
        .unwrap();
    assert!(released < trade);
    assert_eq!(
        events
            .iter()
            .filter(|e| matches!(e, EngineEvent::SettlementDelta(_)))
            .count(),
        1
    );
    assert_eq!(
        book.reservation_state(OrderId(1)),
        registry.get(ReservationId(1))
    );
    assert_eq!(
        registry.get(ReservationId(1)).unwrap().reserves[0].atoms,
        15
    );
}

#[test]
fn triggered_market_protection_releases_with_protection_reason() {
    protection_case(false);
    protection_case(true);
}

fn protection_case(partial: bool) {
    use matching_domain::{
        LimitOrder, PriceBandConfig, ReservationReleaseReason, StopMarketOrder, StopMarketQuantity,
        TradeId,
    };
    use matching_risk::{SettlementAsset, TradeSettlementContext, check_stop_market_reservation};
    let math = MarketMath::try_new(0, 0, 0, 1, 1).unwrap();
    let policy = LimitReservePolicy::new((AssetId(1), 0), (AssetId(2), 0), math, 1, 1, 0).unwrap();
    let fees = FeeScheduleBook::new(
        FeeSchedule::new(
            1,
            FeeAssetPolicy::Quote,
            [(
                1,
                FeeTier {
                    maker_bps: 0,
                    taker_bps: 0,
                    rounding: FeeRounding::Floor,
                },
            )],
        )
        .unwrap(),
    );
    let state = |id, asset, atoms| ReservationState {
        reservation_id: ReservationId(id),
        order_id: OrderId(id),
        user_id: UserId(id as u64),
        market_id: MarketId(7),
        origin_session_id: OriginSessionId(1),
        version: 0,
        reserves: vec![AssetReserve { asset, atoms }],
        status: ReservationStatus::Active,
        fee_schedule_version: 1,
        fee_tier_id: 1,
    };
    let mut book = ProductionOrderBook::new(QueuePriorityPolicy::try_new(1024).unwrap());
    book.bind_market(MarketId(7)).unwrap();
    let maker = state(4, AssetId(1), 1);
    book.rest_with_reservation(
        LimitOrder {
            order_id: OrderId(4),
            user_id: UserId(4),
            stp_key: StpKey::new(0),
            stp_mode: StpMode::None,
            side: Side::Sell,
            price: Price::try_new(106).unwrap(),
            qty: Qty::try_new(1).unwrap(),
        },
        maker.clone(),
    )
    .unwrap();
    let mut states = vec![maker];
    if partial {
        let first = state(5, AssetId(1), 1);
        book.rest_with_reservation(
            LimitOrder {
                order_id: OrderId(5),
                user_id: UserId(5),
                stp_key: StpKey::new(0),
                stp_mode: StpMode::None,
                side: Side::Sell,
                price: Price::try_new(100).unwrap(),
                qty: Qty::try_new(1).unwrap(),
            },
            first.clone(),
        )
        .unwrap();
        states.push(first);
    }
    let stop = StopOrder::Market(StopMarketOrder {
        trigger_reference: PriceReferenceKind::LastTrade,
        market_protection: None,
        order_id: OrderId(1),
        user_id: UserId(1),
        stp_key: StpKey::new(0),
        stp_mode: StpMode::None,
        reservation_id: ReservationId(1),
        fee_schedule_version: 1,
        side: Side::Buy,
        stop_price: Price::try_new(105).unwrap(),
        quantity: StopMarketQuantity::Base(Qty::try_new(if partial { 2 } else { 1 }).unwrap()),
        child_tif: TimeInForce::Ioc,
        trigger_expiry: TriggerExpiry::Gtc,
    });
    let child = state(1, AssetId(2), if partial { 220 } else { 110 });
    book.place_stop_with_reservation(stop, LogicalTime::new(0), 1, child.clone())
        .unwrap();
    states.push(child);
    let mut registry = ReservationRegistry::new(states).unwrap();
    let ids = [TradeId(1)];
    let context = || TriggerRegistryContext {
        now: LogicalTime::new(0),
        current_session_id: 1,
        math: &math,
        trade_ids: &ids,
    };
    let reference = |seq, price| PriceReference {
        kind: PriceReferenceKind::LastTrade,
        source_seq: seq,
        price: Price::try_new(price).unwrap(),
    };
    let baseline = book
        .plan_trigger_stops_with_registry(reference(1, 100), context(), &registry, |_| Ok(()))
        .unwrap();
    book.apply_trigger_stops_with_registry(&mut registry, baseline)
        .unwrap();
    book.set_price_band_config(Some((
        PriceBandConfig {
            reference_kind: PriceReferenceKind::LastTrade,
            max_deviation_bps: 0,
        },
        math,
    )))
    .unwrap();
    let plan = book
        .plan_trigger_stops_with_r2(
            reference(2, 105),
            context(),
            TradeSettlementContext {
                market_id: MarketId(7),
                math,
                base: SettlementAsset {
                    asset: AssetId(1),
                    scale: 0,
                },
                quote: SettlementAsset {
                    asset: AssetId(2),
                    scale: 0,
                },
                fees: &fees,
            },
            &registry,
            |order| match order {
                StopOrder::Market(order) => check_stop_market_reservation(
                    &policy,
                    &fees,
                    &registry,
                    MarketId(7),
                    order,
                    Some(Price::try_new(105).unwrap()),
                ),
                _ => Err(RejectReason::InvalidOrderCombination),
            },
        )
        .unwrap();
    let events = book
        .apply_trigger_stops_with_registry(&mut registry, plan)
        .unwrap();
    assert!(events.iter().any(|e| matches!(
        e,
        EngineEvent::MarketProtectionTriggered {
            order_id: OrderId(1),
            ..
        }
    )));
    assert!(events.iter().any(|e|matches!(e,EngineEvent::ReservationReleased {order_id:OrderId(1),reason:ReservationReleaseReason::MarketProtection,released:Some(assets),..} if assets[0].atoms==if partial {120} else {110})));
    let protection = events
        .iter()
        .position(|e| {
            matches!(
                e,
                EngineEvent::MarketProtectionTriggered {
                    order_id: OrderId(1),
                    ..
                }
            )
        })
        .unwrap();
    let release = events
        .iter()
        .position(|e| {
            matches!(
                e,
                EngineEvent::ReservationReleased {
                    order_id: OrderId(1),
                    ..
                }
            )
        })
        .unwrap();
    assert!(protection < release);
    assert_eq!(
        events
            .iter()
            .filter(|e| matches!(e, EngineEvent::Trade(_)))
            .count(),
        usize::from(partial)
    );
    assert_eq!(
        events
            .iter()
            .filter(|e| matches!(e, EngineEvent::SettlementDelta(_)))
            .count(),
        usize::from(partial)
    );
}
