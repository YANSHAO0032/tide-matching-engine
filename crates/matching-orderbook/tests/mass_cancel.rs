//! 验证分片本地批量撤单的双模型事件与预留归属语义。

use matching_domain::{
    AmendOrder, AssetId, AssetReserve, EngineEvent, IcebergOrder, LimitOrder, LogicalTime,
    MarketId, OrderId, OrderRemaining, OriginSessionId, Price, PriceReferenceKind, Qty,
    QuoteAmount, QuoteBudget, ReservationId, ShardCancelScope, Side, StopMarketOrder,
    StopMarketQuantity, StopOrder, TimeInForce, TriggerExpiry, UserId,
};
use matching_orderbook::{ProductionOrderBook, QueuePriorityPolicy, ReferenceOrderBook};
use matching_risk::{
    ReservationApplyError, ReservationRegistry, ReservationState, ReservationStatus,
};

fn state(id: u128, user: u64, session: u64) -> ReservationState {
    state_in_market(id, user, 7, session)
}

fn state_in_market(id: u128, user: u64, market: u32, session: u64) -> ReservationState {
    ReservationState {
        reservation_id: ReservationId::new(id + 100),
        order_id: OrderId::new(id),
        user_id: UserId::new(user),
        market_id: MarketId::new(market),
        origin_session_id: OriginSessionId::new(session),
        version: 0,
        reserves: vec![AssetReserve {
            asset: AssetId::new(1),
            atoms: 100,
        }],
        status: ReservationStatus::Active,
        fee_schedule_version: 1,
        fee_tier_id: 1,
    }
}

fn regular(id: u128, user: u64) -> LimitOrder {
    LimitOrder {
        order_id: OrderId::new(id),
        user_id: UserId::new(user),
        stp_key: matching_domain::StpKey::new(0),
        stp_mode: matching_domain::StpMode::None,
        side: Side::Buy,
        price: Price::try_new(10).unwrap(),
        qty: Qty::try_new(5).unwrap(),
    }
}

fn quote_stop(id: u128, user: u64) -> StopOrder {
    StopOrder::Market(StopMarketOrder {
        trigger_reference: PriceReferenceKind::LastTrade,
        market_protection: None,
        order_id: OrderId::new(id),
        user_id: UserId::new(user),
        stp_key: matching_domain::StpKey::new(0),
        stp_mode: matching_domain::StpMode::None,
        reservation_id: ReservationId::new(id + 100),
        fee_schedule_version: 1,
        side: Side::Buy,
        stop_price: Price::try_new(12).unwrap(),
        quantity: StopMarketQuantity::Quote(QuoteBudget::try_new(19).unwrap()),
        child_tif: TimeInForce::Ioc,
        trigger_expiry: TriggerExpiry::Gtc,
    })
}

fn base_stop(id: u128, user: u64, trigger_expiry: TriggerExpiry) -> StopOrder {
    StopOrder::Market(StopMarketOrder {
        trigger_reference: PriceReferenceKind::LastTrade,
        market_protection: None,
        order_id: OrderId::new(id),
        user_id: UserId::new(user),
        stp_key: matching_domain::StpKey::new(0),
        stp_mode: matching_domain::StpMode::None,
        reservation_id: ReservationId::new(id + 100),
        fee_schedule_version: 1,
        side: Side::Sell,
        stop_price: Price::try_new(8).unwrap(),
        quantity: StopMarketQuantity::Base(Qty::try_new(7).unwrap()),
        child_tif: TimeInForce::Ioc,
        trigger_expiry,
    })
}

fn iceberg(id: u128, user: u64) -> IcebergOrder {
    IcebergOrder::try_new(
        LimitOrder {
            order_id: OrderId::new(id),
            user_id: UserId::new(user),
            stp_key: matching_domain::StpKey::new(0),
            stp_mode: matching_domain::StpMode::None,
            side: Side::Sell,
            price: Price::try_new(20).unwrap(),
            qty: Qty::try_new(9).unwrap(),
        },
        Qty::try_new(3).unwrap(),
    )
    .unwrap()
}

fn math() -> matching_domain::MarketMath {
    matching_domain::MarketMath::try_new(0, 0, 0, 1, 1).unwrap()
}

#[test]
fn unprotected_base_market_registry_rejects_overflow_without_mutation() {
    let mut reference = ReferenceOrderBook::new();
    let mut production = ProductionOrderBook::new(QueuePriorityPolicy::try_new(1024).unwrap());
    let mut maker = regular(1, 1);
    maker.side = Side::Sell;
    let maker_state = state(1, 1, 1);
    let taker_state = state(2, 2, 1);
    reference
        .rest_with_reservation(maker.clone(), maker_state.clone())
        .unwrap();
    production
        .rest_with_reservation(maker, maker_state.clone())
        .unwrap();
    let registry = ReservationRegistry::new(vec![maker_state, taker_state.clone()]).unwrap();
    let taker = matching_domain::MarketByBase {
        order_id: OrderId::new(2),
        user_id: UserId::new(2),
        stp_key: matching_domain::StpKey::new(0),
        stp_mode: matching_domain::StpMode::None,
        side: Side::Buy,
        qty: Qty::try_new(5).unwrap(),
        market_protection: None,
    };
    let math = matching_domain::MarketMath::try_new(0, 0, 38, 1, 1).unwrap();
    let ids = [matching_domain::TradeId::new(1)];
    let context = || matching_orderbook::MarketRegistryContext {
        math: &math,
        protection: None,
        trade_ids: &ids,
    };
    let before_r = reference.clone();
    let before_p = production.clone();
    assert!(matches!(
        reference.plan_market_with_registry(
            matching_orderbook::MarketRegistryOrder::Base(taker.clone()),
            taker_state.clone(),
            &registry,
            context()
        ),
        Err(matching_domain::RejectReason::ArithmeticOverflow)
    ));
    assert!(matches!(
        production.plan_market_with_registry(
            matching_orderbook::MarketRegistryOrder::Base(taker),
            taker_state,
            &registry,
            context()
        ),
        Err(matching_domain::RejectReason::ArithmeticOverflow)
    ));
    assert_eq!(reference, before_r);
    assert_eq!(production, before_p);
}

fn amend_quote_stop(id: u128, user: u64, budget: u128) -> AmendOrder {
    AmendOrder {
        market_id: MarketId::new(7),
        user_id: UserId::new(user),
        order_id: OrderId::new(id),
        new_price: None,
        new_qty: None,
        new_quote_budget: Some(QuoteAmount::new(budget)),
        new_visible_qty: None,
        new_stop_price: None,
        new_trigger_reference: None,
        new_trigger_expiry: None,
        new_tif: None,
        new_market_protection: None,
        reservation_adjustment: None,
    }
}

#[test]
fn stop_stp_terminal_moves_only_child_binding_to_pending_r2_in_both_models() {
    let mut reference = ReferenceOrderBook::new();
    let mut production = ProductionOrderBook::new(QueuePriorityPolicy::try_new(1024).unwrap());
    let maker_state = state(30, 1, 10);
    let stop_state = state(10, 2, 10);
    let mut reference_registry =
        ReservationRegistry::new([maker_state.clone(), stop_state.clone()]).unwrap();
    let mut production_registry = reference_registry.clone();
    let mut maker = regular(30, 1);
    maker.side = Side::Sell;
    maker.stp_key = matching_domain::StpKey::new(7);
    reference
        .rest_with_reservation(maker.clone(), maker_state.clone())
        .unwrap();
    production
        .rest_with_reservation(maker, maker_state)
        .unwrap();
    let mut stop = quote_stop(10, 2);
    let StopOrder::Market(ref mut child) = stop else {
        unreachable!()
    };
    child.stp_key = matching_domain::StpKey::new(7);
    child.stp_mode = matching_domain::StpMode::CancelTaker;
    reference
        .place_stop_with_reservation(stop.clone(), LogicalTime::new(1), 1, stop_state.clone())
        .unwrap();
    production
        .place_stop_with_reservation(stop, LogicalTime::new(1), 1, stop_state)
        .unwrap();
    let reference_price = |seq, price| matching_domain::PriceReference {
        kind: PriceReferenceKind::LastTrade,
        source_seq: seq,
        price: Price::try_new(price).unwrap(),
    };
    let math = math();
    let context = || matching_orderbook::TriggerRegistryContext {
        now: LogicalTime::new(1),
        current_session_id: 1,
        math: &math,
        trade_ids: &[],
    };
    let rp = reference
        .plan_trigger_stops_with_registry(
            reference_price(1, 10),
            context(),
            &reference_registry,
            |_| Ok(()),
        )
        .unwrap();
    let pp = production
        .plan_trigger_stops_with_registry(
            reference_price(1, 10),
            context(),
            &production_registry,
            |_| Ok(()),
        )
        .unwrap();
    reference
        .apply_trigger_stops_with_registry(&mut reference_registry, rp)
        .unwrap();
    production
        .apply_trigger_stops_with_registry(&mut production_registry, pp)
        .unwrap();
    let rp = reference
        .plan_trigger_stops_with_registry(
            reference_price(2, 12),
            context(),
            &reference_registry,
            |_| Ok(()),
        )
        .unwrap();
    let pp = production
        .plan_trigger_stops_with_registry(
            reference_price(2, 12),
            context(),
            &production_registry,
            |_| Ok(()),
        )
        .unwrap();
    let re = reference
        .apply_trigger_stops_with_registry(&mut reference_registry, rp)
        .unwrap();
    let pe = production
        .apply_trigger_stops_with_registry(&mut production_registry, pp)
        .unwrap();
    assert_eq!(re, pe);
    assert!(reference_registry.is_pending_r2(ReservationId::new(110)));
    assert!(production_registry.is_pending_r2(ReservationId::new(110)));
    assert!(!reference_registry.is_pending_r2(ReservationId::new(130)));
    assert!(!production_registry.is_pending_r2(ReservationId::new(130)));
    assert!(
        !pe.iter()
            .any(|event| matches!(event, EngineEvent::ReservationReleased { .. }))
    );
}

#[test]
fn stop_registry_plan_rejects_registry_drift_without_book_mutation() {
    let mut production = ProductionOrderBook::new(QueuePriorityPolicy::try_new(1024).unwrap());
    let stop_state = state(10, 2, 10);
    let mut registry = ReservationRegistry::new([stop_state.clone()]).unwrap();
    production
        .place_stop_with_reservation(
            quote_stop(10, 2),
            LogicalTime::new(1),
            1,
            stop_state.clone(),
        )
        .unwrap();
    let math = math();
    let context = matching_orderbook::TriggerRegistryContext {
        now: LogicalTime::new(1),
        current_session_id: 1,
        math: &math,
        trade_ids: &[],
    };
    let price = matching_domain::PriceReference {
        kind: PriceReferenceKind::LastTrade,
        source_seq: 1,
        price: Price::try_new(10).unwrap(),
    };
    let plan = production
        .plan_trigger_stops_with_registry(price, context, &registry, |_| Ok(()))
        .unwrap();
    let before = production.clone();
    let transition = registry
        .plan_binding_transitions([&stop_state], [])
        .unwrap();
    registry.apply_binding_transition(transition).unwrap();
    let changed = registry.clone();
    assert_eq!(
        production.apply_trigger_stops_with_registry(&mut registry, plan),
        Err(ReservationApplyError::StateChanged)
    );
    assert_eq!(production, before);
    assert_eq!(registry, changed);
}

#[test]
fn place_stp_cancel_maker_and_taker_use_pending_r2_in_both_models() {
    let mut reference = ReferenceOrderBook::new();
    let mut production = ProductionOrderBook::new(QueuePriorityPolicy::try_new(1024).unwrap());
    let maker_state = state(30, 1, 10);
    let taker_state = state(10, 2, 10);
    let mut rr = ReservationRegistry::new([maker_state.clone(), taker_state.clone()]).unwrap();
    let mut pr = rr.clone();
    let mut maker = regular(30, 1);
    maker.side = Side::Sell;
    maker.stp_key = matching_domain::StpKey::new(7);
    reference
        .rest_with_reservation(maker.clone(), maker_state.clone())
        .unwrap();
    production
        .rest_with_reservation(maker, maker_state)
        .unwrap();
    let mut taker = regular(10, 2);
    taker.stp_key = matching_domain::StpKey::new(7);
    taker.stp_mode = matching_domain::StpMode::CancelBoth;
    let math = math();
    let rc = matching_orderbook::reference_orderbook::ReferencePlaceContext {
        math: &math,
        now: LogicalTime::new(1),
        current_session_id: 1,
        trade_ids: &[],
    };
    let pc = matching_orderbook::production_orderbook::ProductionCancelReplaceContext {
        math: &math,
        now: LogicalTime::new(1),
        current_session_id: 1,
        trade_ids: &[],
    };
    let rp = reference
        .plan_place_with_registry(
            matching_orderbook::reference_orderbook::ReferencePlaceOrder::Limit {
                order: taker.clone(),
                time_in_force: TimeInForce::Gtc,
                post_only: false,
            },
            taker_state.clone(),
            &rr,
            rc,
        )
        .unwrap();
    let pp = production
        .plan_place_with_registry(
            matching_orderbook::production_orderbook::ProductionPlaceOrder::Limit {
                order: taker,
                time_in_force: TimeInForce::Gtc,
                post_only: false,
            },
            taker_state,
            &pr,
            pc,
        )
        .unwrap();
    let re = reference.apply_place_with_registry(&mut rr, rp).unwrap();
    let pe = production.apply_place_with_registry(&mut pr, pp).unwrap();
    assert_eq!(pe, re);
    assert!(pr.is_pending_r2(ReservationId::new(110)) && pr.is_pending_r2(ReservationId::new(130)));
    assert_eq!(pr, rr);
    assert!(
        !pe.iter()
            .any(|event| matches!(event, EngineEvent::ReservationReleased { .. }))
    );
}

#[test]
fn production_stp_place_missing_maker_binding_is_atomic() {
    let mut book = ProductionOrderBook::new(QueuePriorityPolicy::try_new(1024).unwrap());
    let mut maker = regular(30, 1);
    maker.side = Side::Sell;
    maker.stp_key = matching_domain::StpKey::new(7);
    book.rest(maker).unwrap();
    let state = state(10, 2, 10);
    let registry = ReservationRegistry::new([state.clone()]).unwrap();
    let mut taker = regular(10, 2);
    taker.stp_key = matching_domain::StpKey::new(7);
    taker.stp_mode = matching_domain::StpMode::CancelMaker;
    let before = book.clone();
    let math = math();
    let context = matching_orderbook::production_orderbook::ProductionCancelReplaceContext {
        math: &math,
        now: LogicalTime::new(1),
        current_session_id: 1,
        trade_ids: &[],
    };
    let result = book.plan_place_with_registry(
        matching_orderbook::production_orderbook::ProductionPlaceOrder::Limit {
            order: taker,
            time_in_force: TimeInForce::Gtc,
            post_only: false,
        },
        state,
        &registry,
        context,
    );
    assert_eq!(
        result.unwrap_err(),
        matching_domain::RejectReason::InvalidReservation
    );
    assert_eq!(book, before);
}

#[test]
fn quote_market_stp_cancel_both_preserves_budget_and_moves_bindings() {
    let mut reference = ReferenceOrderBook::new();
    let mut production = ProductionOrderBook::new(QueuePriorityPolicy::try_new(1024).unwrap());
    let maker_state = state(30, 1, 10);
    let taker_state = state(10, 2, 10);
    let mut rr = ReservationRegistry::new([maker_state.clone(), taker_state.clone()]).unwrap();
    let mut pr = rr.clone();
    let mut maker = regular(30, 1);
    maker.side = Side::Sell;
    maker.stp_key = matching_domain::StpKey::new(7);
    reference
        .rest_with_reservation(maker.clone(), maker_state.clone())
        .unwrap();
    production
        .rest_with_reservation(maker, maker_state)
        .unwrap();
    let taker = matching_domain::MarketByQuote {
        order_id: OrderId::new(10),
        user_id: UserId::new(2),
        stp_key: matching_domain::StpKey::new(7),
        stp_mode: matching_domain::StpMode::CancelBoth,
        side: Side::Buy,
        quote_budget: QuoteBudget::try_new(100).unwrap(),
        market_protection: None,
    };
    let math = math();
    let context = || matching_orderbook::MarketRegistryContext {
        math: &math,
        trade_ids: &[],
        protection: None,
    };
    let rp = reference
        .plan_market_with_registry(
            matching_orderbook::MarketRegistryOrder::Quote(taker.clone()),
            taker_state.clone(),
            &rr,
            context(),
        )
        .unwrap();
    let pp = production
        .plan_market_with_registry(
            matching_orderbook::MarketRegistryOrder::Quote(taker),
            taker_state,
            &pr,
            context(),
        )
        .unwrap();
    let re = reference.apply_market_with_registry(&mut rr, rp).unwrap();
    let pe = production.apply_market_with_registry(&mut pr, pp).unwrap();
    assert_eq!(pe, re);
    assert_eq!(pe.1, Some((QuoteAmount::new(0), QuoteAmount::new(100))));
    assert!(pr.is_pending_r2(ReservationId::new(110)) && pr.is_pending_r2(ReservationId::new(130)));
    assert_eq!(pr, rr);
    assert!(
        !pe.0
            .iter()
            .any(|event| matches!(event, EngineEvent::ReservationReleased { .. }))
    );
}

#[test]
fn amend_stp_cancel_taker_keeps_maker_binding_and_terminalizes_taker() {
    let mut r = ReferenceOrderBook::new();
    let mut p = ProductionOrderBook::new(QueuePriorityPolicy::try_new(1024).unwrap());
    let maker_state = state(30, 1, 10);
    let target_state = state(10, 2, 10);
    let mut rr = ReservationRegistry::new([maker_state.clone(), target_state.clone()]).unwrap();
    let mut pr = rr.clone();
    let mut maker = regular(30, 1);
    maker.price = Price::try_new(10).unwrap();
    maker.stp_key = matching_domain::StpKey::new(7);
    let mut target = regular(10, 2);
    target.side = Side::Sell;
    target.price = Price::try_new(12).unwrap();
    target.stp_key = matching_domain::StpKey::new(7);
    target.stp_mode = matching_domain::StpMode::CancelTaker;
    r.rest_with_reservation(maker.clone(), maker_state.clone())
        .unwrap();
    p.rest_with_reservation(maker, maker_state).unwrap();
    r.rest_with_reservation(target.clone(), target_state.clone())
        .unwrap();
    p.rest_with_reservation(target.clone(), target_state)
        .unwrap();
    let mut amend = amend_quote_stop(10, 2, 1);
    amend.new_quote_budget = None;
    amend.new_price = Some(Price::try_new(10).unwrap());
    let keys = matching_risk::RiskKeySet::new(1, []).unwrap();
    let execution = matching_orderbook::AmendExecutionContext {
        market_math: math(),
        command_id: matching_domain::CommandId::new(1),
        fee_schedule_version: 1,
        fee_tier_id: 1,
        risk_keys: &keys,
        required_reserves: vec![AssetReserve {
            asset: AssetId::new(1),
            atoms: 1,
        }],
    };
    let context = || matching_orderbook::AmendRegistryContext {
        now: LogicalTime::new(1),
        current_session_id: 1,
        trade_ids: &[],
        execution: &execution,
    };
    let rp = r.plan_amend_with_registry(&amend, &rr, context()).unwrap();
    let pp = p.plan_amend_with_registry(&amend, &pr, context()).unwrap();
    let re = r.apply_amend_with_registry(&mut rr, rp).unwrap();
    let pe = p.apply_amend_with_registry(&mut pr, pp).unwrap();
    assert_eq!(pe, re);
    assert!(pr.is_pending_r2(ReservationId::new(110)));
    assert!(!pr.is_pending_r2(ReservationId::new(130)));
    assert_eq!(pr, rr);
}

#[test]
fn mixed_session_mass_cancel_has_quote_remainder_and_exact_model_parity() {
    let mut reference = ReferenceOrderBook::new();
    let mut production = ProductionOrderBook::new(QueuePriorityPolicy::try_new(1024).unwrap());
    let a = state(30, 1, 10);
    let b = state(10, 1, 10);
    let untouched = state(20, 1, 11);
    let mut reference_registry =
        ReservationRegistry::new([a.clone(), b.clone(), untouched.clone()]).unwrap();
    let mut production_registry = reference_registry.clone();

    reference
        .rest_with_reservation(regular(30, 1), a.clone())
        .unwrap();
    production.rest_with_reservation(regular(30, 1), a).unwrap();
    reference
        .place_stop_with_reservation(quote_stop(10, 1), LogicalTime::new(1), 1, b.clone())
        .unwrap();
    production
        .place_stop_with_reservation(quote_stop(10, 1), LogicalTime::new(1), 1, b)
        .unwrap();
    reference
        .rest_with_reservation(regular(20, 1), untouched.clone())
        .unwrap();
    production
        .rest_with_reservation(regular(20, 1), untouched)
        .unwrap();

    let scope = ShardCancelScope::Session {
        user_id: UserId::new(1),
        session_id: OriginSessionId::new(10),
    };
    let reference_plan = reference
        .plan_mass_cancel(scope, &reference_registry)
        .unwrap();
    let production_plan = production
        .plan_mass_cancel(scope, &production_registry)
        .unwrap();
    let reference_result = reference
        .apply_mass_cancel(&mut reference_registry, reference_plan)
        .unwrap();
    let production_result = production
        .apply_mass_cancel(&mut production_registry, production_plan)
        .unwrap();

    assert_eq!(reference_result, production_result);
    assert_eq!(reference_result.0.canceled_orders, 2);
    assert_eq!(
        reference_result.1,
        vec![
            EngineEvent::OrderCanceled {
                order_id: OrderId::new(10),
                remaining: OrderRemaining::Quote(QuoteAmount::new(19)),
                reason: matching_domain::CancelReason::MassCancel
            },
            EngineEvent::OrderCanceled {
                order_id: OrderId::new(30),
                remaining: OrderRemaining::Base(Qty::try_new(5).unwrap()),
                reason: matching_domain::CancelReason::MassCancel
            },
        ]
    );
    assert!(reference_registry.is_pending_r2(ReservationId::new(110)));
    assert!(reference_registry.is_pending_r2(ReservationId::new(130)));
    assert!(!reference_registry.is_pending_r2(ReservationId::new(120)));
}

#[test]
fn all_cancels_regular_iceberg_and_base_stop_without_rewinding_priorities() {
    let mut reference = ReferenceOrderBook::new();
    let mut production = ProductionOrderBook::new(QueuePriorityPolicy::try_new(1024).unwrap());
    let regular_state = state(30, 1, 10);
    let iceberg_state = state(20, 1, 10);
    let stop_state = state(10, 1, 10);
    let mut reference_registry = ReservationRegistry::new([
        regular_state.clone(),
        iceberg_state.clone(),
        stop_state.clone(),
    ])
    .unwrap();
    let mut production_registry = reference_registry.clone();

    reference
        .rest_with_reservation(regular(30, 1), regular_state.clone())
        .unwrap();
    production
        .rest_with_reservation(regular(30, 1), regular_state)
        .unwrap();
    reference
        .place_iceberg_post_only_with_reservation(iceberg(20, 1), &math(), iceberg_state.clone())
        .unwrap();
    production
        .place_iceberg_post_only_with_reservation(iceberg(20, 1), &math(), iceberg_state)
        .unwrap();
    reference
        .place_stop_with_reservation(
            base_stop(10, 1, TriggerExpiry::Gtc),
            LogicalTime::new(1),
            9,
            stop_state.clone(),
        )
        .unwrap();
    production
        .place_stop_with_reservation(
            base_stop(10, 1, TriggerExpiry::Gtc),
            LogicalTime::new(1),
            9,
            stop_state,
        )
        .unwrap();
    let reference_priorities = (
        reference.next_queue_priority(),
        reference.next_stop_priority(),
    );
    let production_priorities = (
        production.next_queue_priority(),
        production.next_stop_priority(),
    );
    let reference_before_plan = reference.clone();
    let production_before_plan = production.clone();
    let reference_registry_before_plan = reference_registry.clone();
    let production_registry_before_plan = production_registry.clone();
    let reference_plan = reference
        .plan_mass_cancel(ShardCancelScope::All, &reference_registry)
        .unwrap();
    let production_plan = production
        .plan_mass_cancel(ShardCancelScope::All, &production_registry)
        .unwrap();
    assert_eq!(
        reference_plan,
        reference
            .plan_mass_cancel(ShardCancelScope::All, &reference_registry)
            .unwrap()
    );
    assert_eq!(
        production_plan,
        production
            .plan_mass_cancel(ShardCancelScope::All, &production_registry)
            .unwrap()
    );
    assert_eq!(reference, reference_before_plan);
    assert_eq!(production, production_before_plan);
    assert_eq!(reference_registry, reference_registry_before_plan);
    assert_eq!(production_registry, production_registry_before_plan);
    let reference_result = reference
        .apply_mass_cancel(&mut reference_registry, reference_plan)
        .unwrap();
    let production_result = production
        .apply_mass_cancel(&mut production_registry, production_plan)
        .unwrap();

    assert_eq!(reference_result, production_result);
    assert_eq!(reference_result.0.canceled_orders, 3);
    assert_eq!(
        reference_result.1,
        vec![
            EngineEvent::OrderCanceled {
                order_id: OrderId::new(10),
                remaining: OrderRemaining::Base(Qty::try_new(7).unwrap()),
                reason: matching_domain::CancelReason::MassCancel,
            },
            EngineEvent::OrderCanceled {
                order_id: OrderId::new(20),
                remaining: OrderRemaining::Base(Qty::try_new(9).unwrap()),
                reason: matching_domain::CancelReason::MassCancel,
            },
            EngineEvent::OrderCanceled {
                order_id: OrderId::new(30),
                remaining: OrderRemaining::Base(Qty::try_new(5).unwrap()),
                reason: matching_domain::CancelReason::MassCancel,
            },
        ]
    );
    assert_eq!(
        (
            reference.next_queue_priority(),
            reference.next_stop_priority()
        ),
        reference_priorities
    );
    assert_eq!(
        (
            production.next_queue_priority(),
            production.next_stop_priority()
        ),
        production_priorities
    );
    for reservation_id in [110, 120, 130] {
        assert!(reference_registry.is_pending_r2(ReservationId::new(reservation_id)));
        assert!(production_registry.is_pending_r2(ReservationId::new(reservation_id)));
    }

    let reference_plan = reference
        .plan_mass_cancel(ShardCancelScope::All, &reference_registry)
        .unwrap();
    let production_plan = production
        .plan_mass_cancel(ShardCancelScope::All, &production_registry)
        .unwrap();
    assert_eq!(
        reference
            .apply_mass_cancel(&mut reference_registry, reference_plan)
            .unwrap(),
        production
            .apply_mass_cancel(&mut production_registry, production_plan)
            .unwrap()
    );
    let reference_plan = reference
        .plan_mass_cancel(ShardCancelScope::All, &reference_registry)
        .unwrap();
    let production_plan = production
        .plan_mass_cancel(ShardCancelScope::All, &production_registry)
        .unwrap();
    let reference_empty = reference
        .apply_mass_cancel(&mut reference_registry, reference_plan)
        .unwrap();
    let production_empty = production
        .apply_mass_cancel(&mut production_registry, production_plan)
        .unwrap();
    assert_eq!(reference_empty, production_empty);
    assert_eq!(reference_empty.0.canceled_orders, 0);
    assert!(reference_empty.1.is_empty());
}

#[test]
fn session_uses_origin_not_day_and_amended_quote_budget_is_preserved() {
    let mut reference = ReferenceOrderBook::new();
    let mut production = ProductionOrderBook::new(QueuePriorityPolicy::try_new(1024).unwrap());
    let quote_state = state(10, 1, 10);
    let day_state = state(20, 1, 10);
    let different_origin = state(30, 1, 11);
    let mut reference_registry = ReservationRegistry::new([
        quote_state.clone(),
        day_state.clone(),
        different_origin.clone(),
    ])
    .unwrap();
    let mut production_registry = reference_registry.clone();

    reference
        .place_stop_with_reservation(
            quote_stop(10, 1),
            LogicalTime::new(1),
            77,
            quote_state.clone(),
        )
        .unwrap();
    production
        .place_stop_with_reservation(quote_stop(10, 1), LogicalTime::new(1), 77, quote_state)
        .unwrap();
    reference
        .place_stop_with_reservation(
            base_stop(20, 1, TriggerExpiry::Day { session_id: 77 }),
            LogicalTime::new(1),
            77,
            day_state.clone(),
        )
        .unwrap();
    production
        .place_stop_with_reservation(
            base_stop(20, 1, TriggerExpiry::Day { session_id: 77 }),
            LogicalTime::new(1),
            77,
            day_state,
        )
        .unwrap();
    reference
        .rest_with_reservation(regular(30, 1), different_origin.clone())
        .unwrap();
    production
        .rest_with_reservation(regular(30, 1), different_origin)
        .unwrap();
    let amend = amend_quote_stop(10, 1, 17);
    assert_eq!(
        reference
            .amend_stop_with_events(&amend, LogicalTime::new(2), 77)
            .unwrap(),
        production
            .amend_stop_with_events(&amend, LogicalTime::new(2), 77)
            .unwrap()
    );

    let scope = ShardCancelScope::Session {
        user_id: UserId::new(1),
        session_id: OriginSessionId::new(10),
    };
    let reference_plan = reference
        .plan_mass_cancel(scope, &reference_registry)
        .unwrap();
    let production_plan = production
        .plan_mass_cancel(scope, &production_registry)
        .unwrap();
    let reference_result = reference
        .apply_mass_cancel(&mut reference_registry, reference_plan)
        .unwrap();
    let production_result = production
        .apply_mass_cancel(&mut production_registry, production_plan)
        .unwrap();

    assert_eq!(reference_result, production_result);
    assert_eq!(
        reference_result.1,
        vec![
            EngineEvent::OrderCanceled {
                order_id: OrderId::new(10),
                remaining: OrderRemaining::Quote(QuoteAmount::new(17)),
                reason: matching_domain::CancelReason::MassCancel,
            },
            EngineEvent::OrderCanceled {
                order_id: OrderId::new(20),
                remaining: OrderRemaining::Base(Qty::try_new(7).unwrap()),
                reason: matching_domain::CancelReason::MassCancel,
            },
        ]
    );
    assert!(reference.reservation_state(OrderId::new(30)).is_some());
    assert!(production.reservation_state(OrderId::new(30)).is_some());
    assert!(!reference_registry.is_pending_r2(ReservationId::new(130)));
    assert!(!production_registry.is_pending_r2(ReservationId::new(130)));
}

#[test]
fn apply_rejects_plan_state_drift_without_installing_partial_post_state() {
    let regular_state = state(10, 1, 10);
    let mut reference = ReferenceOrderBook::new();
    let mut production = ProductionOrderBook::new(QueuePriorityPolicy::try_new(1024).unwrap());
    let mut reference_registry = ReservationRegistry::new([regular_state.clone()]).unwrap();
    let mut production_registry = reference_registry.clone();
    reference
        .rest_with_reservation(regular(10, 1), regular_state.clone())
        .unwrap();
    production
        .rest_with_reservation(regular(10, 1), regular_state)
        .unwrap();
    let reference_plan = reference
        .plan_mass_cancel(ShardCancelScope::All, &reference_registry)
        .unwrap();
    let production_plan = production
        .plan_mass_cancel(ShardCancelScope::All, &production_registry)
        .unwrap();
    reference.cancel(OrderId::new(10)).unwrap();
    production.cancel(OrderId::new(10)).unwrap();
    let reference_before_apply = reference.clone();
    let production_before_apply = production.clone();
    let reference_registry_before_apply = reference_registry.clone();
    let production_registry_before_apply = production_registry.clone();

    assert_eq!(
        reference.apply_mass_cancel(&mut reference_registry, reference_plan),
        Err(ReservationApplyError::StateChanged)
    );
    assert_eq!(
        production.apply_mass_cancel(&mut production_registry, production_plan),
        Err(ReservationApplyError::StateChanged)
    );
    assert_eq!(reference, reference_before_apply);
    assert_eq!(production, production_before_apply);
    assert_eq!(reference_registry, reference_registry_before_apply);
    assert_eq!(production_registry, production_registry_before_apply);
}

#[test]
fn user_market_and_market_scopes_only_remove_matching_identities() {
    let mut reference = ReferenceOrderBook::new();
    let mut production = ProductionOrderBook::new(QueuePriorityPolicy::try_new(1024).unwrap());
    let states = [
        state_in_market(10, 1, 7, 10),
        state_in_market(20, 1, 8, 10),
        state_in_market(30, 2, 7, 10),
        state_in_market(40, 2, 8, 10),
    ];
    let mut reference_registry = ReservationRegistry::new(states.clone()).unwrap();
    let mut production_registry = reference_registry.clone();
    for state in &states {
        reference
            .rest_with_reservation(
                regular(state.order_id.get(), state.user_id.get()),
                state.clone(),
            )
            .unwrap();
        production
            .rest_with_reservation(
                regular(state.order_id.get(), state.user_id.get()),
                state.clone(),
            )
            .unwrap();
    }

    let unmatched = ShardCancelScope::UserMarket {
        user_id: UserId::new(9),
        market_id: MarketId::new(9),
    };
    let reference_plan = reference
        .plan_mass_cancel(unmatched, &reference_registry)
        .unwrap();
    let production_plan = production
        .plan_mass_cancel(unmatched, &production_registry)
        .unwrap();
    assert_eq!(
        reference
            .apply_mass_cancel(&mut reference_registry, reference_plan)
            .unwrap(),
        production
            .apply_mass_cancel(&mut production_registry, production_plan)
            .unwrap()
    );

    for (scope, expected_id) in [
        (
            ShardCancelScope::User {
                user_id: UserId::new(1),
            },
            10_u128,
        ),
        (
            ShardCancelScope::UserMarket {
                user_id: UserId::new(2),
                market_id: MarketId::new(7),
            },
            30,
        ),
        (
            ShardCancelScope::Market {
                market_id: MarketId::new(8),
            },
            40,
        ),
    ] {
        let reference_plan = reference
            .plan_mass_cancel(scope, &reference_registry)
            .unwrap();
        let production_plan = production
            .plan_mass_cancel(scope, &production_registry)
            .unwrap();
        let reference_result = reference
            .apply_mass_cancel(&mut reference_registry, reference_plan)
            .unwrap();
        let production_result = production
            .apply_mass_cancel(&mut production_registry, production_plan)
            .unwrap();
        assert_eq!(reference_result, production_result);
        assert!(reference_result.1.iter().all(|event| {
            matches!(
                event,
                EngineEvent::OrderCanceled {
                    reason: matching_domain::CancelReason::MassCancel,
                    ..
                }
            )
        }));
        assert_eq!(
            reference_result
                .1
                .iter()
                .map(|event| match event {
                    EngineEvent::OrderCanceled { order_id, .. } => order_id.get(),
                    _ => unreachable!("批量撤单只能生成撤单事件"),
                })
                .collect::<Vec<_>>(),
            match expected_id {
                10 => vec![10, 20],
                id => vec![id],
            }
        );
    }
    assert!(reference.reservation_state(OrderId::new(10)).is_none());
    assert!(reference.reservation_state(OrderId::new(20)).is_none());
    assert!(reference.reservation_state(OrderId::new(30)).is_none());
    assert!(reference.reservation_state(OrderId::new(40)).is_none());
    assert!(production.reservation_state(OrderId::new(10)).is_none());
    assert!(production.reservation_state(OrderId::new(20)).is_none());
    assert!(production.reservation_state(OrderId::new(30)).is_none());
    assert!(production.reservation_state(OrderId::new(40)).is_none());
}
