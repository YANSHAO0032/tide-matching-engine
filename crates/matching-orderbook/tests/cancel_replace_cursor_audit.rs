//! 撤单替换游标的签名、时效和溢出原子性对照。
use ed25519_dalek::{Signer, SigningKey};
use matching_domain::{
    AssetId, AssetReserve, CancelReplaceOrder, ClusterId, CommandId, LimitOrder, LogicalTime,
    MarketId, MarketMath, OrderFlags, OrderId, Price, Qty, RejectReason, ReplacementOrder,
    ReplacementOrderKind, ReservationId, ReservationToken, ReservationTransition, ShardId, Side,
    StpMode, TimeInForce, TradeId, TradeIdCursor, UserId,
};
use matching_orderbook::production_orderbook::ProductionCancelReplaceContext;
use matching_orderbook::{ProductionOrderBook, QueuePriorityPolicy};
use matching_risk::{
    ExpectedReplacementReservation, ReservationRegistry, ReservationState, ReservationStatus,
    RiskKeySet,
};

fn input(id: u128, side: Side, qty: u64) -> LimitOrder {
    LimitOrder {
        order_id: OrderId::new(id),
        user_id: UserId::new(5),
        stp_key: matching_domain::StpKey::new(0),
        stp_mode: matching_domain::StpMode::None,
        side,
        price: Price::try_new(100).unwrap(),
        qty: Qty::try_new(qty).unwrap(),
    }
}

fn reserve(id: u128) -> ReservationState {
    ReservationState {
        reservation_id: ReservationId::new(100 + id),
        order_id: OrderId::new(id),
        user_id: UserId::new(5),
        market_id: MarketId::new(7),
        origin_session_id: matching_domain::OriginSessionId::new(1),
        version: 3,
        reserves: vec![AssetReserve {
            asset: AssetId::new(1),
            atoms: 10_000,
        }],
        status: ReservationStatus::Active,
        fee_schedule_version: 1,
        fee_tier_id: 2,
    }
}

fn expected() -> ExpectedReplacementReservation {
    ExpectedReplacementReservation {
        command_id: CommandId::new(41),
        old_reservation_id: reserve(1).reservation_id,
        old_order_id: OrderId::new(1),
        new_order_id: OrderId::new(9),
        user_id: UserId::new(5),
        market_id: MarketId::new(7),
        expected_version: 3,
        fee_schedule_version: 1,
        fee_tier_id: 2,
        required_reserves: vec![AssetReserve {
            asset: AssetId::new(1),
            atoms: 500,
        }],
    }
}

fn keys() -> RiskKeySet {
    RiskKeySet::new(1, [(31, SigningKey::from_bytes(&[7; 32]).verifying_key())]).unwrap()
}

fn request(new: bool, side: Side, qty: u64, tif: TimeInForce) -> CancelReplaceOrder {
    let e = expected();
    let reservation_transition = if new {
        let mut token = ReservationToken {
            token_id: 8,
            command_id: e.command_id,
            reservation_id: ReservationId::new(109),
            order_id: e.new_order_id,
            user_id: e.user_id,
            market_id: e.market_id,
            reserves: e.required_reserves.clone().into(),
            fee_schedule_version: 1,
            fee_tier_id: 2,
            risk_key_id: 31,
            signature: [0; 64],
        };
        token.signature = SigningKey::from_bytes(&[7; 32])
            .sign(&token.canonical_signing_payload().unwrap())
            .to_bytes();
        ReservationTransition::ReplaceWithNew {
            new_reservation: token,
        }
    } else {
        ReservationTransition::ReuseOld { adjustment: None }
    };
    CancelReplaceOrder {
        market_id: e.market_id,
        user_id: e.user_id,
        old_order_id: e.old_order_id,
        replacement: ReplacementOrder {
            market_id: e.market_id,
            user_id: e.user_id,
            stp_key: matching_domain::StpKey::new(0),
            stp_mode: matching_domain::StpMode::None,
            order_id: e.new_order_id,
            side,
            price: Price::try_new(100).unwrap(),
            qty: Qty::try_new(qty).unwrap(),
            kind: ReplacementOrderKind::Limit {
                tif,
                flags: OrderFlags {
                    post_only: false,
                    reduce_only: false,
                },
            },
        },
        reservation_transition,
    }
}

fn context<'a>(
    math: &'a MarketMath,
    trade_ids: &'a [TradeId],
) -> ProductionCancelReplaceContext<'a> {
    ProductionCancelReplaceContext {
        math,
        now: LogicalTime::new(10),
        current_session_id: 7,
        trade_ids,
    }
}

#[test]
fn signed_replace_cursor_failure_is_atomic_and_timed_stp_matches_legacy() {
    let math = MarketMath::try_new(0, 0, 0, 1, 1).unwrap();
    for tif in [
        TimeInForce::Day { session_id: 7 },
        TimeInForce::Gtd {
            expire_at: LogicalTime::new(20),
        },
    ] {
        let mut book = ProductionOrderBook::new(QueuePriorityPolicy::try_new(1024).unwrap());
        let mut old = input(1, Side::Buy, 3);
        old.price = Price::try_new(90).unwrap();
        book.rest_with_reservation(old, reserve(1)).unwrap();
        let mut same = input(2, Side::Sell, 1);
        same.user_id = UserId(5);
        book.rest_with_reservation(same, reserve(2)).unwrap();
        for id in [3, 4] {
            let mut maker = input(id, Side::Sell, 1);
            maker.user_id = UserId(id as u64 + 10);
            let mut state = reserve(id);
            state.user_id = maker.user_id;
            book.rest_with_reservation(maker, state).unwrap();
        }
        let mut state3 = reserve(3);
        state3.user_id = UserId(13);
        let mut state4 = reserve(4);
        state4.user_id = UserId(14);
        let mut registry =
            ReservationRegistry::new([reserve(1), reserve(2), state3, state4]).unwrap();
        let mut req = request(true, Side::Buy, 2, tif);
        req.replacement.stp_mode = StpMode::CancelMaker;
        let original = book.clone();
        let original_registry = registry.clone();
        let mut exhausted =
            TradeIdCursor::with_next_index(ClusterId(1), ShardId(2), 3, u64::from(u32::MAX))
                .unwrap();
        let before = exhausted.clone();
        let error = book
            .cancel_replace_with_cursor(
                &req,
                &expected(),
                &mut registry,
                &keys(),
                context(&math, &[]),
                &mut exhausted,
            )
            .unwrap_err();
        assert_eq!(error.reason, RejectReason::ArithmeticOverflow);
        assert_eq!(book, original);
        assert_eq!(registry, original_registry);
        assert_eq!(exhausted, before);
        let mut bad = req.clone();
        if let ReservationTransition::ReplaceWithNew {
            ref mut new_reservation,
        } = bad.reservation_transition
        {
            new_reservation.signature[0] ^= 1;
        }
        let mut cursor = TradeIdCursor::new(ClusterId(1), ShardId(2), 3);
        let before = cursor.clone();
        assert!(
            book.cancel_replace_with_cursor(
                &bad,
                &expected(),
                &mut registry,
                &keys(),
                context(&math, &[]),
                &mut cursor
            )
            .is_err()
        );
        assert_eq!(book, original);
        assert_eq!(registry, original_registry);
        assert_eq!(cursor, before);
        let mut ids_cursor = cursor.clone();
        let ids = [
            ids_cursor.next_trade_id().unwrap(),
            ids_cursor.next_trade_id().unwrap(),
        ];
        let mut legacy = book.clone();
        let mut legacy_registry = registry.clone();
        let plan = legacy
            .plan_cancel_replace(
                &req,
                &expected(),
                &legacy_registry,
                &keys(),
                context(&math, &ids),
            )
            .unwrap();
        let expected_events = legacy
            .apply_cancel_replace(&mut legacy_registry, plan)
            .unwrap();
        let events = book
            .cancel_replace_with_cursor(
                &req,
                &expected(),
                &mut registry,
                &keys(),
                context(&math, &[]),
                &mut cursor,
            )
            .unwrap();
        assert_eq!(events, expected_events);
        assert_eq!(book, legacy);
        assert_eq!(registry, legacy_registry);
        assert_eq!(cursor, ids_cursor);
    }
}

#[test]
fn replace_cursor_band_and_tick_fail_without_mutation() {
    use matching_domain::{PriceBandConfig, PriceReferenceKind};
    let math = MarketMath::try_new(0, 0, 0, 2, 1).unwrap();
    let mut book = ProductionOrderBook::new(QueuePriorityPolicy::try_new(1024).unwrap());
    let mut old = input(1, Side::Buy, 3);
    old.price = Price::try_new(90).unwrap();
    book.rest_with_reservation(old, reserve(1)).unwrap();
    let mut registry = ReservationRegistry::new([reserve(1)]).unwrap();
    let mut cursor = TradeIdCursor::new(ClusterId(1), ShardId(2), 3);
    let mut req = request(true, Side::Buy, 2, TimeInForce::Gtc);
    req.replacement.price = Price::try_new(101).unwrap();
    let before = (book.clone(), registry.clone(), cursor.clone());
    let result = book
        .cancel_replace_with_cursor(
            &req,
            &expected(),
            &mut registry,
            &keys(),
            context(&math, &[]),
            &mut cursor,
        )
        .unwrap_err();
    assert_eq!(result.reason, RejectReason::InvalidTick);
    assert_eq!((book.clone(), registry.clone(), cursor.clone()), before);
    book.set_price_band_config(Some((
        PriceBandConfig {
            reference_kind: PriceReferenceKind::MarkPrice,
            max_deviation_bps: 100,
        },
        math,
    )))
    .unwrap();
    let before = (book.clone(), registry.clone(), cursor.clone());
    let result = book
        .cancel_replace_with_cursor(
            &req,
            &expected(),
            &mut registry,
            &keys(),
            context(&math, &[]),
            &mut cursor,
        )
        .unwrap_err();
    assert_eq!(result.reason, RejectReason::ReferencePriceUnavailable);
    assert_eq!((book, registry, cursor), before);
}
