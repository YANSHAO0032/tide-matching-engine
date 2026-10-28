//! 验证撤单事件的基础数量与报价预算余量协议。

use matching_domain::{
    CancelReason, EngineEvent, MarketId, OrderId, OrderRemaining, OriginSessionId, Qty,
    QuoteAmount, ReservationReleaseReason, ShardCancelScope, UserId, matches_shard_cancel_scope,
};

#[test]
fn canceled_base_order_carries_base_remainder() {
    let event = EngineEvent::OrderCanceled {
        order_id: OrderId::new(1),
        remaining: OrderRemaining::Base(Qty::try_new(7).unwrap()),
        reason: CancelReason::MassCancel,
    };

    assert!(matches!(
        event,
        EngineEvent::OrderCanceled {
            remaining: OrderRemaining::Base(qty),
            ..
        } if qty == Qty::try_new(7).unwrap()
    ));
}

#[test]
fn canceled_quote_budget_order_carries_quote_remainder() {
    let event = EngineEvent::OrderCanceled {
        order_id: OrderId::new(2),
        remaining: OrderRemaining::Quote(QuoteAmount::new(19)),
        reason: CancelReason::MassCancel,
    };

    assert!(matches!(
        event,
        EngineEvent::OrderCanceled {
            remaining: OrderRemaining::Quote(amount),
            ..
        } if amount == QuoteAmount::new(19)
    ));
}

#[test]
fn mass_cancel_reason_codes_are_stable() {
    assert_eq!(CancelReason::MassCancel as u16, 1102);
    assert_eq!(ReservationReleaseReason::MassCancel as u16, 1303);
}

#[test]
fn shard_cancel_scope_matches_only_persistent_identity_fields() {
    let user = UserId::new(1);
    let other_user = UserId::new(2);
    let market = MarketId::new(7);
    let other_market = MarketId::new(8);
    let origin = OriginSessionId::new(10);
    let other_origin = OriginSessionId::new(11);

    assert!(matches_shard_cancel_scope(
        ShardCancelScope::User { user_id: user },
        user,
        other_market,
        other_origin,
    ));
    assert!(!matches_shard_cancel_scope(
        ShardCancelScope::User { user_id: user },
        other_user,
        market,
        origin,
    ));
    assert!(matches_shard_cancel_scope(
        ShardCancelScope::Market { market_id: market },
        other_user,
        market,
        other_origin,
    ));
    assert!(!matches_shard_cancel_scope(
        ShardCancelScope::Market { market_id: market },
        user,
        other_market,
        origin,
    ));
    assert!(matches_shard_cancel_scope(
        ShardCancelScope::UserMarket {
            user_id: user,
            market_id: market,
        },
        user,
        market,
        other_origin,
    ));
    assert!(!matches_shard_cancel_scope(
        ShardCancelScope::UserMarket {
            user_id: user,
            market_id: market,
        },
        user,
        other_market,
        origin,
    ));
    assert!(!matches_shard_cancel_scope(
        ShardCancelScope::UserMarket {
            user_id: user,
            market_id: market,
        },
        other_user,
        market,
        origin,
    ));
    assert!(matches_shard_cancel_scope(
        ShardCancelScope::Session {
            user_id: user,
            session_id: origin,
        },
        user,
        other_market,
        origin,
    ));
    assert!(!matches_shard_cancel_scope(
        ShardCancelScope::Session {
            user_id: user,
            session_id: origin,
        },
        user,
        market,
        other_origin,
    ));
    assert!(!matches_shard_cancel_scope(
        ShardCancelScope::Session {
            user_id: user,
            session_id: origin,
        },
        other_user,
        market,
        origin,
    ));
    assert!(matches_shard_cancel_scope(
        ShardCancelScope::All,
        other_user,
        other_market,
        other_origin,
    ));
}
