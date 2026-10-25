use matching_domain::{
    AmendOrder, AmendQuantityMode, AmendedQuantity, EngineEvent, LogicalTime, MarketId,
    MarketProtection, MarketProtectionPatch, OrderId, Price, PriceReferenceKind, Qty, QuoteAmount,
    RegularAmendKind, RejectReason, Side, TimeInForce, TradeEvent, TradeId, TriggerExpiry,
    TriggerExpiryPatch, UserId, classify_regular_amend, remaining_after_target_total,
    validate_amend_quantity_mode,
};

fn qty(value: u64) -> Qty {
    Qty::try_new(value).unwrap()
}

fn price(value: i64) -> Price {
    Price::try_new(value).unwrap()
}

#[test]
fn absolute_target_uses_original_filled_amount() {
    assert_eq!(
        remaining_after_target_total(qty(100), qty(70), Some(qty(80))),
        Ok(qty(50))
    );
    assert_eq!(
        remaining_after_target_total(qty(100), qty(70), None),
        Ok(qty(70))
    );
}

#[test]
fn target_must_leave_positive_remaining_and_state_must_be_consistent() {
    assert_eq!(
        remaining_after_target_total(qty(100), qty(70), Some(qty(30))),
        Err(RejectReason::InvalidQty)
    );
    assert_eq!(
        remaining_after_target_total(qty(100), qty(70), Some(qty(29))),
        Err(RejectReason::InvalidQty)
    );
    assert_eq!(
        remaining_after_target_total(qty(70), qty(100), None),
        Err(RejectReason::ArithmeticOverflow)
    );
    assert_eq!(
        remaining_after_target_total(qty(70), qty(100), Some(qty(120))),
        Err(RejectReason::ArithmeticOverflow)
    );
}

#[test]
fn regular_classification_respects_price_and_remaining() {
    assert_eq!(
        classify_regular_amend(price(10), price(10), qty(70), qty(50)),
        RegularAmendKind::InPlaceReduce
    );
    assert_eq!(
        classify_regular_amend(price(10), price(10), qty(70), qty(70)),
        RegularAmendKind::InPlaceReduce
    );
    assert_eq!(
        classify_regular_amend(price(10), price(10), qty(70), qty(90)),
        RegularAmendKind::RequeueSamePrice
    );
    assert_eq!(
        classify_regular_amend(price(10), price(11), qty(70), qty(50)),
        RegularAmendKind::RepriceAndMatch
    );
}

#[test]
fn quantity_modes_reject_switches_and_zero_budget() {
    assert_eq!(
        validate_amend_quantity_mode(AmendQuantityMode::BaseQty, Some(qty(2)), None),
        Ok(())
    );
    assert_eq!(
        validate_amend_quantity_mode(AmendQuantityMode::BaseQty, None, None),
        Ok(())
    );
    assert_eq!(
        validate_amend_quantity_mode(AmendQuantityMode::BaseQty, None, Some(QuoteAmount::new(1))),
        Err(RejectReason::InvalidOrderCombination)
    );
    assert_eq!(
        validate_amend_quantity_mode(
            AmendQuantityMode::UntriggeredStopMarketByQuote,
            Some(qty(2)),
            None
        ),
        Err(RejectReason::InvalidOrderCombination)
    );
    assert_eq!(
        validate_amend_quantity_mode(
            AmendQuantityMode::UntriggeredStopMarketByQuote,
            None,
            Some(QuoteAmount::new(1))
        ),
        Ok(())
    );
    assert_eq!(
        validate_amend_quantity_mode(
            AmendQuantityMode::UntriggeredStopMarketByQuote,
            None,
            Some(QuoteAmount::new(0))
        ),
        Err(RejectReason::InvalidQty)
    );
    assert_eq!(
        validate_amend_quantity_mode(
            AmendQuantityMode::UntriggeredStopMarketByQuote,
            Some(qty(2)),
            Some(QuoteAmount::new(1))
        ),
        Err(RejectReason::InvalidOrderCombination)
    );
}

#[test]
fn explicit_patches_preserve_keep_clear_and_set() {
    let expiry = TriggerExpiry::Gtd {
        expire_at: LogicalTime::new(12),
    };
    assert_ne!(TriggerExpiryPatch::Keep, TriggerExpiryPatch::ClearToGtc);
    assert_ne!(TriggerExpiryPatch::Keep, TriggerExpiryPatch::Set(expiry));
    assert_eq!(
        TriggerExpiryPatch::Set(expiry),
        TriggerExpiryPatch::Set(expiry)
    );

    let protection = MarketProtection {
        max_slippage_bps: Some(5),
        worst_price: Some(price(20)),
    };
    assert_ne!(MarketProtectionPatch::Keep, MarketProtectionPatch::Clear);
    assert_ne!(
        MarketProtectionPatch::Keep,
        MarketProtectionPatch::Set(protection.clone())
    );
    assert_eq!(
        MarketProtectionPatch::Set(protection.clone()),
        MarketProtectionPatch::Set(protection)
    );
}

#[test]
fn amend_payload_has_all_absolute_targets_and_explicit_patches() {
    let command = AmendOrder {
        market_id: MarketId::new(1),
        user_id: UserId::new(2),
        order_id: OrderId::new(3),
        new_price: Some(price(20)),
        new_qty: Some(qty(80)),
        new_quote_budget: None,
        new_visible_qty: Some(qty(10)),
        new_stop_price: Some(price(19)),
        new_trigger_reference: Some(PriceReferenceKind::MarkPrice),
        new_trigger_expiry: Some(TriggerExpiryPatch::Set(TriggerExpiry::Day {
            session_id: 4,
        })),
        new_tif: Some(TimeInForce::Gtc),
        new_market_protection: Some(MarketProtectionPatch::Clear),
        reservation_adjustment: None,
    };
    assert_eq!(command.new_qty, Some(qty(80)));
    assert_eq!(
        command.new_trigger_expiry,
        Some(TriggerExpiryPatch::Set(TriggerExpiry::Day {
            session_id: 4
        }))
    );
    assert_eq!(
        command.new_market_protection,
        Some(MarketProtectionPatch::Clear)
    );

    let omitted = AmendOrder {
        new_trigger_expiry: None,
        new_market_protection: None,
        ..command.clone()
    };
    let explicit_keep = AmendOrder {
        new_trigger_expiry: Some(TriggerExpiryPatch::Keep),
        new_market_protection: Some(MarketProtectionPatch::Keep),
        ..command
    };
    assert_ne!(omitted, explicit_keep);
}

#[test]
fn amended_event_carries_candidate_before_match_for_both_modes() {
    let base = EngineEvent::OrderAmended {
        order_id: OrderId::new(3),
        old_price: Some(price(10)),
        new_price: Some(price(11)),
        old_quantity: AmendedQuantity::BaseRemaining(qty(70)),
        new_quantity: AmendedQuantity::BaseRemaining(qty(50)),
        priority_retained: false,
    };
    let same = base.clone();
    assert_eq!(base, same);
    assert_ne!(
        base,
        EngineEvent::OrderAmended {
            order_id: OrderId::new(3),
            old_price: Some(price(10)),
            new_price: Some(price(11)),
            old_quantity: AmendedQuantity::BaseRemaining(qty(70)),
            new_quantity: AmendedQuantity::BaseRemaining(qty(49)),
            priority_retained: false,
        }
    );

    let quote = EngineEvent::OrderAmended {
        order_id: OrderId::new(4),
        old_price: None,
        new_price: None,
        old_quantity: AmendedQuantity::QuoteBudget(QuoteAmount::new(100)),
        new_quantity: AmendedQuantity::QuoteBudget(QuoteAmount::new(80)),
        priority_retained: true,
    };
    assert_ne!(base, quote);
    assert!(
        matches!(quote, EngineEvent::OrderAmended { new_quantity: AmendedQuantity::QuoteBudget(amount), .. } if amount.get() == 80)
    );

    let transcript = [
        base,
        EngineEvent::Trade(TradeEvent {
            trade_id: TradeId::new(1),
            maker_order_id: OrderId::new(5),
            taker_order_id: OrderId::new(3),
            price: price(11),
            qty: qty(20),
            maker_side: Side::Sell,
        }),
        EngineEvent::OrderPartiallyFilled {
            order_id: OrderId::new(3),
            remaining: qty(30),
        },
    ];
    assert!(
        matches!(&transcript[0], EngineEvent::OrderAmended { new_quantity: AmendedQuantity::BaseRemaining(candidate), .. } if *candidate == qty(50))
    );
    assert!(matches!(&transcript[1], EngineEvent::Trade(_)));
    assert!(
        matches!(&transcript[2], EngineEvent::OrderPartiallyFilled { remaining, .. } if *remaining == qty(30))
    );
}
