use super::*;

fn execution_trade(id: u128, maker: u128) -> TradeEvent {
    use crate::{Price, TradeId};

    TradeEvent {
        trade_id: TradeId::new(id),
        maker_order_id: OrderId::new(maker),
        taker_order_id: OrderId::new(100),
        price: Price::try_new(105).unwrap(),
        qty: Qty::try_new(3).unwrap(),
        maker_side: Side::Sell,
    }
}

#[test]
fn market_by_quote_execution_preserves_trade_order() {
    let first = execution_trade(20, 30);
    let second = execution_trade(10, 40);
    let execution = MarketByQuoteExecution::new(
        vec![first.clone(), second.clone()],
        QuoteAmount::new(315),
        QuoteAmount::new(5),
    );

    assert_eq!(execution.trades(), &[first, second]);
    assert_eq!(execution.trades()[0].trade_id, crate::TradeId::new(20));
    assert_eq!(execution.trades()[1].trade_id, crate::TradeId::new(10));
}

#[test]
fn market_by_quote_execution_allows_zero_amounts() {
    let empty = MarketByQuoteExecution::new(Vec::new(), QuoteAmount::new(0), QuoteAmount::new(0));
    assert!(empty.trades().is_empty());
    assert_eq!(empty.spent_quote(), QuoteAmount::new(0));
    assert_eq!(empty.unused_quote(), QuoteAmount::new(0));

    let only_spent = MarketByQuoteExecution::new(
        vec![execution_trade(1, 2)],
        QuoteAmount::new(315),
        QuoteAmount::new(0),
    );
    assert_eq!(only_spent.unused_quote(), QuoteAmount::new(0));

    let only_unused =
        MarketByQuoteExecution::new(Vec::new(), QuoteAmount::new(0), QuoteAmount::new(100));
    assert_eq!(only_unused.spent_quote(), QuoteAmount::new(0));
}

#[test]
fn market_by_quote_execution_has_value_semantics_clone_and_debug() {
    let original = MarketByQuoteExecution::new(
        vec![execution_trade(1, 30), execution_trade(2, 10)],
        QuoteAmount::new(630),
        QuoteAmount::new(20),
    );
    assert_eq!(original.clone(), original);
    assert!(format!("{original:?}").contains("MarketByQuoteExecution"));

    let changed_trades = MarketByQuoteExecution::new(
        vec![execution_trade(2, 10), execution_trade(1, 30)],
        QuoteAmount::new(630),
        QuoteAmount::new(20),
    );
    assert_ne!(original, changed_trades);

    let changed_spent = MarketByQuoteExecution::new(
        original.trades().to_vec(),
        QuoteAmount::new(631),
        QuoteAmount::new(20),
    );
    assert_ne!(original, changed_spent);

    let changed_unused = MarketByQuoteExecution::new(
        original.trades().to_vec(),
        QuoteAmount::new(630),
        QuoteAmount::new(21),
    );
    assert_ne!(original, changed_unused);
}
fn protected_quote_trigger() -> EngineEvent {
    use crate::ProtectionSource;

    EngineEvent::MarketProtectionTriggered {
        order_id: OrderId::new(100),
        next_price: Price::try_new(110).unwrap(),
        effective_worst_price: Price::try_new(100).unwrap(),
        source: ProtectionSource::Both,
    }
}

#[test]
fn protected_market_by_quote_execution_preserves_amounts() {
    for (spent, unused) in [(0, 0), (0, 100), (100, 0), (315, 7)] {
        let execution = ProtectedMarketByQuoteExecution::new(
            Vec::new(),
            QuoteAmount::new(spent),
            QuoteAmount::new(unused),
        );

        assert!(execution.events().is_empty());
        assert_eq!(execution.spent_quote(), QuoteAmount::new(spent));
        assert_eq!(execution.unused_quote(), QuoteAmount::new(unused));
    }

    // 不进行金额加法、预算校验或溢出检查。
    let execution = ProtectedMarketByQuoteExecution::new(
        Vec::new(),
        QuoteAmount::new(u128::MAX),
        QuoteAmount::new(u128::MAX),
    );

    assert_eq!(execution.spent_quote().get(), u128::MAX);
    assert_eq!(execution.unused_quote().get(), u128::MAX);
}

#[test]
fn protected_market_by_quote_execution_preserves_event_order() {
    let first = EngineEvent::Trade(execution_trade(20, 30));
    let second = EngineEvent::Trade(execution_trade(10, 40));
    let trigger = protected_quote_trigger();

    // 只有 Trade。
    let trades_only = ProtectedMarketByQuoteExecution::new(
        vec![first.clone(), second.clone()],
        QuoteAmount::new(630),
        QuoteAmount::new(20),
    );

    assert_eq!(trades_only.events(), &[first.clone(), second.clone()]);

    // Trade* 后紧接保护终结事件。
    let with_trigger = ProtectedMarketByQuoteExecution::new(
        vec![first.clone(), second.clone(), trigger.clone()],
        QuoteAmount::new(630),
        QuoteAmount::new(20),
    );

    assert_eq!(with_trigger.events(), &[first, second, trigger]);

    assert_eq!(with_trigger.events().len(), 3);
    assert_eq!(with_trigger.spent_quote().get(), 630);
    assert_eq!(with_trigger.unused_quote().get(), 20);
}

#[test]
fn protected_market_by_quote_execution_has_value_semantics() {
    let events = vec![
        EngineEvent::Trade(execution_trade(20, 30)),
        protected_quote_trigger(),
    ];

    let original = ProtectedMarketByQuoteExecution::new(
        events.clone(),
        QuoteAmount::new(315),
        QuoteAmount::new(7),
    );

    // 相同字段的独立实例相等。
    let same = ProtectedMarketByQuoteExecution::new(
        events.clone(),
        QuoteAmount::new(315),
        QuoteAmount::new(7),
    );

    assert_eq!(original, same);
    assert_eq!(original.clone(), original);

    assert!(format!("{original:?}").contains("ProtectedMarketByQuoteExecution"));

    // 仅改变 `events`。
    let changed_events = ProtectedMarketByQuoteExecution::new(
        vec![
            EngineEvent::Trade(execution_trade(21, 30)),
            protected_quote_trigger(),
        ],
        QuoteAmount::new(315),
        QuoteAmount::new(7),
    );

    assert_ne!(original, changed_events);

    // 仅改变事件顺序，仍须不相等。
    let mut reversed_events = events.clone();
    reversed_events.reverse();

    let reordered = ProtectedMarketByQuoteExecution::new(
        reversed_events,
        QuoteAmount::new(315),
        QuoteAmount::new(7),
    );

    assert_ne!(original, reordered);

    // 仅改变 spent_quote。
    let changed_spent = ProtectedMarketByQuoteExecution::new(
        events.clone(),
        QuoteAmount::new(316),
        QuoteAmount::new(7),
    );

    assert_ne!(original, changed_spent);

    // 仅改变 unused_quote。
    let changed_unused =
        ProtectedMarketByQuoteExecution::new(events, QuoteAmount::new(315), QuoteAmount::new(8));

    assert_ne!(original, changed_unused);
}
