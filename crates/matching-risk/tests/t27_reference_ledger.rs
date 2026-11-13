//! 验证参考账本的本金和费用守恒及失败原子性。
use matching_domain::{AssetId, ReservationId, TradeId, UserId};
use matching_risk::{FeeAmount, ReferenceLedger, ReferenceLedgerError, TradeSettlement};

fn settlement() -> TradeSettlement {
    TradeSettlement {
        trade_id: TradeId::new(1),
        market_id: matching_domain::MarketId(7),
        base_asset: AssetId(1),
        quote_asset: AssetId(2),
        maker_order: matching_domain::OrderId(1),
        taker_order: matching_domain::OrderId(2),
        maker_user: UserId(1),
        taker_user: UserId(2),
        maker_base_delta: -2,
        maker_quote_delta: 20,
        maker_fee: FeeAmount {
            asset: AssetId(2),
            atoms: 2,
            schedule_version: 1,
            tier_id: 1,
        },
        taker_fee: FeeAmount {
            asset: AssetId(2),
            atoms: 1,
            schedule_version: 2,
            tier_id: 1,
        },
        maker_reservation: ReservationId(1),
        taker_reservation: ReservationId(2),
    }
}

#[test]
fn ledger_conserves_each_asset_and_rejects_duplicate_trade() {
    let mut ledger = ReferenceLedger::new(
        UserId(99),
        [(UserId(1), AssetId(1), 2), (UserId(2), AssetId(2), 21)],
    )
    .unwrap();
    ledger
        .apply_trade(AssetId(1), AssetId(2), &settlement())
        .unwrap();
    assert_eq!(ledger.balance(UserId(1), AssetId(1)), 0);
    assert_eq!(ledger.balance(UserId(1), AssetId(2)), 18);
    assert_eq!(ledger.balance(UserId(2), AssetId(1)), 2);
    assert_eq!(ledger.balance(UserId(2), AssetId(2)), 0);
    assert_eq!(ledger.balance(UserId(99), AssetId(2)), 3);
    let before = ledger.clone();
    assert_eq!(
        ledger.apply_trade(AssetId(1), AssetId(2), &settlement()),
        Err(ReferenceLedgerError::DuplicateTrade)
    );
    assert_eq!(ledger, before);
}

#[test]
fn insufficient_funds_does_not_apply_any_leg() {
    let mut ledger = ReferenceLedger::new(
        UserId(99),
        [(UserId(1), AssetId(1), 2), (UserId(2), AssetId(2), 20)],
    )
    .unwrap();
    let before = ledger.clone();
    assert_eq!(
        ledger.apply_trade(AssetId(1), AssetId(2), &settlement()),
        Err(ReferenceLedgerError::InsufficientBalance)
    );
    assert_eq!(ledger, before);
}

#[test]
fn rebate_debits_fee_account_and_failure_is_atomic() {
    let mut trade = settlement();
    trade.maker_fee.atoms = -2;
    trade.taker_fee.atoms = 0;
    let mut ledger = ReferenceLedger::new(
        UserId(99),
        [
            (UserId(1), AssetId(1), 2),
            (UserId(2), AssetId(2), 20),
            (UserId(99), AssetId(2), 2),
        ],
    )
    .unwrap();
    ledger.apply_trade(AssetId(1), AssetId(2), &trade).unwrap();
    assert_eq!(ledger.balance(UserId(1), AssetId(2)), 22);
    assert_eq!(ledger.balance(UserId(99), AssetId(2)), 0);
    let mut insufficient = ReferenceLedger::new(
        UserId(99),
        [(UserId(1), AssetId(1), 2), (UserId(2), AssetId(2), 20)],
    )
    .unwrap();
    let before = insufficient.clone();
    assert_eq!(
        insufficient.apply_trade(AssetId(1), AssetId(2), &trade),
        Err(ReferenceLedgerError::InsufficientBalance)
    );
    assert_eq!(insufficient, before);
}
