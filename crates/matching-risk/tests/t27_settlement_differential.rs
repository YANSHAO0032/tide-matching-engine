//! 独立整数算式逐案例核对成交、费用、预留释放和参考账本守恒。
use matching_domain::{
    AssetId, AssetReserve, FeeAssetPolicy, FeeRounding, FeeSchedule, FeeScheduleBook, FeeTier,
    MarketId, MarketMath, OrderId, OriginSessionId, Price, Qty, ReservationId, Side, TradeEvent,
    TradeId, UserId,
};
use matching_risk::{
    ReferenceLedger, ReservationRegistry, ReservationState, ReservationStatus, SettlementAsset,
    TradeSettlementContext, plan_trade_r2,
};

#[test]
fn independent_integer_oracle_checks_fees_releases_and_conservation() {
    for side in [Side::Buy, Side::Sell] {
        for asset in [
            FeeAssetPolicy::Base,
            FeeAssetPolicy::Quote,
            FeeAssetPolicy::ThirdPriced {
                asset: AssetId(3),
                quote_asset: AssetId(2),
                numerator: 1,
                denominator: 1,
            },
        ] {
            for maker_rate in [-1_000_i32, 0, 1_000] {
                for taker_rate in [-1_000_i32, 0, 1_000] {
                    let fees = FeeScheduleBook::new(
                        FeeSchedule::new(
                            1,
                            asset,
                            [(
                                1,
                                FeeTier {
                                    maker_bps: maker_rate,
                                    taker_bps: taker_rate,
                                    rounding: FeeRounding::Floor,
                                },
                            )],
                        )
                        .unwrap(),
                    );
                    let state = |id, user| ReservationState {
                        reservation_id: ReservationId(id),
                        order_id: OrderId(id),
                        user_id: UserId(user),
                        market_id: MarketId(7),
                        origin_session_id: OriginSessionId(1),
                        version: 0,
                        status: ReservationStatus::Active,
                        reserves: (1..=3)
                            .map(|asset| AssetReserve {
                                asset: AssetId(asset),
                                atoms: 100,
                            })
                            .collect(),
                        fee_schedule_version: 1,
                        fee_tier_id: 1,
                    };
                    let registry = ReservationRegistry::new([state(1, 1), state(2, 2)]).unwrap();
                    let trade = TradeEvent {
                        trade_id: TradeId(1),
                        maker_order_id: OrderId(1),
                        taker_order_id: OrderId(2),
                        price: Price::try_new(3).unwrap(),
                        qty: Qty::try_new(2).unwrap(),
                        maker_side: side,
                    };
                    let context = TradeSettlementContext {
                        market_id: MarketId(7),
                        math: MarketMath::try_new(0, 0, 0, 1, 1).unwrap(),
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
                    let (settlement, plan) = plan_trade_r2(
                        context,
                        &trade,
                        &registry,
                        ReservationId(1),
                        ReservationId(2),
                        &[ReservationId(1), ReservationId(2)],
                    )
                    .unwrap();
                    let fee_asset = match asset {
                        FeeAssetPolicy::Base => AssetId(1),
                        FeeAssetPolicy::Quote => AssetId(2),
                        _ => AssetId(3),
                    };
                    let basis = if fee_asset == AssetId(1) {
                        2_i128
                    } else {
                        6_i128
                    };
                    let expected = |rate: i32| (basis * i128::from(rate)).div_euclid(10_000);
                    assert_eq!(
                        (settlement.maker_fee.asset, settlement.maker_fee.atoms),
                        (fee_asset, expected(maker_rate))
                    );
                    assert_eq!(
                        (settlement.taker_fee.asset, settlement.taker_fee.atoms),
                        (fee_asset, expected(taker_rate))
                    );
                    let (maker_base, maker_quote) = if side == Side::Buy {
                        (2_i128, -6_i128)
                    } else {
                        (-2, 6)
                    };
                    assert_eq!(
                        (settlement.maker_base_delta, settlement.maker_quote_delta),
                        (maker_base, maker_quote)
                    );
                    for (index, (base, quote, fee)) in [
                        (maker_base, maker_quote, expected(maker_rate)),
                        (-maker_base, -maker_quote, expected(taker_rate)),
                    ]
                    .into_iter()
                    .enumerate()
                    {
                        let proceeds = if fee_asset == AssetId(1) {
                            base.max(0)
                        } else if fee_asset == AssetId(2) {
                            quote.max(0)
                        } else {
                            0
                        };
                        for a in 1..=3 {
                            let principal = if a == 1 {
                                (-base).max(0)
                            } else if a == 2 {
                                (-quote).max(0)
                            } else {
                                0
                            };
                            let fee_debit = if a == fee_asset.0 {
                                (fee - proceeds).max(0)
                            } else {
                                0
                            };
                            let remaining = 100 - u128::try_from(principal + fee_debit).unwrap();
                            assert_eq!(
                                plan.releases()[index]
                                    .reserves
                                    .iter()
                                    .find(|v| v.asset == AssetId(a))
                                    .unwrap()
                                    .atoms,
                                remaining
                            );
                        }
                    }
                    let mut ledger = ReferenceLedger::new(
                        UserId(99),
                        (1..=3)
                            .flat_map(|user| {
                                (1..=3).map(move |asset| (UserId(user), AssetId(asset), 100))
                            })
                            .chain((1..=3).map(|asset| (UserId(99), AssetId(asset), 100))),
                    )
                    .unwrap();
                    ledger.apply_event(&settlement.to_event_payload()).unwrap();
                    for a in 1..=3 {
                        assert_eq!(
                            ledger.balance(UserId(1), AssetId(a))
                                + ledger.balance(UserId(2), AssetId(a))
                                + ledger.balance(UserId(99), AssetId(a)),
                            300
                        );
                    }
                }
            }
        }
    }
}
