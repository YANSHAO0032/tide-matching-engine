use super::*;
use crate::market_plan::{ProtectionBoundaryStop, QuoteFillPlan};
use matching_domain::{MarketProtection, QuoteBudget, UserId};

fn math(tick_size: i64, lot_size: u64) -> MarketMath {
    MarketMath::try_new(0, 0, 0, tick_size, lot_size).unwrap()
}
fn market(id: u128, side: Side, budget: u128) -> MarketByQuote {
    MarketByQuote {
        order_id: OrderId::new(id),
        user_id: UserId::new(id as u64),
        stp_key: matching_domain::StpKey::new(0),
        stp_mode: matching_domain::StpMode::None,
        side,
        quote_budget: QuoteBudget::try_new(budget).unwrap(),
        market_protection: None,
    }
}

fn limit(id: u128, side: Side, price: i64, qty: u64) -> LimitOrder {
    LimitOrder {
        order_id: OrderId::new(id),
        user_id: UserId::new(id as u64),
        stp_key: matching_domain::StpKey::new(0),
        stp_mode: matching_domain::StpMode::None,
        side,
        price: Price::try_new(price).unwrap(),
        qty: Qty::try_new(qty).unwrap(),
    }
}

fn snapshot(book: &ReferenceOrderBook) -> (Vec<(LimitOrder, u64, QueuePriority)>, u128) {
    (
        book.orders
            .iter()
            .map(|maker| {
                (
                    maker.original_order().clone(),
                    maker.remaining(),
                    maker.priority(),
                )
            })
            .collect(),
        book.next_priority,
    )
}

#[test]
fn protected_quote_preflight_multilevel_fifo_and_sources() {
    let p = |value| Price::try_new(value).unwrap();
    let math = math(10, 1);

    let cases = [
        // 方向, 挂单方向, 更优价格, 边界外价格, 更远价格, 用户, 市场价格区间,
        // 已用预算, 来源
        (
            Side::Buy,
            Side::Sell,
            90,
            110,
            120,
            100,
            110,
            880,
            ProtectionSource::User,
        ),
        (
            Side::Buy,
            Side::Sell,
            90,
            110,
            120,
            110,
            100,
            880,
            ProtectionSource::MarketBand,
        ),
        (
            Side::Buy,
            Side::Sell,
            90,
            110,
            120,
            100,
            100,
            880,
            ProtectionSource::Both,
        ),
        (
            Side::Sell,
            Side::Buy,
            110,
            90,
            80,
            100,
            90,
            920,
            ProtectionSource::User,
        ),
        (
            Side::Sell,
            Side::Buy,
            110,
            90,
            80,
            90,
            100,
            920,
            ProtectionSource::MarketBand,
        ),
        (
            Side::Sell,
            Side::Buy,
            110,
            90,
            80,
            100,
            100,
            920,
            ProtectionSource::Both,
        ),
    ];

    for (side, maker_side, better, outside, farther, user, band, spent, source) in cases {
        let mut book = ReferenceOrderBook::new();

        // 源 Vec 顺序刻意不等于业务撮合顺序。
        // 价位 100 上标识符=40 先于标识符=10。
        for (id, price, qty) in [
            (90, farther, 2),
            (40, 100, 4),
            (30, better, 2),
            (10, 100, 3),
            (50, outside, 1),
        ] {
            book.rest(limit(id, maker_side, price, qty)).unwrap();
        }

        let mut incoming = market(100, side, 2000);
        incoming.market_protection = Some(MarketProtection {
            max_slippage_bps: None,
            worst_price: Some(p(user)),
        });

        let context = CommandStartProtectionContext {
            reference_price_at_command_start: p(100_003),
            market_band_worst: Some(p(band)),
        };

        let before = snapshot(&book);

        let plan = book
            .preflight_market_by_quote_with_protection(&incoming, &context, &math)
            .unwrap();

        assert_eq!(
            plan.fills,
            vec![
                QuoteFillPlan {
                    maker_order_id: OrderId::new(30),
                    maker_price: p(better),
                    qty: Qty::try_new(2).unwrap(),
                    notional: QuoteAmount::new((2 * better) as u128),
                },
                QuoteFillPlan {
                    maker_order_id: OrderId::new(40),
                    maker_price: p(100),
                    qty: Qty::try_new(4).unwrap(),
                    notional: QuoteAmount::new(400),
                },
                QuoteFillPlan {
                    maker_order_id: OrderId::new(10),
                    maker_price: p(100),
                    qty: Qty::try_new(3).unwrap(),
                    notional: QuoteAmount::new(300),
                },
            ]
        );

        assert_eq!(plan.spent_quote, QuoteAmount::new(spent));
        assert_eq!(plan.unused_quote, QuoteAmount::new(2000 - spent));

        assert_eq!(
            plan.stop,
            Some(ProtectionBoundaryStop {
                next_price: p(outside),
                effective_worst_price: p(100),
                source,
            })
        );

        // 包含 Vec 原顺序、订单内容、剩余量、优先级分配器。
        assert_eq!(snapshot(&book), before);
    }
}
#[test]
fn protected_quote_preflight_empty_dust_and_unbounded() {
    let p = |value| Price::try_new(value).unwrap();
    let math = math(10, 1);

    for (side, maker_side, better, outside, farther) in [
        (Side::Buy, Side::Sell, 90, 110, 120),
        (Side::Sell, Side::Buy, 110, 90, 80),
    ] {
        let context = CommandStartProtectionContext {
            reference_price_at_command_start: p(100_003),
            market_band_worst: Some(p(100)),
        };

        // 1. 空簿。
        let book = ReferenceOrderBook::new();
        let before = snapshot(&book);

        let plan = book
            .preflight_market_by_quote_with_protection(&market(100, side, 1000), &context, &math)
            .unwrap();

        assert!(plan.fills.is_empty());
        assert_eq!(plan.spent_quote, QuoteAmount::new(0));
        assert_eq!(plan.unused_quote, QuoteAmount::new(1000));
        assert_eq!(plan.stop, None);
        assert_eq!(snapshot(&book), before);

        // 2. 首档越界：即使预算 1 不够一手，也必须先终止。
        let mut book = ReferenceOrderBook::new();
        book.rest(limit(1, maker_side, outside, 5)).unwrap();
        book.rest(limit(2, maker_side, farther, 5)).unwrap();

        let before = snapshot(&book);

        let plan = book
            .preflight_market_by_quote_with_protection(&market(100, side, 1), &context, &math)
            .unwrap();

        assert!(plan.fills.is_empty());
        assert_eq!(plan.spent_quote.get(), 0);
        assert_eq!(plan.unused_quote.get(), 1);
        assert_eq!(
            plan.stop,
            Some(ProtectionBoundaryStop {
                next_price: p(outside),
                effective_worst_price: p(100),
                source: ProtectionSource::MarketBand,
            })
        );
        assert_eq!(snapshot(&book), before);

        // 3. 第一张挂单在保护范围内，但预算不足一手。
        let mut book = ReferenceOrderBook::new();
        book.rest(limit(1, maker_side, 100, 5)).unwrap();
        let before = snapshot(&book);

        let plan = book
            .preflight_market_by_quote_with_protection(&market(100, side, 99), &context, &math)
            .unwrap();

        assert!(plan.fills.is_empty());
        assert_eq!(plan.spent_quote.get(), 0);
        assert_eq!(plan.unused_quote.get(), 99);
        assert_eq!(plan.stop, None);
        assert_eq!(snapshot(&book), before);

        // 4. 已经成交一张挂单，下一档在保护边界内，但发生不足一手的预算。
        let mut book = ReferenceOrderBook::new();
        book.rest(limit(1, maker_side, better, 1)).unwrap();
        book.rest(limit(2, maker_side, 100, 5)).unwrap();

        let budget = better as u128 + 5;
        let before = snapshot(&book);

        let plan = book
            .preflight_market_by_quote_with_protection(&market(100, side, budget), &context, &math)
            .unwrap();

        assert_eq!(plan.fills.len(), 1);
        assert_eq!(plan.fills[0].maker_order_id, OrderId::new(1));
        assert_eq!(plan.fills[0].qty.get(), 1);
        assert_eq!(plan.spent_quote.get(), better as u128);
        assert_eq!(plan.unused_quote.get(), 5);
        assert_eq!(plan.stop, None);
        assert_eq!(snapshot(&book), before);

        // 5. 已成交一张挂单、预算只剩 5，
        // 但下一张挂单越界，应当先记录保护终止，而非不足一手的预算。
        let mut book = ReferenceOrderBook::new();
        book.rest(limit(1, maker_side, better, 1)).unwrap();
        book.rest(limit(2, maker_side, outside, 5)).unwrap();
        let before = snapshot(&book);

        let plan = book
            .preflight_market_by_quote_with_protection(&market(100, side, budget), &context, &math)
            .unwrap();

        assert_eq!(plan.fills.len(), 1);
        assert_eq!(plan.spent_quote.get(), better as u128);
        assert_eq!(plan.unused_quote.get(), 5);
        assert_eq!(
            plan.stop,
            Some(ProtectionBoundaryStop {
                next_price: p(outside),
                effective_worst_price: p(100),
                source: ProtectionSource::MarketBand,
            })
        );
        assert_eq!(snapshot(&book), before);

        // 6. 无用户保护、无市场价格区间：所有挂单可按预算进入计划。
        let mut book = ReferenceOrderBook::new();

        for (id, price, qty) in [
            (90, farther, 2),
            (40, 100, 4),
            (30, better, 2),
            (10, 100, 3),
            (50, outside, 1),
        ] {
            book.rest(limit(id, maker_side, price, qty)).unwrap();
        }

        let no_boundary = CommandStartProtectionContext {
            reference_price_at_command_start: p(100_003),
            market_band_worst: None,
        };

        let before = snapshot(&book);

        let plan = book
            .preflight_market_by_quote_with_protection(
                &market(100, side, 2000),
                &no_boundary,
                &math,
            )
            .unwrap();

        assert_eq!(
            plan.fills
                .iter()
                .map(|f| f.maker_order_id)
                .collect::<Vec<_>>(),
            vec![
                OrderId::new(30),
                OrderId::new(40),
                OrderId::new(10),
                OrderId::new(50),
                OrderId::new(90),
            ]
        );
        assert_eq!(plan.fills.len(), 5);
        assert_eq!(
            plan.spent_quote.get(),
            match side {
                Side::Buy => 1230,
                Side::Sell => 1170,
            }
        );
        assert_eq!(
            plan.unused_quote.get(),
            match side {
                Side::Buy => 770,
                Side::Sell => 830,
            }
        );
        assert_eq!(plan.stop, None);
        assert_eq!(snapshot(&book), before);
    }
}
#[test]
fn protected_quote_preflight_rejections_are_atomic() {
    let p = |value| Price::try_new(value).unwrap();
    let math = math(10, 1);

    for (side, maker_side) in [(Side::Buy, Side::Sell), (Side::Sell, Side::Buy)] {
        let mut book = ReferenceOrderBook::new();
        book.rest(limit(1, maker_side, 100_000, 5)).unwrap();

        let context = CommandStartProtectionContext {
            reference_price_at_command_start: p(100_003),
            market_band_worst: Some(p(100_000)),
        };

        let invalid_bps = MarketProtection {
            max_slippage_bps: Some(u32::MAX),
            worst_price: None,
        };

        let before = snapshot(&book);

        // 重复标识符优先于非法基点。
        let mut duplicate = market(1, side, 1000);
        duplicate.market_protection = Some(invalid_bps.clone());

        assert_eq!(
            book.preflight_market_by_quote_with_protection(&duplicate, &context, &math,),
            Err(RejectReason::DuplicateOrderId)
        );
        assert_eq!(snapshot(&book), before);

        // 不重复的订单：非法基点必须返回稳定线协议 3007。
        let mut invalid = market(100, side, 1000);
        invalid.market_protection = Some(invalid_bps);

        let result = book.preflight_market_by_quote_with_protection(&invalid, &context, &math);

        assert_eq!(result, Err(RejectReason::InvalidSlippageBps));
        assert_eq!(RejectReason::InvalidSlippageBps as u16, 3007);
        assert_eq!(snapshot(&book), before);

        // 显式最差价格未按价格步长=10 对齐。
        let mut invalid = market(100, side, 1000);
        invalid.market_protection = Some(MarketProtection {
            max_slippage_bps: None,
            worst_price: Some(p(100_003)),
        });

        assert_eq!(
            book.preflight_market_by_quote_with_protection(&invalid, &context, &math,),
            Err(RejectReason::InvalidTick)
        );
        assert_eq!(snapshot(&book), before);
    }

    // 买入基点计算溢出：即使市场价格区间有效也必须拒绝。
    let mut book = ReferenceOrderBook::new();
    book.rest(limit(1, Side::Sell, 100_000, 5)).unwrap();

    let context = CommandStartProtectionContext {
        reference_price_at_command_start: p(i64::MAX),
        market_band_worst: Some(p(100_000)),
    };

    let mut incoming = market(100, Side::Buy, 1000);
    incoming.market_protection = Some(MarketProtection {
        max_slippage_bps: Some(100),
        worst_price: None,
    });

    let before = snapshot(&book);

    assert_eq!(
        book.preflight_market_by_quote_with_protection(&incoming, &context, &math,),
        Err(RejectReason::ArithmeticOverflow)
    );
    assert_eq!(snapshot(&book), before);

    // 更重要的原子性：即使已只读规划一笔成交，
    // 后续挂单的数学校验失败，也绝不能改簿。
    for (side, maker_side, better, invalid_price) in [
        (Side::Buy, Side::Sell, 90, 101),
        (Side::Sell, Side::Buy, 110, 101),
    ] {
        let mut book = ReferenceOrderBook::new();

        book.rest(limit(1, maker_side, better, 1)).unwrap();
        book.rest(limit(2, maker_side, invalid_price, 5)).unwrap();

        let context = CommandStartProtectionContext {
            reference_price_at_command_start: p(100_003),
            market_band_worst: None,
        };

        let before = snapshot(&book);

        assert_eq!(
            book.preflight_market_by_quote_with_protection(
                &market(100, side, 1000),
                &context,
                &math,
            ),
            Err(RejectReason::InvalidTick)
        );

        assert_eq!(snapshot(&book), before);
    }
}
