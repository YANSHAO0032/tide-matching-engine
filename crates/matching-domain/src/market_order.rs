use crate::{
    EngineEvent, MarketMath, OrderId, Price, Qty, QuoteAmount, QuoteBudget, RejectReason, Side,
    TradeEvent, UserId,
};

/// 按基础币数量提交的市价订单输入。
///
/// 不携带价格、TIF 或撮合运行态；可承载用户保护参数。
/// 只有接收命令开始保护上下文的执行入口才会解释该参数。
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MarketByBase {
    pub order_id: OrderId,
    pub user_id: UserId,
    pub side: Side,
    pub qty: Qty,
    pub market_protection: Option<MarketProtection>,
}

/// 按报价币预算提交的市价订单输入。
///
/// quote_budget 是严格正的初始预算，不含手续费。
/// 剩余预算属于后续撮合运行态，不保存在该输入对象中。
/// 普通执行入口不解释 market_protection；带命令开始保护上下文的执行入口
/// 使用它冻结有效边界并可产生保护终结事件。
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MarketByQuote {
    pub order_id: OrderId,
    pub user_id: UserId,
    pub side: Side,
    pub quote_budget: QuoteBudget,
    pub market_protection: Option<MarketProtection>,
}

/// 市价命令可选的用户保护参数。
///
/// 该值对象仅承载输入配置；有效保护边界须在命令开始捕获参考价后由后续撮合路径计算。
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MarketProtection {
    pub max_slippage_bps: Option<u32>,
    pub worst_price: Option<Price>,
}

/// 市价保护触发事件的来源标签。
///
/// Wire 编码固定为 u8：
/// - 1: User
/// - 2: MarketBand
/// - 3: Both
///
/// 已发布的数值不可修改、重排或复用。
/// 此类型不负责判断实际保护来源。
#[repr(u8)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ProtectionSource {
    User = 1,
    MarketBand = 2,
    Both = 3,
}

/// 单次命令开始时固定的市价保护上下文。
///
/// reference_price_at_command_start:
///   由外层在命令开始时捕获的严格正参考价格。
///
/// market_band_worst:
///   由外层根据市场配置预先计算完成的可选价格带边界。
///
/// 本对象仅保存不可变值语义，不负责：
/// - 选择或更新 reference 来源；
/// - 计算 market band；
/// - 校验用户保护参数；
/// - 判断是否必须要求保护；
/// - 执行撮合或产生事件。
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CommandStartProtectionContext {
    pub reference_price_at_command_start: Price,
    pub market_band_worst: Option<Price>,
}

impl MarketProtection {
    /// 计算用户侧最终保护候选价。
    ///
    /// 只合并：
    /// - max_slippage_bps 派生出的保护价；
    /// - 显式 worst_price。
    ///
    /// 不处理 market band、reference state、maker price 或任何撮合副作用。
    pub fn user_protection_price(
        &self,
        reference: Price,
        side: Side,
        tick_size: i64,
    ) -> Result<Option<Price>, RejectReason> {
        // 保存方向，避免依赖 Side 必须是 Copy。
        let buy_side = match &side {
            Side::Buy => true,
            Side::Sell => false,
        };

        // 1. bps 候选必须优先计算。
        //
        // 因此：
        // - bps >= 10_000 仍稳定优先返回 InvalidSlippageBps(3007)；
        // - tick / overflow 完全复用已验收 helper 的语义；
        // - 此处绝不复制 bps 公式。
        let bps_price = match self.max_slippage_bps {
            Some(bps) => Some(slippage_protection_price(reference, side, bps, tick_size)?),
            None => None,
        };

        // 2. explicit worst price 单独验证 tick。
        let explicit_price = match self.worst_price {
            Some(price) => {
                if tick_size <= 0 {
                    return Err(RejectReason::InvalidTick);
                }

                if price.get() % tick_size != 0 {
                    return Err(RejectReason::InvalidTick);
                }

                Some(price)
            }
            None => None,
        };

        // 3. 只做用户候选合并。
        let result = match (bps_price, explicit_price) {
            (None, None) => None,

            (Some(price), None) | (None, Some(price)) => Some(price),

            (Some(bps), Some(explicit)) => {
                if buy_side {
                    // Buy 越低越严格。
                    Some(if bps <= explicit { bps } else { explicit })
                } else {
                    // Sell 越高越严格。
                    Some(if bps >= explicit { bps } else { explicit })
                }
            }
        };

        Ok(result)
    }
}

/// 带市场价格保护的 MarketByQuote 执行结果。
///
/// 仅保存调用方提供的原序 EngineEvent 与报价金额。
/// 可承载 Trade 和 MarketProtectionTriggered。
///
/// 不计算成交名义额、不检查预算守恒、不计算保护边界，
/// 不验证事件顺序，也不执行订单簿操作。
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ProtectedMarketByQuoteExecution {
    events: Vec<EngineEvent>,
    spent_quote: QuoteAmount,
    unused_quote: QuoteAmount,
}

impl ProtectedMarketByQuoteExecution {
    /// 保留已按撮合顺序产生的事件及已由执行器校验的报价金额。
    pub fn new(
        events: Vec<EngineEvent>,
        spent_quote: QuoteAmount,
        unused_quote: QuoteAmount,
    ) -> Self {
        Self {
            events,
            spent_quote,
            unused_quote,
        }
    }

    /// 按产生顺序借用 Trade 与可选的终结保护事件。
    pub fn events(&self) -> &[EngineEvent] {
        &self.events
    }

    /// 返回已执行的 quote 本金；零表示没有成交。
    pub fn spent_quote(&self) -> QuoteAmount {
        self.spent_quote
    }

    /// 返回未使用的初始 quote 预算；该金额不是成交。
    pub fn unused_quote(&self) -> QuoteAmount {
        self.unused_quote
    }
}

/// 判断 maker 价格是否位于已经计算好的用户保护边界内。
///
/// `effective_worst_price` 必须由上游提前计算完成：
/// - Buy: maker_price <= boundary 时允许；
/// - Sell: maker_price >= boundary 时允许；
/// - None: 当前命令没有有效保护边界，此项价格检查允许。
///
/// 本函数不负责：
/// - 计算 slippage bps；
/// - 校验 tick；
/// - 合并 explicit worst price；
/// - 获取 reference；
/// - 生成事件或修改任何撮合状态。
pub fn maker_price_within_protection(
    side: Side,
    maker_price: Price,
    effective_worst_price: Option<Price>,
) -> bool {
    match effective_worst_price {
        None => true,
        Some(boundary) => match side {
            Side::Buy => maker_price <= boundary,
            Side::Sell => maker_price >= boundary,
        },
    }
}

/// 合并用户保护价格和市场价格带候选边界。
///
/// 两个输入均应由调用方预先计算完成。
/// Buy 取更低价格，Sell 取更高价格。
///
/// None 只表示没有有效边界，不决定订单是否必须要求保护。
pub fn effective_protection_price(
    side: Side,
    user_worst: Option<Price>,
    market_band_worst: Option<Price>,
) -> Option<Price> {
    match (user_worst, market_band_worst) {
        (None, None) => None,
        (Some(price), None) | (None, Some(price)) => Some(price),

        (Some(user), Some(band)) => Some(match side {
            Side::Buy => user.min(band),
            Side::Sell => user.max(band),
        }),
    }
}

/// 合并已计算的用户保护与市场价格带候选，并归因有效边界。
///
/// Buy 取较小边界，Sell 取较大边界。
///
/// 来源表示哪个候选决定最终有效边界：
/// - 仅用户候选生效：User
/// - 仅市场 band 候选生效：MarketBand
/// - 两个候选边界相等：Both
///
/// 不计算 bps/tick，不获取 reference 或市场状态，
/// 不判断 maker 是否越界，也不产生事件。
pub fn effective_protection_with_source(
    side: Side,
    user_worst: Option<Price>,
    market_band_worst: Option<Price>,
) -> Option<(Price, ProtectionSource)> {
    // 完全复用已验收的 Buy/Sell 取严逻辑。
    let effective_worst = effective_protection_price(side, user_worst, market_band_worst)?;

    // 只根据哪项候选等于有效边界决定来源。
    let user_binding = user_worst == Some(effective_worst);
    let band_binding = market_band_worst == Some(effective_worst);

    let source = match (user_binding, band_binding) {
        (true, true) => ProtectionSource::Both,
        (true, false) => ProtectionSource::User,
        (false, true) => ProtectionSource::MarketBand,
        (false, false) => unreachable!("effective protection must equal an input candidate"),
    };

    Some((effective_worst, source))
}

/// 在命令开始时组合用户保护候选与已计算的 market-band 候选。
///
/// reference_price_at_command_start 由调用方提前捕获。
/// math 提供已验证的 tick_size。
/// market_band_worst 由外部配置/市场参考价计算层提供并完成 tick 校验；
/// 本函数不会再次验证 band 对齐。
///
/// 先验证用户输入，再合并有效候选。
/// 本函数不获取市场状态，也不判断是否必须要求保护。
pub fn effective_protection_at_command_start(
    protection: Option<&MarketProtection>,
    context: &CommandStartProtectionContext,
    side: Side,
    math: &MarketMath,
) -> Result<Option<Price>, RejectReason> {
    Ok(
        effective_protection_at_command_start_with_source(protection, context, side, math)?
            .map(|(price, _source)| price),
    )
}

/// 根据命令开始时捕获的保护上下文，计算有效价格及其来源。
///
/// 原有用户保护校验错误直接传播。
/// 不获取市场状态，不更新 context，不判断 maker 是否越界。
pub fn effective_protection_at_command_start_with_source(
    protection: Option<&MarketProtection>,
    context: &CommandStartProtectionContext,
    side: Side,
    math: &MarketMath,
) -> Result<Option<(Price, ProtectionSource)>, RejectReason> {
    let user_worst = match protection {
        Some(protection) => protection.user_protection_price(
            context.reference_price_at_command_start,
            side,
            math.tick_size(),
        )?,
        None => None,
    };

    Ok(effective_protection_with_source(
        side,
        user_worst,
        context.market_band_worst,
    ))
}

const BPS_DENOMINATOR: u128 = 10_000;

/// 根据命令开始时捕获的 reference price 计算用户滑点保护边界。
///
/// 这是纯整数计算：
/// - Buy: 先按 bps 向下取整，再向下对齐 tick；
/// - Sell: 先按 bps 向上取整，再向上对齐 tick。
///
/// 本函数只负责单一 `max_slippage_bps` 边界：
/// 不读取 `MarketProtection`，不合并 `worst_price` / price band。
pub fn slippage_protection_price(
    reference: Price,
    side: Side,
    max_slippage_bps: u32,
    tick_size: i64,
) -> Result<Price, RejectReason> {
    if max_slippage_bps >= 10_000 {
        return Err(RejectReason::InvalidSlippageBps);
    }

    if tick_size <= 0 {
        return Err(RejectReason::InvalidTick);
    }

    let reference =
        u128::try_from(reference.get()).map_err(|_| RejectReason::ArithmeticOverflow)?;
    let tick = u128::try_from(tick_size).map_err(|_| RejectReason::ArithmeticOverflow)?;
    let bps = u128::from(max_slippage_bps);

    let aligned = match side {
        Side::Buy => {
            let factor = BPS_DENOMINATOR
                .checked_add(bps)
                .ok_or(RejectReason::ArithmeticOverflow)?;

            let numerator = reference
                .checked_mul(factor)
                .ok_or(RejectReason::ArithmeticOverflow)?;

            // Buy:
            // floor(reference * (10000 + bps) / 10000)
            let raw = numerator
                .checked_div(BPS_DENOMINATOR)
                .ok_or(RejectReason::ArithmeticOverflow)?;

            // 再向下对齐 tick。
            let tick_count = raw
                .checked_div(tick)
                .ok_or(RejectReason::ArithmeticOverflow)?;

            tick_count
                .checked_mul(tick)
                .ok_or(RejectReason::ArithmeticOverflow)?
        }

        Side::Sell => {
            let factor = BPS_DENOMINATOR
                .checked_sub(bps)
                .ok_or(RejectReason::ArithmeticOverflow)?;

            let numerator = reference
                .checked_mul(factor)
                .ok_or(RejectReason::ArithmeticOverflow)?;

            // Sell:
            // ceil(reference * (10000 - bps) / 10000)
            //
            // 禁止使用 `(a + d - 1) / d`，显式使用商和余数。
            let quotient = numerator
                .checked_div(BPS_DENOMINATOR)
                .ok_or(RejectReason::ArithmeticOverflow)?;
            let remainder = numerator
                .checked_rem(BPS_DENOMINATOR)
                .ok_or(RejectReason::ArithmeticOverflow)?;

            let raw = if remainder == 0 {
                quotient
            } else {
                quotient
                    .checked_add(1)
                    .ok_or(RejectReason::ArithmeticOverflow)?
            };

            // 再向上对齐 tick，同样只使用商和余数。
            let tick_quotient = raw
                .checked_div(tick)
                .ok_or(RejectReason::ArithmeticOverflow)?;
            let tick_remainder = raw
                .checked_rem(tick)
                .ok_or(RejectReason::ArithmeticOverflow)?;

            let tick_count = if tick_remainder == 0 {
                tick_quotient
            } else {
                tick_quotient
                    .checked_add(1)
                    .ok_or(RejectReason::ArithmeticOverflow)?
            };

            tick_count
                .checked_mul(tick)
                .ok_or(RejectReason::ArithmeticOverflow)?
        }
    };

    let aligned = i64::try_from(aligned).map_err(|_| RejectReason::ArithmeticOverflow)?;

    // reference 合法、bps/tick 合法后，如果结果无法构造合法 Price，
    // 对本纯计算边界统一视为“结果不可表示”。
    Price::try_new(aligned).map_err(|_| RejectReason::ArithmeticOverflow)
}

/// 市价按报价币预算执行后的纯结果值对象。
///
/// 保留成交事件的原始顺序；金额以非负 QuoteAmount 表示。
/// 不负责检查 spent_quote + unused_quote 与初始预算的关系。
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MarketByQuoteExecution {
    trades: Vec<TradeEvent>,
    spent_quote: QuoteAmount,
    unused_quote: QuoteAmount,
}

impl MarketByQuoteExecution {
    /// 保留按撮合顺序产生的 Trade 与已由执行器校验的报价金额。
    pub fn new(
        trades: Vec<TradeEvent>,
        spent_quote: QuoteAmount,
        unused_quote: QuoteAmount,
    ) -> Self {
        Self {
            trades,
            spent_quote,
            unused_quote,
        }
    }

    /// 按产生顺序借用成交事件。
    pub fn trades(&self) -> &[TradeEvent] {
        &self.trades
    }

    /// 返回已执行的 quote 本金；零表示没有成交。
    pub fn spent_quote(&self) -> QuoteAmount {
        self.spent_quote
    }

    /// 返回未使用的初始 quote 预算；该金额不是成交。
    pub fn unused_quote(&self) -> QuoteAmount {
        self.unused_quote
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn base(side: Side) -> MarketByBase {
        MarketByBase {
            order_id: OrderId::new(1),
            user_id: UserId::new(10),
            side,
            qty: Qty::try_new(100).unwrap(),
            market_protection: None,
        }
    }

    fn quote(side: Side) -> MarketByQuote {
        MarketByQuote {
            order_id: OrderId::new(2),
            user_id: UserId::new(20),
            side,
            quote_budget: QuoteBudget::try_new(1000).unwrap(),
            market_protection: None,
        }
    }

    fn price(value: i64) -> Price {
        Price::try_new(value).unwrap()
    }

    #[test]
    fn market_orders_support_both_sides_clone_and_debug() {
        for side in [Side::Buy, Side::Sell] {
            let base_order = base(side);
            let quote_order = quote(side);

            assert_eq!(base_order.clone(), base_order);
            assert_eq!(quote_order.clone(), quote_order);

            assert!(format!("{base_order:?}").contains("MarketByBase"));
            assert!(format!("{quote_order:?}").contains("MarketByQuote"));
        }
    }
    #[test]
    fn market_by_base_has_value_semantics() {
        let original = base(Side::Buy);
        let same = base(Side::Buy);

        assert_eq!(original, same);

        let mut changed = original.clone();
        changed.order_id = OrderId::new(99);
        assert_ne!(original, changed);

        let mut changed = original.clone();
        changed.user_id = UserId::new(99);
        assert_ne!(original, changed);

        let mut changed = original.clone();
        changed.side = Side::Sell;
        assert_ne!(original, changed);

        let mut changed = original.clone();
        changed.qty = Qty::try_new(200).unwrap();
        assert_ne!(original, changed);

        // 原始 fixture 明确没有保护。
        assert_eq!(original.market_protection, None);
        assert_eq!(original, same);

        // None 与 Some 不相等。
        let mut changed = original.clone();
        changed.market_protection = Some(MarketProtection {
            max_slippage_bps: Some(100),
            worst_price: Some(Price::try_new(105).unwrap()),
        });
        assert_ne!(original, changed);

        // 不同保护值不相等，其余订单字段保持一致。
        let mut other = changed.clone();
        other.market_protection = Some(MarketProtection {
            max_slippage_bps: Some(200),
            worst_price: Some(Price::try_new(105).unwrap()),
        });
        assert_ne!(changed, other);
    }
    #[test]
    fn market_by_quote_has_value_semantics() {
        let original = quote(Side::Buy);
        let same = quote(Side::Buy);

        assert_eq!(original, same);

        let mut changed = original.clone();
        changed.order_id = OrderId::new(99);
        assert_ne!(original, changed);

        let mut changed = original.clone();
        changed.user_id = UserId::new(99);
        assert_ne!(original, changed);

        let mut changed = original.clone();
        changed.side = Side::Sell;
        assert_ne!(original, changed);

        let mut changed = original.clone();
        changed.quote_budget = QuoteBudget::try_new(2000).unwrap();
        assert_ne!(original, changed);

        // 原始 fixture 明确没有保护。
        assert_eq!(original.market_protection, None);
        assert_eq!(original, same);

        // None 与 Some 不相等。
        let mut changed = original.clone();
        changed.market_protection = Some(MarketProtection {
            max_slippage_bps: Some(100),
            worst_price: Some(Price::try_new(105).unwrap()),
        });
        assert_ne!(original, changed);

        // 不同保护值不相等，其余订单字段保持一致。
        let mut other = changed.clone();
        other.market_protection = Some(MarketProtection {
            max_slippage_bps: Some(100),
            worst_price: Some(Price::try_new(110).unwrap()),
        });
        assert_ne!(changed, other);
    }
    #[test]
    fn market_by_quote_requires_positive_initial_budget() {
        assert!(QuoteBudget::try_new(0).is_err());

        let order = quote(Side::Buy);
        assert!(order.quote_budget.get() > 0);
    }

    #[test]
    fn market_protection_supports_all_optional_configurations() {
        // 1. 两字段均为 None。
        let empty = MarketProtection {
            max_slippage_bps: None,
            worst_price: None,
        };
        assert_eq!(empty.max_slippage_bps, None);
        assert_eq!(empty.worst_price, None);

        // 2. 仅 max_slippage_bps，包含 Some(0)。
        for bps in [0, 100, 500] {
            let bps_only = MarketProtection {
                max_slippage_bps: Some(bps),
                worst_price: None,
            };
            assert_eq!(bps_only.max_slippage_bps, Some(bps));
            assert_eq!(bps_only.worst_price, None);
        }

        // 3. 仅 worst_price。
        let price = Price::try_new(105).unwrap();
        let price_only = MarketProtection {
            max_slippage_bps: None,
            worst_price: Some(price),
        };
        assert_eq!(price_only.max_slippage_bps, None);
        assert_eq!(price_only.worst_price, Some(price));

        // 4. 两字段同时存在。
        let complete = MarketProtection {
            max_slippage_bps: Some(100),
            worst_price: Some(price),
        };
        assert_eq!(complete.max_slippage_bps, Some(100));
        assert_eq!(complete.worst_price, Some(price));
    }

    #[test]
    fn market_protection_has_value_semantics_clone_and_debug() {
        let original = MarketProtection {
            max_slippage_bps: Some(100),
            worst_price: Some(Price::try_new(105).unwrap()),
        };

        // Clone 后相等。
        assert_eq!(original.clone(), original);

        // Debug 可用。
        assert!(format!("{original:?}").contains("MarketProtection"));

        // 修改 bps 后不相等。
        let mut changed = original.clone();
        changed.max_slippage_bps = Some(200);
        assert_ne!(original, changed);

        // 修改 worst_price 后不相等。
        let mut changed = original.clone();
        changed.worst_price = Some(Price::try_new(110).unwrap());
        assert_ne!(original, changed);

        // bps: Some -> None 后不相等。
        let mut changed = original.clone();
        changed.max_slippage_bps = None;
        assert_ne!(original, changed);

        // worst_price: Some -> None 后不相等。
        let mut changed = original.clone();
        changed.worst_price = None;
        assert_ne!(original, changed);
    }

    fn protection_price(
        reference: i64,
        side: Side,
        bps: u32,
        tick: i64,
    ) -> Result<Price, RejectReason> {
        slippage_protection_price(Price::try_new(reference).unwrap(), side, bps, tick)
    }

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
        let empty =
            MarketByQuoteExecution::new(Vec::new(), QuoteAmount::new(0), QuoteAmount::new(0));
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

        // 仅改变 events。
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
        let changed_unused = ProtectedMarketByQuoteExecution::new(
            events,
            QuoteAmount::new(315),
            QuoteAmount::new(8),
        );

        assert_ne!(original, changed_unused);
    }

    #[test]
    fn slippage_protection_price_applies_directional_rounding() {
        assert_eq!(
            protection_price(100_003, Side::Buy, 25, 10),
            Ok(Price::try_new(100_250).unwrap()),
        );

        assert_eq!(
            protection_price(100_003, Side::Sell, 25, 10),
            Ok(Price::try_new(99_760).unwrap()),
        );
    }

    #[test]
    fn slippage_protection_price_zero_bps_still_aligns_directionally() {
        assert_eq!(
            protection_price(100_003, Side::Buy, 0, 10),
            Ok(Price::try_new(100_000).unwrap()),
        );

        assert_eq!(
            protection_price(100_003, Side::Sell, 0, 10),
            Ok(Price::try_new(100_010).unwrap()),
        );
    }

    #[test]
    fn slippage_protection_price_accepts_maximum_valid_bps() {
        assert_eq!(
            protection_price(100_003, Side::Buy, 9_999, 10),
            Ok(Price::try_new(199_990).unwrap()),
        );

        assert_eq!(
            protection_price(100_003, Side::Sell, 9_999, 10),
            Ok(Price::try_new(20).unwrap()),
        );
    }

    #[test]
    fn slippage_protection_price_rejects_bps_at_or_above_denominator() {
        for bps in [10_000, 10_001, u32::MAX] {
            assert_eq!(
                protection_price(100_003, Side::Buy, bps, 10),
                Err(RejectReason::InvalidSlippageBps),
            );

            assert_eq!(
                protection_price(100_003, Side::Sell, bps, 10),
                Err(RejectReason::InvalidSlippageBps),
            );
        }
    }

    #[test]
    fn slippage_protection_price_rejects_non_positive_tick() {
        for tick in [0, -1, -10, i64::MIN] {
            assert_eq!(
                protection_price(100_003, Side::Buy, 25, tick),
                Err(RejectReason::InvalidTick),
            );

            assert_eq!(
                protection_price(100_003, Side::Sell, 25, tick),
                Err(RejectReason::InvalidTick),
            );
        }
    }

    #[test]
    fn slippage_protection_price_zero_bps_preserves_aligned_reference() {
        let expected = Price::try_new(100_000).unwrap();

        assert_eq!(protection_price(100_000, Side::Buy, 0, 10), Ok(expected),);

        assert_eq!(protection_price(100_000, Side::Sell, 0, 10), Ok(expected),);
    }

    #[test]
    fn slippage_protection_price_rejects_buy_result_above_price_range() {
        assert_eq!(
            protection_price(i64::MAX, Side::Buy, 1, 1),
            Err(RejectReason::ArithmeticOverflow),
        );
    }

    #[test]
    fn slippage_protection_price_rejects_sell_tick_ceil_above_price_range() {
        assert_eq!(
            protection_price(i64::MAX, Side::Sell, 0, 10),
            Err(RejectReason::ArithmeticOverflow),
        );
    }

    #[test]
    fn user_protection_price_returns_none_when_no_user_protection_is_configured() {
        let protection = MarketProtection {
            max_slippage_bps: None,
            worst_price: None,
        };

        assert_eq!(
            protection.user_protection_price(price(100_003), Side::Buy, 10,),
            Ok(None),
        );

        assert_eq!(
            protection.user_protection_price(price(100_003), Side::Sell, 10,),
            Ok(None),
        );

        // 没有任何候选时，不应为了 tick_size 人为制造错误。
        assert_eq!(
            protection.user_protection_price(price(100_003), Side::Buy, 0,),
            Ok(None),
        );
    }

    #[test]
    fn user_protection_price_uses_existing_bps_helper_when_only_bps_is_present() {
        let protection = MarketProtection {
            max_slippage_bps: Some(25),
            worst_price: None,
        };

        let expected_buy = price(100_250);
        let expected_sell = price(99_760);

        // 固定值先锁协议。
        assert_eq!(
            slippage_protection_price(price(100_003), Side::Buy, 25, 10,),
            Ok(expected_buy),
        );

        assert_eq!(
            slippage_protection_price(price(100_003), Side::Sell, 25, 10,),
            Ok(expected_sell),
        );

        assert_eq!(
            protection.user_protection_price(price(100_003), Side::Buy, 10,),
            Ok(Some(expected_buy)),
        );

        assert_eq!(
            protection.user_protection_price(price(100_003), Side::Sell, 10,),
            Ok(Some(expected_sell)),
        );
    }

    #[test]
    fn user_protection_price_returns_aligned_explicit_worst_price_unchanged() {
        let explicit = price(100_200);

        let protection = MarketProtection {
            max_slippage_bps: None,
            worst_price: Some(explicit),
        };

        assert_eq!(
            protection.user_protection_price(price(100_003), Side::Buy, 10,),
            Ok(Some(explicit)),
        );

        assert_eq!(
            protection.user_protection_price(price(100_003), Side::Sell, 10,),
            Ok(Some(explicit)),
        );
    }

    #[test]
    fn user_protection_price_buy_chooses_stricter_lower_candidate() {
        // 25bps helper:
        // reference=100_003, tick=10
        // => Buy bps candidate = 100_250
        //
        // explicit=100_200 更严格。
        let protection = MarketProtection {
            max_slippage_bps: Some(25),
            worst_price: Some(price(100_200)),
        };

        assert_eq!(
            protection.user_protection_price(price(100_003), Side::Buy, 10,),
            Ok(Some(price(100_200))),
        );
    }

    #[test]
    fn user_protection_price_sell_chooses_stricter_higher_candidate() {
        // 25bps helper:
        // reference=100_003, tick=10
        // => Sell bps candidate = 99_760
        //
        // explicit=99_800 更严格。
        let protection = MarketProtection {
            max_slippage_bps: Some(25),
            worst_price: Some(price(99_800)),
        };

        assert_eq!(
            protection.user_protection_price(price(100_003), Side::Sell, 10,),
            Ok(Some(price(99_800))),
        );
    }

    #[test]
    fn user_protection_price_rejects_unaligned_explicit_worst_price() {
        let protection = MarketProtection {
            max_slippage_bps: None,
            worst_price: Some(price(100_003)),
        };

        assert_eq!(
            protection.user_protection_price(price(100_000), Side::Buy, 10,),
            Err(RejectReason::InvalidTick),
        );

        assert_eq!(
            protection.user_protection_price(price(100_000), Side::Sell, 10,),
            Err(RejectReason::InvalidTick),
        );
    }

    #[test]
    fn user_protection_price_prioritizes_invalid_bps_over_explicit_tick_error() {
        let protection = MarketProtection {
            max_slippage_bps: Some(10_000),
            // 同时故意未对齐 tick。
            worst_price: Some(price(100_003)),
        };

        assert_eq!(
            protection.user_protection_price(price(100_000), Side::Buy, 10,),
            Err(RejectReason::InvalidSlippageBps),
        );

        assert_eq!(
            protection.user_protection_price(price(100_000), Side::Sell, 10,),
            Err(RejectReason::InvalidSlippageBps),
        );
    }

    #[test]
    fn user_protection_price_rejects_non_positive_tick_for_explicit_worst_price() {
        let protection = MarketProtection {
            max_slippage_bps: None,
            worst_price: Some(price(100_000)),
        };

        for tick in [0, -1, -10, i64::MIN] {
            assert_eq!(
                protection.user_protection_price(price(100_003), Side::Buy, tick,),
                Err(RejectReason::InvalidTick),
            );

            assert_eq!(
                protection.user_protection_price(price(100_003), Side::Sell, tick,),
                Err(RejectReason::InvalidTick),
            );
        }
    }

    #[test]
    fn maker_price_within_protection_allows_any_price_without_boundary() {
        assert!(maker_price_within_protection(Side::Buy, price(1), None,));
        assert!(maker_price_within_protection(
            Side::Buy,
            price(999_999),
            None,
        ));

        assert!(maker_price_within_protection(Side::Sell, price(1), None,));
        assert!(maker_price_within_protection(
            Side::Sell,
            price(999_999),
            None,
        ));
    }

    #[test]
    fn maker_price_within_protection_buy_uses_inclusive_upper_boundary() {
        let boundary = Some(price(100_000));

        // 内侧：低于 Buy 最大允许价格。
        assert!(maker_price_within_protection(
            Side::Buy,
            price(99_990),
            boundary,
        ));

        // 相等：边界具有包容性。
        assert!(maker_price_within_protection(
            Side::Buy,
            price(100_000),
            boundary,
        ));

        // 越界：高于 Buy 最大允许价格。
        assert!(!maker_price_within_protection(
            Side::Buy,
            price(100_010),
            boundary,
        ));
    }

    #[test]
    fn maker_price_within_protection_sell_uses_inclusive_lower_boundary() {
        let boundary = Some(price(100_000));

        // 内侧：高于 Sell 最低允许价格。
        assert!(maker_price_within_protection(
            Side::Sell,
            price(100_010),
            boundary,
        ));

        // 相等：边界具有包容性。
        assert!(maker_price_within_protection(
            Side::Sell,
            price(100_000),
            boundary,
        ));

        // 越界：低于 Sell 最低允许价格。
        assert!(!maker_price_within_protection(
            Side::Sell,
            price(99_990),
            boundary,
        ));
    }

    #[test]
    fn maker_price_within_protection_handles_price_domain_boundaries() {
        let min = price(1);
        let max = price(i64::MAX);

        assert!(maker_price_within_protection(Side::Buy, min, Some(max),));
        assert!(maker_price_within_protection(Side::Buy, max, Some(max),));

        assert!(maker_price_within_protection(Side::Sell, max, Some(min),));
        assert!(maker_price_within_protection(Side::Sell, min, Some(min),));
    }

    #[test]
    fn effective_protection_price_merges_optional_candidates() {
        let p100 = Price::try_new(100).unwrap();
        let p105 = Price::try_new(105).unwrap();
        let p110 = Price::try_new(110).unwrap();

        for side in [Side::Buy, Side::Sell] {
            // 两个候选都不存在。
            assert_eq!(effective_protection_price(side, None, None), None);

            // 仅有用户候选。
            assert_eq!(
                effective_protection_price(side, Some(p100), None),
                Some(p100)
            );

            // 仅有 Market Band 候选。
            assert_eq!(
                effective_protection_price(side, None, Some(p105)),
                Some(p105)
            );

            // 两值相等，不因来源改变结果。
            assert_eq!(
                effective_protection_price(side, Some(p105), Some(p105)),
                Some(p105)
            );
        }

        // Buy：取较小值，与候选参数位置无关。
        assert_eq!(
            effective_protection_price(Side::Buy, Some(p100), Some(p110)),
            Some(p100)
        );

        assert_eq!(
            effective_protection_price(Side::Buy, Some(p110), Some(p100)),
            Some(p100)
        );

        // Sell：取较大值，与候选参数位置无关。
        assert_eq!(
            effective_protection_price(Side::Sell, Some(p100), Some(p110)),
            Some(p110)
        );

        assert_eq!(
            effective_protection_price(Side::Sell, Some(p110), Some(p100)),
            Some(p110)
        );
    }
    #[test]
    fn effective_protection_at_command_start_composes_candidates() {
        let math = MarketMath::try_new(0, 0, 0, 10, 1).unwrap();
        let reference = Price::try_new(100_003).unwrap();

        let bps_only = MarketProtection {
            max_slippage_bps: Some(100),
            worst_price: None,
        };

        let explicit_only = MarketProtection {
            max_slippage_bps: None,
            worst_price: Some(Price::try_new(100_050).unwrap()),
        };

        let band = Price::try_new(100_000).unwrap();

        let no_band_context = CommandStartProtectionContext {
            reference_price_at_command_start: reference,
            market_band_worst: None,
        };

        let band_context = CommandStartProtectionContext {
            reference_price_at_command_start: reference,
            market_band_worst: Some(band),
        };

        for side in [Side::Buy, Side::Sell] {
            // 1. 用户和 band 均不存在。
            assert_eq!(
                effective_protection_at_command_start(None, &no_band_context, side, &math,),
                Ok(None)
            );

            // 2. 仅有 band，原样返回。
            assert_eq!(
                effective_protection_at_command_start(None, &band_context, side, &math,),
                Ok(Some(band))
            );

            // 3. 仅有用户 bps。
            // 期望由已验收 helper 产生，不复制 bps 公式。
            let expected_user = bps_only
                .user_protection_price(
                    no_band_context.reference_price_at_command_start,
                    side,
                    math.tick_size(),
                )
                .unwrap();

            assert!(expected_user.is_some());

            assert_eq!(
                effective_protection_at_command_start(
                    Some(&bps_only),
                    &no_band_context,
                    side,
                    &math
                ),
                Ok(expected_user)
            );
        }

        let lower_band_context = CommandStartProtectionContext {
            reference_price_at_command_start: reference,
            market_band_worst: Some(Price::try_new(100_040).unwrap()),
        };

        let upper_band_context = CommandStartProtectionContext {
            reference_price_at_command_start: reference,
            market_band_worst: Some(Price::try_new(100_060).unwrap()),
        };

        // Buy：band 更严格。
        assert_eq!(
            effective_protection_at_command_start(
                Some(&explicit_only),
                &lower_band_context,
                Side::Buy,
                &math,
            ),
            Ok(Some(Price::try_new(100_040).unwrap()))
        );

        // Buy：用户更严格。
        assert_eq!(
            effective_protection_at_command_start(
                Some(&explicit_only),
                &upper_band_context,
                Side::Buy,
                &math,
            ),
            Ok(Some(Price::try_new(100_050).unwrap()))
        );

        // Sell：band 更严格。
        assert_eq!(
            effective_protection_at_command_start(
                Some(&explicit_only),
                &upper_band_context,
                Side::Sell,
                &math,
            ),
            Ok(Some(Price::try_new(100_060).unwrap()))
        );

        // Sell：用户更严格。
        assert_eq!(
            effective_protection_at_command_start(
                Some(&explicit_only),
                &lower_band_context,
                Side::Sell,
                &math,
            ),
            Ok(Some(Price::try_new(100_050).unwrap()))
        );
    }

    #[test]
    fn effective_protection_at_command_start_propagates_user_errors() {
        let math = MarketMath::try_new(0, 0, 0, 10, 1).unwrap();
        let reference = Price::try_new(100_003).unwrap();
        let band = Some(Price::try_new(100_000).unwrap());
        let context = CommandStartProtectionContext {
            reference_price_at_command_start: reference,
            market_band_worst: band,
        };

        // 非法 bps。
        let invalid_bps = MarketProtection {
            max_slippage_bps: Some(u32::MAX),
            worst_price: None,
        };

        // 显式价格不符合 tick=10。
        let invalid_explicit = MarketProtection {
            max_slippage_bps: None,
            worst_price: Some(Price::try_new(100_003).unwrap()),
        };

        for side in [Side::Buy, Side::Sell] {
            // 1. 已验收用户 helper 必须返回 3007。
            let expected_error = invalid_bps
                .user_protection_price(
                    context.reference_price_at_command_start,
                    side,
                    math.tick_size(),
                )
                .unwrap_err();

            assert_eq!(expected_error as u16, 3007);

            // 即使 band 有效，也必须原样传播用户错误。
            assert_eq!(
                effective_protection_at_command_start(Some(&invalid_bps), &context, side, &math,),
                Err(expected_error)
            );

            // 2. 显式价格未对齐：传播既有错误。
            let expected_error = invalid_explicit
                .user_protection_price(
                    context.reference_price_at_command_start,
                    side,
                    math.tick_size(),
                )
                .unwrap_err();

            assert_eq!(
                effective_protection_at_command_start(
                    Some(&invalid_explicit),
                    &context,
                    side,
                    &math,
                ),
                Err(expected_error)
            );
        }
    }
    #[test]
    fn command_start_protection_context_has_value_semantics() {
        let reference = Price::try_new(100_003).unwrap();
        let band = Price::try_new(100_010).unwrap();

        // 1. 相同 reference，band=None。
        let without_band = CommandStartProtectionContext {
            reference_price_at_command_start: reference,
            market_band_worst: None,
        };

        // 2. 相同 reference，band=Some。
        let with_band = CommandStartProtectionContext {
            reference_price_at_command_start: reference,
            market_band_worst: Some(band),
        };

        // 字段必须原样保留。
        assert_eq!(without_band.reference_price_at_command_start, reference);
        assert_eq!(without_band.market_band_worst, None);

        assert_eq!(with_band.reference_price_at_command_start, reference);
        assert_eq!(with_band.market_band_worst, Some(band));

        // Clone 后相等。
        assert_eq!(without_band.clone(), without_band);
        assert_eq!(with_band.clone(), with_band);

        // Debug 可用。
        let debug = format!("{with_band:?}");
        assert!(debug.contains("CommandStartProtectionContext"));
        assert!(debug.contains("reference_price_at_command_start"));
        assert!(debug.contains("market_band_worst"));

        // 相同字段值的独立对象相等。
        let same = CommandStartProtectionContext {
            reference_price_at_command_start: Price::try_new(100_003).unwrap(),
            market_band_worst: Some(Price::try_new(100_010).unwrap()),
        };
        assert_eq!(with_band, same);

        // 相同 reference，但 None 与 Some 不相等。
        assert_ne!(without_band, with_band);

        // 仅改变 reference，不相等。
        let changed_reference = CommandStartProtectionContext {
            reference_price_at_command_start: Price::try_new(100_013).unwrap(),
            market_band_worst: Some(band),
        };
        assert_ne!(with_band, changed_reference);

        // 仅改变 band，不相等。
        let changed_band = CommandStartProtectionContext {
            reference_price_at_command_start: reference,
            market_band_worst: Some(Price::try_new(100_020).unwrap()),
        };
        assert_ne!(with_band, changed_band);
    }
    #[test]
    fn protection_source_has_stable_u8_wire_codes() {
        let golden = [
            (ProtectionSource::User, 1u8),
            (ProtectionSource::MarketBand, 2u8),
            (ProtectionSource::Both, 3u8),
        ];

        // 明确固定单字节表示。
        assert_eq!(std::mem::size_of::<ProtectionSource>(), 1);

        // Golden Wire：数值必须与协议规格完全一致。
        for (source, expected) in golden {
            assert_eq!(source as u8, expected);
            assert_eq!([source as u8], [expected]);

            // Copy / Clone / Debug / Eq。
            let copied = source;
            assert_eq!(copied, source);
            assert_eq!(source.clone(), source);
            assert!(!format!("{source:?}").is_empty());
        }

        assert_ne!(ProtectionSource::User, ProtectionSource::MarketBand);
        assert_ne!(ProtectionSource::User, ProtectionSource::Both);
        assert_ne!(ProtectionSource::MarketBand, ProtectionSource::Both);
    }
    #[test]
    fn protection_source_is_exported_from_crate_root() {
        let source: crate::ProtectionSource = ProtectionSource::Both;

        assert_eq!(source as u8, 3);
    }
    #[test]
    fn effective_protection_with_source_has_stable_attribution() {
        let p = |value| Price::try_new(value).unwrap();

        // 预期独立固定，不从被测函数生成。
        let cases = [
            // 无候选。
            (Side::Buy, None, None, None),
            (Side::Sell, None, None, None),
            // 只有用户候选。
            (
                Side::Buy,
                Some(p(100)),
                None,
                Some((p(100), ProtectionSource::User)),
            ),
            (
                Side::Sell,
                Some(p(100)),
                None,
                Some((p(100), ProtectionSource::User)),
            ),
            // 只有 market band。
            (
                Side::Buy,
                None,
                Some(p(110)),
                Some((p(110), ProtectionSource::MarketBand)),
            ),
            (
                Side::Sell,
                None,
                Some(p(110)),
                Some((p(110), ProtectionSource::MarketBand)),
            ),
            // Buy：较小值更严格。
            (
                Side::Buy,
                Some(p(100)),
                Some(p(110)),
                Some((p(100), ProtectionSource::User)),
            ),
            (
                Side::Buy,
                Some(p(110)),
                Some(p(100)),
                Some((p(100), ProtectionSource::MarketBand)),
            ),
            // Sell：较大值更严格。
            (
                Side::Sell,
                Some(p(110)),
                Some(p(100)),
                Some((p(110), ProtectionSource::User)),
            ),
            (
                Side::Sell,
                Some(p(100)),
                Some(p(110)),
                Some((p(110), ProtectionSource::MarketBand)),
            ),
            // 边界相等：两项共同决定有效边界。
            (
                Side::Buy,
                Some(p(100)),
                Some(p(100)),
                Some((p(100), ProtectionSource::Both)),
            ),
            (
                Side::Sell,
                Some(p(100)),
                Some(p(100)),
                Some((p(100), ProtectionSource::Both)),
            ),
        ];

        for (side, user, band, expected) in cases {
            let actual = effective_protection_with_source(side, user, band);

            assert_eq!(
                actual, expected,
                "side={side:?}, user={user:?}, band={band:?}"
            );

            // 新 helper 与已有纯合并函数的有效价格必须一致。
            assert_eq!(
                actual.map(|(price, _)| price),
                effective_protection_price(side, user, band),
            );
        }
    }

    #[test]
    fn command_start_with_source_preserves_candidate_attribution() {
        let p = |value| Price::try_new(value).unwrap();
        let math = MarketMath::try_new(0, 0, 0, 10, 1).unwrap();

        let cases = [
            // 两方均无候选。
            (Side::Buy, None, None, None),
            (Side::Sell, None, None, None),
            // 仅用户候选。
            (
                Side::Buy,
                Some(100),
                None,
                Some((100, ProtectionSource::User)),
            ),
            (
                Side::Sell,
                Some(100),
                None,
                Some((100, ProtectionSource::User)),
            ),
            // 仅市场 Band。
            (
                Side::Buy,
                None,
                Some(110),
                Some((110, ProtectionSource::MarketBand)),
            ),
            (
                Side::Sell,
                None,
                Some(110),
                Some((110, ProtectionSource::MarketBand)),
            ),
            // Buy：较小价格更严格。
            (
                Side::Buy,
                Some(100),
                Some(110),
                Some((100, ProtectionSource::User)),
            ),
            (
                Side::Buy,
                Some(110),
                Some(100),
                Some((100, ProtectionSource::MarketBand)),
            ),
            // Sell：较大价格更严格。
            (
                Side::Sell,
                Some(110),
                Some(100),
                Some((110, ProtectionSource::User)),
            ),
            (
                Side::Sell,
                Some(100),
                Some(110),
                Some((110, ProtectionSource::MarketBand)),
            ),
            // 候选价格相等：Both。
            (
                Side::Buy,
                Some(100),
                Some(100),
                Some((100, ProtectionSource::Both)),
            ),
            (
                Side::Sell,
                Some(100),
                Some(100),
                Some((100, ProtectionSource::Both)),
            ),
        ];

        for (side, user, band, expected) in cases {
            let protection = user.map(|price| MarketProtection {
                max_slippage_bps: None,
                worst_price: Some(p(price)),
            });

            let context = CommandStartProtectionContext {
                reference_price_at_command_start: p(100_000),
                market_band_worst: band.map(p),
            };

            let expected = expected.map(|(price, source)| (p(price), source));

            let attributed = effective_protection_at_command_start_with_source(
                protection.as_ref(),
                &context,
                side,
                &math,
            )
            .unwrap();

            assert_eq!(attributed, expected);

            // 旧接口严格等于新接口的价格投影。
            assert_eq!(
                effective_protection_at_command_start(protection.as_ref(), &context, side, &math,),
                Ok(expected.map(|(price, _)| price))
            );
        }
    }

    #[test]
    fn command_start_with_source_reuses_user_bps_calculation() {
        let p = |value| Price::try_new(value).unwrap();
        let math = MarketMath::try_new(0, 0, 0, 10, 1).unwrap();

        let protection = MarketProtection {
            max_slippage_bps: Some(100),
            worst_price: None,
        };

        // reference=100000，滑点 100bps：
        // Buy 用户边界=101000，Sell 用户边界=99000。
        let cases = [
            (Side::Buy, None, 101_000, ProtectionSource::User),
            (Side::Sell, None, 99_000, ProtectionSource::User),
            (
                Side::Buy,
                Some(100_900),
                100_900,
                ProtectionSource::MarketBand,
            ),
            (
                Side::Sell,
                Some(99_100),
                99_100,
                ProtectionSource::MarketBand,
            ),
            (Side::Buy, Some(101_000), 101_000, ProtectionSource::Both),
            (Side::Sell, Some(99_000), 99_000, ProtectionSource::Both),
        ];

        for (side, band, expected_price, expected_source) in cases {
            let context = CommandStartProtectionContext {
                reference_price_at_command_start: p(100_000),
                market_band_worst: band.map(p),
            };

            assert_eq!(
                effective_protection_at_command_start_with_source(
                    Some(&protection),
                    &context,
                    side,
                    &math,
                ),
                Ok(Some((p(expected_price), expected_source)))
            );

            assert_eq!(
                effective_protection_at_command_start(Some(&protection), &context, side, &math,),
                Ok(Some(p(expected_price)))
            );
        }
    }

    #[test]
    fn command_start_with_source_propagates_original_errors() {
        let p = |value| Price::try_new(value).unwrap();
        let math = MarketMath::try_new(0, 0, 0, 10, 1).unwrap();

        let cases = [
            // 非法 bps。
            (
                Side::Buy,
                100_000,
                MarketProtection {
                    max_slippage_bps: Some(u32::MAX),
                    worst_price: None,
                },
            ),
            // 显式价格不符合 tick=10。
            (
                Side::Sell,
                100_000,
                MarketProtection {
                    max_slippage_bps: None,
                    worst_price: Some(p(100_003)),
                },
            ),
            // Buy 边界计算 checked overflow。
            (
                Side::Buy,
                i64::MAX,
                MarketProtection {
                    max_slippage_bps: Some(100),
                    worst_price: None,
                },
            ),
        ];

        for (index, (side, reference, protection)) in cases.into_iter().enumerate() {
            let context = CommandStartProtectionContext {
                reference_price_at_command_start: p(reference),
                market_band_worst: Some(p(100_000)),
            };

            // 以已验收的用户保护函数作为错误来源。
            let expected_error = protection
                .user_protection_price(
                    context.reference_price_at_command_start,
                    side,
                    math.tick_size(),
                )
                .unwrap_err();

            if index == 0 {
                assert_eq!(expected_error as u16, 3007);
            }

            if index == 2 {
                assert_eq!(expected_error, RejectReason::ArithmeticOverflow);
            }

            assert_eq!(
                effective_protection_at_command_start_with_source(
                    Some(&protection),
                    &context,
                    side,
                    &math,
                ),
                Err(expected_error)
            );

            assert_eq!(
                effective_protection_at_command_start(Some(&protection), &context, side, &math,),
                Err(expected_error)
            );
        }
    }
}
