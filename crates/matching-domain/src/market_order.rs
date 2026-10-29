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
    /// 调用方提供的订单唯一标识符。
    pub order_id: OrderId,
    /// 订单所属用户标识符。
    pub user_id: UserId,
    /// 账户层固定的自成交防护分组身份。
    pub stp_key: crate::StpKey,
    /// 本市价订单作为吃单时决定冲突处理的策略。
    pub stp_mode: crate::StpMode,
    /// 买入或卖出的撮合方向。
    pub side: Side,
    /// 请求成交的严格正基础资产数量。
    pub qty: Qty,
    /// 可选的用户价格保护输入。
    pub market_protection: Option<MarketProtection>,
}

/// 基础数量市价单的有序业务事件及成交投影。
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MarketByBaseExecution {
    /// 完整保留撮合和自成交取消原序的业务事件。
    events: Vec<EngineEvent>,
    /// 从业务事件中按原序提取的真实成交。
    trades: Vec<TradeEvent>,
}

/// 可保留完整生命周期事件及成交投影的限价 GTC 执行结果。
pub type LimitGtcExecution = MarketByBaseExecution;

impl MarketByBaseExecution {
    /// 从已有原序业务事件构造结果；不会产生新的撮合行为。
    pub fn from_events(events: Vec<EngineEvent>) -> Self {
        let trades = events
            .iter()
            .filter_map(|event| match event {
                EngineEvent::Trade(trade) => Some(trade.clone()),
                _ => None,
            })
            .collect();
        Self { events, trades }
    }

    /// 按原序返回成交及取消等完整业务事件。
    pub fn events(&self) -> &[EngineEvent] {
        &self.events
    }

    /// 按原序返回真实成交投影。
    pub fn trades(&self) -> &[TradeEvent] {
        &self.trades
    }
}

impl std::ops::Deref for MarketByBaseExecution {
    type Target = [TradeEvent];
    fn deref(&self) -> &Self::Target {
        &self.trades
    }
}

impl IntoIterator for MarketByBaseExecution {
    type Item = TradeEvent;
    type IntoIter = std::vec::IntoIter<TradeEvent>;
    fn into_iter(self) -> Self::IntoIter {
        self.trades.into_iter()
    }
}

impl<'a> IntoIterator for &'a MarketByBaseExecution {
    type Item = &'a TradeEvent;
    type IntoIter = std::slice::Iter<'a, TradeEvent>;
    fn into_iter(self) -> Self::IntoIter {
        self.trades.iter()
    }
}

/// 按报价币预算提交的市价订单输入。
///
/// quote_budget 是严格正的初始预算，不含手续费。
/// 剩余预算属于后续撮合运行态，不保存在该输入对象中。
/// 普通执行入口不解释 market_protection；带命令开始保护上下文的执行入口
/// 使用它冻结有效边界并可产生保护终结事件。
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MarketByQuote {
    /// 调用方提供的订单唯一标识符。
    pub order_id: OrderId,
    /// 订单所属用户标识符。
    pub user_id: UserId,
    /// 账户层固定的自成交防护分组身份。
    pub stp_key: crate::StpKey,
    /// 本市价订单作为吃单时决定冲突处理的策略。
    pub stp_mode: crate::StpMode,
    /// 买入或卖出的撮合方向。
    pub side: Side,
    /// 不含手续费的严格正初始报价资产预算。
    pub quote_budget: QuoteBudget,
    /// 可选的用户价格保护输入。
    pub market_protection: Option<MarketProtection>,
}

impl MarketByBase {
    /// 在扫描或修改可执行流动性前验证自成交模式可以在当前阶段执行。
    pub fn validate_stp_mode(&self) -> Result<(), RejectReason> {
        self.stp_mode.submission_rejection().map_or(Ok(()), Err)
    }
}

impl MarketByQuote {
    /// 在扫描或修改可执行流动性前验证自成交模式可以在当前阶段执行。
    pub fn validate_stp_mode(&self) -> Result<(), RejectReason> {
        self.stp_mode.submission_rejection().map_or(Ok(()), Err)
    }
}

/// 市价命令可选的用户保护参数。
///
/// 该值对象仅承载输入配置；有效保护边界须在命令开始捕获参考价后由后续撮合路径计算。
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MarketProtection {
    /// 以基点表示的可选最大滑点上限。
    pub max_slippage_bps: Option<u32>,
    /// 用户指定的可选最差可成交价格。
    pub worst_price: Option<Price>,
}

/// 市价保护触发事件的来源标签。
///
/// 线协议编码固定为 u8：
/// - 1：用户边界 `User`
/// - 2：市场价格区间 `MarketBand`
/// - 3：双方共同约束 `Both`
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
/// 命令开始时的参考价格 `reference_price_at_command_start`：
///   由外层在命令开始时捕获的严格正参考价格。
///
/// 市场价格区间边界 `market_band_worst`：
///   由外层根据市场配置预先计算完成的可选价格带边界。
///
/// 本对象仅保存不可变值语义，不负责：
/// - 选择或更新参考值来源；
/// - 计算市场价格区间；
/// - 校验用户保护参数；
/// - 判断是否必须要求保护；
/// - 执行撮合或产生事件。
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CommandStartProtectionContext {
    /// 命令开始时已冻结的参考价格。
    pub reference_price_at_command_start: Price,
    /// 市场价格区间计算出的可选最差边界。
    pub market_band_worst: Option<Price>,
}

impl MarketProtection {
    /// 计算用户侧最终保护候选价。
    ///
    /// 只合并：
    /// - max_slippage_bps 派生出的保护价；
    /// - 显式 worst_price。
    ///
    /// 不处理市场价格区间、参考值状态、挂单价格或任何撮合副作用。
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

        // 1. 基点候选必须优先计算。
        //
        // 因此：
        // - 基点 >= 10_000 仍稳定优先返回 InvalidSlippageBps(3007)；
        // - 价格步长 / 溢出完全复用已验收辅助函数的语义；
        // - 此处绝不复制基点公式。
        let bps_price = match self.max_slippage_bps {
            Some(bps) => Some(slippage_protection_price(reference, side, bps, tick_size)?),
            None => None,
        };

        // 2. 显式最差价格单独验证价格步长。
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
                    // 买入越低越严格。
                    Some(if bps <= explicit { bps } else { explicit })
                } else {
                    // 卖出越高越严格。
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
    /// 按撮合顺序产生的成交与保护终结事件。
    events: Vec<EngineEvent>,
    /// 已实际使用的报价资产本金。
    spent_quote: QuoteAmount,
    /// 未使用且不构成成交的报价资产金额。
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

    /// 返回已执行的报价资产本金；零表示没有成交。
    pub fn spent_quote(&self) -> QuoteAmount {
        self.spent_quote
    }

    /// 返回未使用的初始报价资产预算；该金额不是成交。
    pub fn unused_quote(&self) -> QuoteAmount {
        self.unused_quote
    }
}

/// 判断挂单价格是否位于已经计算好的用户保护边界内。
///
/// `effective_worst_price` 必须由上游提前计算完成：
/// - 买入: maker_price <= 边界时允许；
/// - 卖出: maker_price >= 边界时允许；
/// - None: 当前命令没有有效保护边界，此项价格检查允许。
///
/// 本函数不负责：
/// - 计算滑点基点；
/// - 校验价格步长；
/// - 合并显式最差价格；
/// - 获取参考值；
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
/// 买入取更低价格，卖出取更高价格。
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
/// 买入取较小边界，卖出取较大边界。
///
/// 来源表示哪个候选决定最终有效边界：
/// - 仅用户候选生效：User
/// - 仅市场价格区间候选生效：MarketBand
/// - 两个候选边界相等：Both
///
/// 不计算基点/价格步长，不获取参考值或市场状态，
/// 不判断挂单是否越界，也不产生事件。
pub fn effective_protection_with_source(
    side: Side,
    user_worst: Option<Price>,
    market_band_worst: Option<Price>,
) -> Option<(Price, ProtectionSource)> {
    // 完全复用已验收的买入/卖出取严逻辑。
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

/// 在命令开始时组合用户保护候选与已计算的市场价格区间候选。
///
/// reference_price_at_command_start 由调用方提前捕获。
/// 算术提供已验证的 tick_size。
/// market_band_worst 由外部配置/市场参考价计算层提供并完成价格步长校验；
/// 本函数不会再次验证市场价格区间对齐。
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
/// 不获取市场状态，不更新上下文，不判断挂单是否越界。
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

/// 根据命令开始时捕获的参考值价格计算用户滑点保护边界。
///
/// 这是纯整数计算：
/// - 买入: 先按基点向下取整，再向下对齐价格步长；
/// - 卖出: 先按基点向上取整，再向上对齐价格步长。
///
/// 本函数只负责单一 `max_slippage_bps` 边界：
/// 不读取 `MarketProtection`，不合并 `worst_price` / 市场价格区间。
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

            // 买入:
            // 向下取整：`floor(reference * (10000 + bps) / 10000)`
            let raw = numerator
                .checked_div(BPS_DENOMINATOR)
                .ok_or(RejectReason::ArithmeticOverflow)?;

            // 再向下对齐价格步长。
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

            // 卖出:
            // 向上取整：`ceil(reference * (10000 - bps) / 10000)`
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

            // 再向上对齐价格步长，同样只使用商和余数。
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

    // 参考值合法、基点/价格步长合法后，如果结果无法构造合法 Price，
    // 对本纯计算边界统一视为“结果不可表示”。
    Price::try_new(aligned).map_err(|_| RejectReason::ArithmeticOverflow)
}

/// 市价按报价币预算执行后的纯结果值对象。
///
/// 保留成交事件的原始顺序；金额以非负 QuoteAmount 表示。
/// 不负责检查 spent_quote + unused_quote 与初始预算的关系。
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MarketByQuoteExecution {
    /// 按撮合顺序保留成交、自成交取消及冰山生命周期事件。
    events: Vec<EngineEvent>,
    /// 按撮合顺序产生的成交事件。
    trades: Vec<TradeEvent>,
    /// 已实际使用的报价资产本金。
    spent_quote: QuoteAmount,
    /// 未使用且不构成成交的报价资产金额。
    unused_quote: QuoteAmount,
}

impl MarketByQuoteExecution {
    /// 保留按撮合顺序产生的 Trade 与已由执行器校验的报价金额。
    pub fn new(
        trades: Vec<TradeEvent>,
        spent_quote: QuoteAmount,
        unused_quote: QuoteAmount,
    ) -> Self {
        let events = trades.iter().cloned().map(EngineEvent::Trade).collect();
        Self {
            events,
            trades,
            spent_quote,
            unused_quote,
        }
    }

    /// 从原序事件构造结果，保留报价金额和真实成交投影。
    pub fn from_events(
        events: Vec<EngineEvent>,
        spent_quote: QuoteAmount,
        unused_quote: QuoteAmount,
    ) -> Self {
        let trades = events
            .iter()
            .filter_map(|event| match event {
                EngineEvent::Trade(trade) => Some(trade.clone()),
                _ => None,
            })
            .collect();
        Self {
            events,
            trades,
            spent_quote,
            unused_quote,
        }
    }

    /// 按原序读取全部业务事件。
    pub fn events(&self) -> &[EngineEvent] {
        &self.events
    }

    /// 按产生顺序借用成交事件。
    pub fn trades(&self) -> &[TradeEvent] {
        &self.trades
    }

    /// 返回已执行的报价资产本金；零表示没有成交。
    pub fn spent_quote(&self) -> QuoteAmount {
        self.spent_quote
    }

    /// 返回未使用的初始报价资产预算；该金额不是成交。
    pub fn unused_quote(&self) -> QuoteAmount {
        self.unused_quote
    }
}

#[cfg(test)]
#[path = "market_order/tests.rs"]
mod tests;
