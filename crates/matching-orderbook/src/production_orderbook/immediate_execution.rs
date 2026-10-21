use super::*;
use matching_domain::LimitImmediateExecution;

impl ProductionOrderBook {
    /// 仅当 Limit 不会立即作为 taker 时，将其挂入本侧 FIFO 尾部。
    ///
    /// Duplicate 优先于含等价的 crossing 拒绝；只有全部挂单预检成功后才
    /// 消耗 QueuePriority。正值由领域类型保证；市场 tick/lot 对齐须由调用方校验。
    /// 不接受 Market 或 IOC/FOK 标志组合。
    pub fn place_limit_post_only(&mut self, order: LimitGtcOrder) -> Result<(), RejectReason> {
        if self.orders.contains_key(&order.order_id) {
            return Err(RejectReason::DuplicateOrderId);
        }
        let crosses = match order.side {
            Side::Buy => self
                .best_price(Side::Sell)
                .is_some_and(|ask| order.price >= ask),
            Side::Sell => self
                .best_price(Side::Buy)
                .is_some_and(|bid| order.price <= bid),
        };
        if crosses {
            return Err(RejectReason::PostOnlyWouldCross);
        }
        self.rest(order)
    }

    /// 按价格/FIFO 立即成交 Limit，正余量以 OrderCanceled 终结且永不入簿。
    ///
    /// 此入口选择 IOC 策略；LimitGtcOrder 仅复用基础限价载荷。
    /// 整单及所有可执行 fill 的 MarketMath 本金和累计金额均在 mutation 前校验。
    /// IOC 不分配 priority，不预留/释放资金，也不实现完整命令生命周期。
    ///
    /// # Panics
    ///
    /// TradeId 数量不足或内部簿结构损坏时，在提交前 fail-closed。
    /// TradeId 的全局唯一性由调用方保证，多余 ID 不使用。
    pub fn place_limit_ioc(
        &mut self,
        order: LimitGtcOrder,
        math: &MarketMath,
        trade_ids: &[TradeId],
    ) -> Result<LimitImmediateExecution, RejectReason> {
        self.place_immediate_limit(order, math, trade_ids, false)
    }

    /// 只读预检通过后完整成交 Limit，否则拒绝且保持物理簿与 priority 不变。
    ///
    /// 此入口选择 FOK 策略；不会先部分成交再回滚。只计入限价可执行 maker，
    /// 并预检整单、每笔及累计本金。流动性不足返回 FokInsufficientLiquidity；
    /// 成功结果仅含原序 Trade，既不 rest 也不取消剩余量。
    /// Market FOK、资金 reservation 及完整命令协议不在此接口范围。
    ///
    /// # Panics
    ///
    /// 完整成交需要的 TradeId 不足或内部簿结构损坏时，在提交前 fail-closed。
    /// TradeId 的全局唯一性由调用方保证，多余 ID 不使用。
    pub fn place_limit_fok(
        &mut self,
        order: LimitGtcOrder,
        math: &MarketMath,
        trade_ids: &[TradeId],
    ) -> Result<LimitImmediateExecution, RejectReason> {
        self.place_immediate_limit(order, math, trade_ids, true)
    }

    fn place_immediate_limit(
        &mut self,
        order: LimitGtcOrder,
        math: &MarketMath,
        trade_ids: &[TradeId],
        fill_or_kill: bool,
    ) -> Result<LimitImmediateExecution, RejectReason> {
        if self.orders.contains_key(&order.order_id) {
            return Err(RejectReason::DuplicateOrderId);
        }
        math.trade_notional(order.price, order.qty)
            .map_err(Self::reject_immediate_math_error)?;
        let mut incoming = IncomingOrder::new(order.clone());

        // 既有结构预检验证链、ID 索引和模拟 count/aggregate，使后续数学扫描
        // 与 commit 依赖的所有可执行节点均在首个写入前检查完毕。
        let plan = self.preflight_fill_plan(&incoming);
        let math_plan = match order.side {
            Side::Buy => self.preflight_immediate_notionals(
                self.asks.range(..=order.price),
                order.qty,
                math,
            )?,
            Side::Sell => self.preflight_immediate_notionals(
                self.bids.range(order.price..).rev(),
                order.qty,
                math,
            )?,
        };
        assert_eq!(
            math_plan, plan,
            "immediate Limit structural and numeric plans disagree"
        );

        if fill_or_kill && plan.remaining != 0 {
            return Err(RejectReason::FokInsufficientLiquidity);
        }
        assert!(
            trade_ids.len() >= plan.trade_count,
            "insufficient TradeIds for immediate Limit: required={}, provided={}",
            plan.trade_count,
            trade_ids.len()
        );

        // 写屏障：所有可预期业务拒绝、算术和 TradeId 检查已完成。
        // 复用 Production 自身的提交逻辑，不依赖 Reference 的扫描/答案。
        let priority_before = self.next_queue_priority();
        let trades = self
            .execute_fill_trades(&mut incoming, &trade_ids[..plan.trade_count])
            .unwrap_or_else(|reason| {
                panic!("preflighted immediate Limit commit failed: {reason:?}")
            });
        assert_eq!(trades.len(), plan.trade_count);
        assert_eq!(incoming.remaining(), plan.remaining);
        assert_eq!(self.next_queue_priority(), priority_before);
        assert!(!self.orders.contains_key(&order.order_id));

        let canceled = if plan.remaining == 0 {
            None
        } else {
            Some(Qty::try_new(plan.remaining).expect("positive IOC remainder must form Qty"))
        };
        Ok(LimitImmediateExecution::new(
            order.order_id,
            trades,
            canceled,
        ))
    }

    /// 对结构已验证的可执行价档按 FIFO 校验每笔及累计本金，不修改簿。
    fn preflight_immediate_notionals<'a>(
        &self,
        levels: impl Iterator<Item = (&'a Price, &'a PriceLevel)>,
        qty: Qty,
        math: &MarketMath,
    ) -> Result<FillPlan, RejectReason> {
        let mut remaining = qty.get();
        let mut trade_count = 0usize;
        let mut total_quote = 0u128;
        for (price, level) in levels {
            if remaining == 0 {
                break;
            }
            let mut cursor = level.head();
            while let Some(index) = cursor {
                if remaining == 0 {
                    break;
                }
                let maker = self
                    .arena
                    .get(index)
                    .expect("preflighted immediate maker missing");
                let fill = remaining.min(maker.remaining());
                let fill_qty =
                    Qty::try_new(fill).expect("preflighted immediate fill must be positive");
                let notional = math
                    .trade_notional(*price, fill_qty)
                    .map_err(Self::reject_immediate_math_error)?;
                total_quote = total_quote
                    .checked_add(notional.get())
                    .ok_or(RejectReason::ArithmeticOverflow)?;
                remaining = remaining
                    .checked_sub(fill)
                    .expect("preflighted immediate quantity underflow");
                trade_count = trade_count
                    .checked_add(1)
                    .expect("preflighted immediate fill count overflow");
                cursor = maker.next();
            }
        }
        Ok(FillPlan {
            trade_count,
            remaining,
        })
    }

    fn reject_immediate_math_error(error: MathError) -> RejectReason {
        error
            .to_reject_reason()
            .unwrap_or_else(|| panic!("non-rejectable immediate Limit math error: {error:?}"))
    }
}
