//! 生产订单簿与分片配置的市场归属。
use super::ProductionOrderBook;
use matching_domain::MarketId;

/// 市场绑定与现有订单簿状态冲突。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MarketBindingError;

/// 单笔成交的订单簿与预留登记表隔离候选。
pub struct ProductionTradeR2Plan {
    /// 验证提交时未漂移的订单簿前态。
    before_book: ProductionOrderBook,
    /// 验证提交时未漂移的登记表前态。
    before_registry: matching_risk::ReservationRegistry,
    /// 完整订单簿候选。
    after_book: ProductionOrderBook,
    /// 完整登记表候选。
    after_registry: matching_risk::ReservationRegistry,
    /// 已预检的成交本金及费用。
    settlement: matching_risk::TradeSettlement,
    /// 已预检的真实剩余释放。
    releases: Vec<matching_risk::SettlementRelease>,
}

impl ProductionOrderBook {
    /// 构造成交 R2 的双状态候选；返回值尚未写入 WAL 或发布事件。
    pub fn plan_trade_r2_candidate(
        &self,
        registry: &matching_risk::ReservationRegistry,
        context: matching_risk::TradeSettlementContext<'_>,
        trade: &matching_domain::TradeEvent,
        maker: matching_domain::ReservationId,
        taker: matching_domain::ReservationId,
        terminal: &[matching_domain::ReservationId],
    ) -> Result<ProductionTradeR2Plan, matching_risk::SettlementPlanError> {
        use matching_risk::SettlementPlanError;
        if self.market_id != Some(context.market_id) {
            return Err(SettlementPlanError::InvalidReservation);
        }
        self.validate_registry_bindings(
            registry,
            self.reservations.iter().map(|(id, state)| (*id, state)),
        )
        .map_err(SettlementPlanError::Reservation)?;
        let (settlement, plan) =
            matching_risk::plan_trade_r2(context, trade, registry, maker, taker, terminal)?;
        let after_book = self
            .plan_r2_shadow_update(&plan)
            .map_err(SettlementPlanError::Reservation)?;
        let releases = plan.releases().to_vec();
        let mut after_registry = registry.clone();
        after_registry
            .apply_r2(plan)
            .expect("独立 R2 候选前态不能漂移");
        after_book
            .validate_registry_bindings(
                &after_registry,
                after_book
                    .reservations
                    .iter()
                    .map(|(id, state)| (*id, state)),
            )
            .map_err(SettlementPlanError::Reservation)?;
        Ok(ProductionTradeR2Plan {
            before_book: self.clone(),
            before_registry: registry.clone(),
            after_book,
            after_registry,
            settlement,
            releases,
        })
    }

    /// 验证两个前态后一次安装；结果尚须由调用方封装并持久发布。
    pub fn apply_trade_r2_candidate(
        &mut self,
        registry: &mut matching_risk::ReservationRegistry,
        plan: ProductionTradeR2Plan,
    ) -> Result<
        (
            matching_risk::TradeSettlement,
            Vec<matching_risk::SettlementRelease>,
        ),
        matching_risk::ReservationApplyError,
    > {
        if *self != plan.before_book || *registry != plan.before_registry {
            return Err(matching_risk::ReservationApplyError::StateChanged);
        }
        *self = plan.after_book;
        *registry = plan.after_registry;
        Ok((plan.settlement, plan.releases))
    }
    /// 在独立副本更新活动订单的 R2 预留影子；调用方须与登记表候选原子安装。
    pub fn plan_r2_shadow_update(
        &self,
        plan: &matching_risk::PlannedR2,
    ) -> Result<Self, matching_domain::RejectReason> {
        let mut candidate = self.clone();
        for (order_id, shadow) in &self.reservations {
            if plan.before_state(shadow.reservation_id) != Some(shadow) {
                return Err(matching_domain::RejectReason::InvalidReservation);
            }
            let next = plan
                .after_state(shadow.reservation_id)
                .ok_or(matching_domain::RejectReason::InvalidReservation)?;
            if next.status != matching_risk::ReservationStatus::Active
                || next.order_id != *order_id
                || next.market_id != shadow.market_id
                || next.user_id != shadow.user_id
                || next.fee_schedule_version != shadow.fee_schedule_version
                || next.fee_tier_id != shadow.fee_tier_id
            {
                return Err(matching_domain::RejectReason::InvalidReservation);
            }
            candidate.reservations.insert(*order_id, next.clone());
        }
        Ok(candidate)
    }
    /// 将空簿绑定到可信市场，绑定后不可更换市场。
    pub fn bind_market(&mut self, market_id: MarketId) -> Result<(), MarketBindingError> {
        match self.market_id {
            Some(current) if current == market_id => Ok(()),
            Some(_) => Err(MarketBindingError),
            None if self.orders.is_empty()
                && self
                    .stop_book
                    .ordered(matching_domain::Side::Buy)
                    .is_empty()
                && self
                    .stop_book
                    .ordered(matching_domain::Side::Sell)
                    .is_empty()
                && self.reservations.is_empty() =>
            {
                self.market_id = Some(market_id);
                Ok(())
            }
            None => Err(MarketBindingError),
        }
    }
    /// 输出按订单身份排序的预留快照，供分片核验全部市场归属。
    pub fn reservation_snapshots(
        &self,
    ) -> Vec<(matching_domain::OrderId, &matching_risk::ReservationState)> {
        let mut entries: Vec<_> = self
            .reservations
            .iter()
            .map(|(id, state)| (*id, state))
            .collect();
        entries.sort_by_key(|(id, _)| *id);
        entries
    }

    /// 绑定市场时仅校验其完整归属；旧独立簿继续使用原全量契约。
    pub(super) fn validate_registry_bindings<'a>(
        &self,
        registry: &matching_risk::ReservationRegistry,
        states: impl IntoIterator<
            Item = (
                matching_domain::OrderId,
                &'a matching_risk::ReservationState,
            ),
        >,
    ) -> Result<(), matching_domain::RejectReason> {
        match self.market_id {
            Some(market) => registry.validate_market_order_bindings(market, states),
            None => registry.validate_order_bindings(states),
        }
    }
}
