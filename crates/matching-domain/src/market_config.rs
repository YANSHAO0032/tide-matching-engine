//! 市场管理使用的静态配置及其发布前数值校验。
use crate::{MarketId, MarketMath, PriceBandConfig, Qty, QuoteAmount};

/// 单笔订单的数量和名义金额上下限。
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct OrderLimits {
    /// 最小基础数量。
    pub min_qty: Qty,
    /// 最大基础数量。
    pub max_qty: Qty,
    /// 最小报价资产本金。
    pub min_notional: QuoteAmount,
    /// 最大报价资产本金。
    pub max_notional: QuoteAmount,
}

/// 一条根命令及市场优先级分配的安全边界。
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ExecutionLimits {
    /// 根命令允许的内部动作上限。
    pub max_internal_actions_per_command: u32,
    /// 根命令允许的成交上限。
    pub max_trades_per_command: u32,
    /// 优先级空间末尾预留的非零保护区。
    pub priority_exhaustion_guard: u128,
    /// 开发或测试可用的提前耗尽边界。
    pub synthetic_priority_ceiling: Option<u128>,
}

/// 关市后活动订单的处置策略。
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ClosePolicy {
    /// 终结所有活动订单并处理对应预留。
    CancelAll,
}

/// 必须通过校验并与市场状态一起持久化的配置。
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MarketConfig {
    /// 市场身份。
    pub market_id: MarketId,
    /// 价格的十进制精度。
    pub price_scale: u8,
    /// 基础数量的十进制精度。
    pub qty_scale: u8,
    /// 报价金额的十进制精度。
    pub quote_scale: u8,
    /// 价格最小步长。
    pub tick_size: i64,
    /// 数量最小步长。
    pub lot_size: u64,
    /// 单笔订单限制。
    pub order_limits: OrderLimits,
    /// 可选的市场价格带配置。
    pub price_band: Option<PriceBandConfig>,
    /// 当前生效的费率版本身份。
    pub active_fee_schedule_version: u64,
    /// 关市策略。
    pub close_policy: ClosePolicy,
    /// 安全执行边界。
    pub execution_limits: ExecutionLimits,
}

/// 市场配置本身不满足静态校验；不是对外稳定协议编号。
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum MarketConfigError {
    /// 数值配置未通过精确金额检查，保留具体失败分类。
    Math(crate::MathConfigError),
    /// 价格带偏离基点超出合法范围。
    InvalidPriceBand,
    /// 订单限制或优先级保护不满足约束。
    Constraints,
}

impl MarketConfigError {
    /// 将配置校验失败映射为冻结的管理命令拒绝原因。
    pub const fn to_reject_reason(self) -> crate::RejectReason {
        match self {
            Self::Math(crate::MathConfigError::InvalidTick) => {
                crate::RejectReason::MarketConfigInvalidTick
            }
            Self::Math(crate::MathConfigError::InvalidLot) => {
                crate::RejectReason::MarketConfigInvalidLot
            }
            Self::Math(crate::MathConfigError::UnsupportedScale) => {
                crate::RejectReason::MarketConfigUnsupportedScale
            }
            Self::Math(crate::MathConfigError::NonIntegralQuoteUnit) => {
                crate::RejectReason::MarketConfigNonIntegralQuoteUnit
            }
            Self::Math(crate::MathConfigError::ArithmeticOverflow) => {
                crate::RejectReason::MarketConfigArithmeticOverflow
            }
            Self::InvalidPriceBand => crate::RejectReason::InvalidSlippageBps,
            Self::Constraints => crate::RejectReason::InvalidOrderCombination,
        }
    }
}

/// 由同一单写者提供的配置更新预检事实。
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct MarketConfigUpdateChecks {
    /// 普通簿或 StopBook 是否存在活动订单。
    pub has_active_orders: bool,
    /// 历史成交、预留或结算是否仍引用原金额精度。
    pub has_referenced_history: bool,
    /// 既有订单是否都满足目标配置与其业务约束。
    pub existing_orders_valid: bool,
    /// 目标费率版本是否由可信的费率来源确认可用。
    pub fee_version_available: bool,
}

/// 配置候选未通过内部校验；不是对外稳定协议原因编号。
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum MarketConfigUpdateError {
    /// 目标配置本身无效。
    InvalidConfig,
    /// 更新试图更换市场身份。
    MarketMismatch,
    /// 数值协议的状态或历史引用约束未满足。
    Math(crate::MarketMathUpdateError),
    /// 既有订单不能按新配置解释。
    ExistingOrdersInvalid,
    /// 目标费率版本尚不可用。
    FeeVersionUnavailable,
    /// 无法预留输出事件空间。
    Capacity,
}

/// 候选市场的配置与生命周期；不拥有订单簿、预留或持久化写入器。
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ManagedMarket {
    /// 已通过静态校验的市场配置。
    config: MarketConfig,
    /// 对应的市场状态及交易时段。
    lifecycle: crate::MarketLifecycle,
    /// 是否存在需要按原精度解释的历史金额。
    referenced_history: bool,
}

impl ManagedMarket {
    /// 验证目标配置与可信状态后原子更新候选，并产生配置更新事件。
    ///
    /// 预检事实须在同一单写者的订单簿、历史金额和费率视图中计算；
    /// 本函数不查询账户、持久写入，也不自行建立历史版本。
    pub fn update_config(
        &mut self,
        next: MarketConfig,
        checks: MarketConfigUpdateChecks,
    ) -> Result<Vec<crate::EngineEvent>, MarketConfigUpdateError> {
        if next.market_id != self.config.market_id {
            return Err(MarketConfigUpdateError::MarketMismatch);
        }
        let old_math = self
            .config
            .validate()
            .map_err(|_| MarketConfigUpdateError::InvalidConfig)?;
        let new_math = next
            .validate()
            .map_err(|_| MarketConfigUpdateError::InvalidConfig)?;
        crate::validate_market_math_update(
            old_math,
            new_math,
            self.lifecycle.state(),
            checks.has_active_orders,
            checks.has_referenced_history || self.referenced_history,
        )
        .map_err(MarketConfigUpdateError::Math)?;
        if !checks.existing_orders_valid {
            return Err(MarketConfigUpdateError::ExistingOrdersInvalid);
        }
        if !checks.fee_version_available {
            return Err(MarketConfigUpdateError::FeeVersionUnavailable);
        }
        let mut events = Vec::new();
        events
            .try_reserve_exact(1)
            .map_err(|_| MarketConfigUpdateError::Capacity)?;
        events.push(crate::EngineEvent::MarketConfigUpdated {
            market_id: next.market_id,
        });
        self.config = next;
        Ok(events)
    }
    /// 原子编码市场配置及生命周期，不改变失败时的外层缓冲区。
    pub fn write_snapshot(
        &self,
        writer: &mut crate::snapshot_wire::SnapshotEncoder,
    ) -> Result<(), crate::snapshot_wire::SnapshotWireError> {
        let mut buffer = crate::snapshot_wire::SnapshotEncoder::new(512);
        buffer.u8(if self.referenced_history { 1 } else { 2 })?;
        self.config.write_snapshot(&mut buffer)?;
        self.lifecycle.write_snapshot(&mut buffer)?;
        writer.raw_bytes(&buffer.finish())
    }
    /// 恢复并验证版本、配置和生命周期。
    pub fn read_snapshot(
        reader: &mut crate::snapshot_wire::SnapshotDecoder<'_>,
    ) -> Result<Self, crate::snapshot_wire::SnapshotWireError> {
        let referenced_history = match reader.u8()? {
            1 => true,
            2 => false,
            _ => return Err(crate::snapshot_wire::SnapshotWireError),
        };
        let config = MarketConfig::read_snapshot(reader)?;
        let lifecycle = crate::MarketLifecycle::read_snapshot(reader)?;
        Ok(Self {
            config,
            lifecycle,
            referenced_history,
        })
    }
    /// 用有效配置建立关闭的空候选状态。
    pub fn new(config: MarketConfig) -> Result<Self, MarketConfigError> {
        config.validate()?;
        Ok(Self {
            config,
            lifecycle: crate::MarketLifecycle::new_closed(),
            referenced_history: false,
        })
    }
    /// 返回当前生命周期。
    pub const fn lifecycle(&self) -> &crate::MarketLifecycle {
        &self.lifecycle
    }
    /// 返回市场配置。
    pub const fn config(&self) -> &MarketConfig {
        &self.config
    }
    /// 返回当前市场是否已记录需保留精度的历史金额。
    pub const fn has_referenced_history(&self) -> bool {
        self.referenced_history
    }
    /// 在接受引用当前金额精度的业务状态后永久标记历史引用。
    pub fn record_referenced_history(&mut self) {
        self.referenced_history = true;
    }
    /// 结束指定交易时段；调用方须在同一候选内清理当日订单。
    pub fn end_session(&mut self, session_id: u64) -> Result<(), crate::RejectReason> {
        self.lifecycle.end_session(session_id)
    }
    /// 对关闭且订单簿为空的候选启动交易时段并按协议顺序生成事件。
    ///
    /// `book_empty` 必须由受同一单写者保护的普通簿和 StopBook 共同计算。
    pub fn start_session(
        &mut self,
        session_id: u64,
        book_empty: bool,
    ) -> Result<Vec<crate::EngineEvent>, crate::RejectReason> {
        if !book_empty {
            return Err(crate::RejectReason::InvalidMarketStateTransition);
        }
        let old = self.lifecycle.state();
        let mut events = Vec::new();
        events
            .try_reserve_exact(2)
            .map_err(|_| crate::RejectReason::EngineBusy)?;
        self.lifecycle.start_session(session_id)?;
        let market_id = self.config.market_id;
        events.push(crate::EngineEvent::SessionStarted {
            market_id,
            session_id,
        });
        events.push(crate::EngineEvent::MarketStateChanged {
            market_id,
            old,
            new: self.lifecycle.state(),
            reason: crate::MarketStateReason::SessionStarted,
        });
        Ok(events)
    }
    /// 按时段及价格带准备状态切换；预检结果须来自同一单写者的可信状态。
    pub fn open(
        &mut self,
        session_id: u64,
        valid_reference: bool,
        revalidation_complete: bool,
    ) -> Result<Vec<crate::EngineEvent>, crate::RejectReason> {
        let old = self.lifecycle.state();
        let mut candidate = self.lifecycle;
        candidate.open(session_id)?;
        if self.config.price_band.is_some() && !valid_reference {
            return Err(crate::RejectReason::ReferencePriceUnavailable);
        }
        if self.config.price_band.is_some() && !revalidation_complete {
            return Err(crate::RejectReason::InvalidMarketStateTransition);
        }
        let mut events = Vec::new();
        events
            .try_reserve_exact(1)
            .map_err(|_| crate::RejectReason::EngineBusy)?;
        events.push(crate::EngineEvent::MarketStateChanged {
            market_id: self.config.market_id,
            old,
            new: candidate.state(),
            reason: crate::MarketStateReason::Opened,
        });
        self.lifecycle = candidate;
        Ok(events)
    }
    /// 在候选状态中执行人工暂停并生成状态变化事件。
    pub fn suspend(&mut self) -> Result<Vec<crate::EngineEvent>, crate::RejectReason> {
        let old = self.lifecycle.state();
        let mut candidate = self.lifecycle;
        candidate.suspend()?;
        self.install_transition(candidate, old, crate::MarketStateReason::Admin)
    }
    /// 仅在价格带参考有效时恢复已暂停的候选市场。
    pub fn resume(
        &mut self,
        valid_reference: bool,
    ) -> Result<Vec<crate::EngineEvent>, crate::RejectReason> {
        let old = self.lifecycle.state();
        let mut candidate = self.lifecycle;
        candidate.resume()?;
        if self.config.price_band.is_some() && !valid_reference {
            return Err(crate::RejectReason::ReferencePriceUnavailable);
        }
        self.install_transition(candidate, old, crate::MarketStateReason::Admin)
    }
    /// 在普通簿和 StopBook 的候选清理完成后将市场置为关闭。
    pub fn close(
        &mut self,
        cleanup_complete: bool,
    ) -> Result<Vec<crate::EngineEvent>, crate::RejectReason> {
        if !cleanup_complete {
            return Err(crate::RejectReason::InvalidMarketStateTransition);
        }
        let old = self.lifecycle.state();
        let mut candidate = self.lifecycle;
        candidate.close()?;
        self.install_transition(candidate, old, crate::MarketStateReason::Admin)
    }
    /// 预留事件空间后安装经过验证的生命周期候选。
    fn install_transition(
        &mut self,
        candidate: crate::MarketLifecycle,
        old: crate::MarketState,
        reason: crate::MarketStateReason,
    ) -> Result<Vec<crate::EngineEvent>, crate::RejectReason> {
        let mut events = Vec::new();
        events
            .try_reserve_exact(1)
            .map_err(|_| crate::RejectReason::EngineBusy)?;
        events.push(crate::EngineEvent::MarketStateChanged {
            market_id: self.config.market_id,
            old,
            new: candidate.state(),
            reason,
        });
        self.lifecycle = candidate;
        Ok(events)
    }
}

impl MarketConfig {
    /// 将有效配置按版本及固定字段顺序写入有界编码器。
    pub fn write_snapshot(
        &self,
        writer: &mut crate::snapshot_wire::SnapshotEncoder,
    ) -> Result<(), crate::snapshot_wire::SnapshotWireError> {
        use crate::snapshot_wire::SnapshotWireError;
        self.validate().map_err(|_| SnapshotWireError)?;
        self.write_command_config(writer)
    }

    /// 对命令保存完整配置；数值合法性由业务执行时检查。
    pub(crate) fn write_command_config(
        &self,
        writer: &mut crate::snapshot_wire::SnapshotEncoder,
    ) -> Result<(), crate::snapshot_wire::SnapshotWireError> {
        let mut buffer = crate::snapshot_wire::SnapshotEncoder::new(256);
        let w = &mut buffer;
        w.u8(1)?;
        w.u32(self.market_id.get())?;
        w.u8(self.price_scale)?;
        w.u8(self.qty_scale)?;
        w.u8(self.quote_scale)?;
        w.i64(self.tick_size)?;
        w.u64(self.lot_size)?;
        w.u64(self.order_limits.min_qty.get())?;
        w.u64(self.order_limits.max_qty.get())?;
        w.u128(self.order_limits.min_notional.get())?;
        w.u128(self.order_limits.max_notional.get())?;
        match self.price_band {
            None => w.u8(0)?,
            Some(band) => {
                w.u8(1)?;
                w.u8(band.reference_kind as u8)?;
                w.u32(band.max_deviation_bps)?;
            }
        }
        w.u64(self.active_fee_schedule_version)?;
        match self.close_policy {
            ClosePolicy::CancelAll => w.u8(0)?,
        }
        w.u32(self.execution_limits.max_internal_actions_per_command)?;
        w.u32(self.execution_limits.max_trades_per_command)?;
        w.u128(self.execution_limits.priority_exhaustion_guard)?;
        match self.execution_limits.synthetic_priority_ceiling {
            None => w.u8(0),
            Some(value) => {
                w.u8(1)?;
                w.u128(value)
            }
        }?;
        writer.raw_bytes(&buffer.finish())
    }

    /// 读取配置并重新检查全部静态约束。
    pub fn read_snapshot(
        r: &mut crate::snapshot_wire::SnapshotDecoder<'_>,
    ) -> Result<Self, crate::snapshot_wire::SnapshotWireError> {
        let config = Self::read_command_config(r)?;
        config
            .validate()
            .map_err(|_| crate::snapshot_wire::SnapshotWireError)?;
        Ok(config)
    }

    /// 解析命令配置的结构，保留待业务校验的数值。
    pub(crate) fn read_command_config(
        r: &mut crate::snapshot_wire::SnapshotDecoder<'_>,
    ) -> Result<Self, crate::snapshot_wire::SnapshotWireError> {
        use crate::{PriceReferenceKind, snapshot_wire::SnapshotWireError};
        if r.u8()? != 1 {
            return Err(SnapshotWireError);
        }
        let market_id = MarketId(r.u32()?);
        let price_scale = r.u8()?;
        let qty_scale = r.u8()?;
        let quote_scale = r.u8()?;
        let tick_size = r.i64()?;
        let lot_size = r.u64()?;
        let order_limits = OrderLimits {
            min_qty: Qty::try_new(r.u64()?).map_err(|_| SnapshotWireError)?,
            max_qty: Qty::try_new(r.u64()?).map_err(|_| SnapshotWireError)?,
            min_notional: QuoteAmount::new(r.u128()?),
            max_notional: QuoteAmount::new(r.u128()?),
        };
        let price_band = match r.u8()? {
            0 => None,
            1 => {
                let reference_kind = match r.u8()? {
                    1 => PriceReferenceKind::LastTrade,
                    2 => PriceReferenceKind::MarkPrice,
                    3 => PriceReferenceKind::IndexPrice,
                    4 => PriceReferenceKind::StaticOpening,
                    _ => return Err(SnapshotWireError),
                };
                Some(PriceBandConfig {
                    reference_kind,
                    max_deviation_bps: r.u32()?,
                })
            }
            _ => return Err(SnapshotWireError),
        };
        let active_fee_schedule_version = r.u64()?;
        let close_policy = match r.u8()? {
            0 => ClosePolicy::CancelAll,
            _ => return Err(SnapshotWireError),
        };
        let max_internal_actions_per_command = r.u32()?;
        let max_trades_per_command = r.u32()?;
        let priority_exhaustion_guard = r.u128()?;
        let synthetic_priority_ceiling = match r.u8()? {
            0 => None,
            1 => Some(r.u128()?),
            _ => return Err(SnapshotWireError),
        };
        let config = Self {
            market_id,
            price_scale,
            qty_scale,
            quote_scale,
            tick_size,
            lot_size,
            order_limits,
            price_band,
            active_fee_schedule_version,
            close_policy,
            execution_limits: ExecutionLimits {
                max_internal_actions_per_command,
                max_trades_per_command,
                priority_exhaustion_guard,
                synthetic_priority_ceiling,
            },
        };
        Ok(config)
    }

    /// 验证精确本金、上下限和优先级保护空间，返回对应金额换算配置。
    pub fn validate(&self) -> Result<MarketMath, MarketConfigError> {
        let math = MarketMath::try_new(
            self.price_scale,
            self.qty_scale,
            self.quote_scale,
            self.tick_size,
            self.lot_size,
        )
        .map_err(MarketConfigError::Math)?;
        if self
            .price_band
            .is_some_and(|band| band.max_deviation_bps > 9_999)
        {
            return Err(MarketConfigError::InvalidPriceBand);
        }
        if self.order_limits.min_qty > self.order_limits.max_qty
            || self.order_limits.min_notional > self.order_limits.max_notional
            || self.execution_limits.priority_exhaustion_guard == 0
            || self.execution_limits.priority_exhaustion_guard == u128::MAX
            || self.execution_limits.synthetic_priority_ceiling == Some(0)
        {
            return Err(MarketConfigError::Constraints);
        }
        Ok(math)
    }
}
