//! 市场状态和交易时段的纯转换规则。
use crate::{MarketMath, RejectReason};

/// 市场整数数值配置更新不满足的业务约束。
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum MarketMathUpdateError {
    /// 仍有活动订单或历史金额引用旧 scale。
    ScaleInUse,
    /// 改变 tick 或 lot 时市场未暂停或仍有活动订单。
    TickLotRequiresSuspendedEmpty,
}

/// 校验已分别构造并验证的数值配置，调用方必须提供可信订单及历史引用状态。
///
/// 返回成功只覆盖数值配置限制；订单限额、资产映射和持久提交仍由调用方校验。
pub fn validate_market_math_update(
    old: MarketMath,
    new: MarketMath,
    state: MarketState,
    has_active_orders: bool,
    has_referenced_history: bool,
) -> Result<(), MarketMathUpdateError> {
    if (old.price_scale(), old.qty_scale(), old.quote_scale())
        != (new.price_scale(), new.qty_scale(), new.quote_scale())
        && (has_active_orders || has_referenced_history)
    {
        return Err(MarketMathUpdateError::ScaleInUse);
    }
    if (old.tick_size(), old.lot_size()) != (new.tick_size(), new.lot_size())
        && (state != MarketState::Suspended || has_active_orders)
    {
        return Err(MarketMathUpdateError::TickLotRequiresSuspendedEmpty);
    }
    Ok(())
}

/// 连续撮合市场的生命周期状态。
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum MarketState {
    /// 开市前允许不交叉的挂单。
    PreOpen,
    /// 连续撮合。
    Open,
    /// 风控或熔断暂停。
    Halted,
    /// 人工暂停。
    Suspended,
    /// 当前时段终态。
    Closed,
}

/// 市场状态转换的稳定业务原因编号。
#[repr(u16)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum MarketStateReason {
    /// 市场创建。
    Created = 1401,
    /// 管理员操作。
    Admin = 1402,
    /// 确定性的熔断规则触发。
    CircuitBreaker = 1403,
    /// 熔断开关触发。
    KillSwitch = 1404,
    /// 当前交易时段结束。
    SessionEnded = 1405,
    /// 新交易时段开始。
    SessionStarted = 1408,
    /// 开始连续撮合。
    Opened = 1409,
}

/// 熔断事件的稳定原因编号。
#[repr(u16)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CircuitBreakerReason {
    /// 价格偏离触发。
    PriceDeviation = 1501,
    /// 所需参考价格不可用。
    ReferenceUnavailable = 1502,
    /// 波动限制触发。
    VolatilityLimit = 1503,
    /// 确定性的内部安全条件触发。
    InternalSafety = 1504,
    /// 持久化的人工风控信号触发。
    ManualRiskSignal = 1505,
}

/// 市场状态与当前交易时段；不拥有订单簿或预留状态。
///
/// 调用方必须在隔离候选中完成清理和其他业务预检，再与订单簿一起原子安装。
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct MarketLifecycle {
    /// 当前市场状态。
    state: MarketState,
    /// 最近启动的交易时段身份。
    session_id: Option<u64>,
    /// 当前时段是否尚未结束。
    session_active: bool,
    /// 风控停止是否发生在仍须重校验的开市前状态。
    preopen_revalidation_required: bool,
}

impl MarketLifecycle {
    /// 构造尚未启动交易时段的关闭状态。
    pub const fn new_closed() -> Self {
        Self {
            state: MarketState::Closed,
            session_id: None,
            session_active: false,
            preopen_revalidation_required: false,
        }
    }
    /// 返回当前状态。
    pub const fn state(&self) -> MarketState {
        self.state
    }
    /// 返回最近启动的交易时段身份。
    pub const fn session_id(&self) -> Option<u64> {
        self.session_id
    }
    /// 返回最近启动的交易时段是否仍在进行。
    pub const fn session_active(&self) -> bool {
        self.session_active
    }
    /// 风控停止发生于 PreOpen，恢复前须重新校验现有订单。
    pub const fn preopen_revalidation_required(&self) -> bool {
        self.preopen_revalidation_required
    }
    /// 以版本、状态标记和可选交易时段编码生命周期。
    pub fn write_snapshot(
        &self,
        writer: &mut crate::snapshot_wire::SnapshotEncoder,
    ) -> Result<(), crate::snapshot_wire::SnapshotWireError> {
        let mut buffer = crate::snapshot_wire::SnapshotEncoder::new(12);
        let legacy = self.session_active == (self.state != MarketState::Closed);
        buffer.u8(match (self.preopen_revalidation_required, legacy) {
            (false, true) => 1,
            (false, false) => 2,
            (true, true) => 3,
            (true, false) => 4,
        })?;
        buffer.u8(match self.state {
            MarketState::PreOpen => 0,
            MarketState::Open => 1,
            MarketState::Halted => 2,
            MarketState::Suspended => 3,
            MarketState::Closed => 4,
        })?;
        match self.session_id {
            None => buffer.u8(0),
            Some(id) => {
                buffer.u8(1)?;
                buffer.u64(id)
            }
        }?;
        if !legacy {
            buffer.u8(u8::from(self.session_active))?;
        }
        writer.raw_bytes(&buffer.finish())
    }
    /// 恢复生命周期；拒绝未知版本、状态及没有时段的非关闭状态。
    pub fn read_snapshot(
        reader: &mut crate::snapshot_wire::SnapshotDecoder<'_>,
    ) -> Result<Self, crate::snapshot_wire::SnapshotWireError> {
        use crate::snapshot_wire::SnapshotWireError;
        let version = reader.u8()?;
        if !(1..=4).contains(&version) {
            return Err(SnapshotWireError);
        }
        let state = match reader.u8()? {
            0 => MarketState::PreOpen,
            1 => MarketState::Open,
            2 => MarketState::Halted,
            3 => MarketState::Suspended,
            4 => MarketState::Closed,
            _ => return Err(SnapshotWireError),
        };
        let session_id = match reader.u8()? {
            0 => None,
            1 => Some(reader.u64()?),
            _ => return Err(SnapshotWireError),
        };
        if state != MarketState::Closed && session_id.is_none() {
            return Err(SnapshotWireError);
        }
        let session_active = if version == 1 || version == 3 {
            state != MarketState::Closed
        } else {
            match reader.u8()? {
                0 => false,
                1 => true,
                _ => return Err(SnapshotWireError),
            }
        };
        if session_active && (session_id.is_none() || state == MarketState::Closed) {
            return Err(SnapshotWireError);
        }
        let preopen_revalidation_required = version >= 3;
        if preopen_revalidation_required && state != MarketState::Halted {
            return Err(SnapshotWireError);
        }
        Ok(Self {
            state,
            session_id,
            session_active,
            preopen_revalidation_required,
        })
    }
    /// 校验限价下单许可；交叉结果须由当前订单簿独立计算。
    pub fn check_limit(&self, crosses: bool) -> Result<(), RejectReason> {
        match self.state {
            MarketState::PreOpen if crosses => Err(RejectReason::PreOpenWouldCross),
            MarketState::PreOpen | MarketState::Open => Ok(()),
            MarketState::Halted => Err(RejectReason::MarketHalted),
            MarketState::Suspended => Err(RejectReason::MarketSuspended),
            MarketState::Closed => Err(RejectReason::MarketClosed),
        }
    }
    /// 校验限价有效期与交叉条件；PreOpen 禁止 IOC 和 FOK。
    pub fn check_limit_tif(
        &self,
        tif: crate::TimeInForce,
        crosses: bool,
    ) -> Result<(), RejectReason> {
        if self.state == MarketState::PreOpen
            && matches!(tif, crate::TimeInForce::Ioc | crate::TimeInForce::Fok)
        {
            return Err(RejectReason::InvalidOrderCombination);
        }
        self.check_limit(crosses)
    }
    /// 校验市价订单许可；开市前不允许市价单。
    pub fn check_market_order(&self) -> Result<(), RejectReason> {
        match self.state {
            MarketState::PreOpen => Err(RejectReason::MarketNotOpen),
            MarketState::Open => Ok(()),
            MarketState::Halted => Err(RejectReason::MarketHalted),
            MarketState::Suspended => Err(RejectReason::MarketSuspended),
            MarketState::Closed => Err(RejectReason::MarketClosed),
        }
    }
    /// 校验改单许可；开市前改动不得穿越对手最优价。
    pub fn check_amend(&self, crosses: bool) -> Result<(), RejectReason> {
        self.check_limit(crosses)
    }
    /// 校验客户撤单许可；关闭过程的内部清理由调用方单独处理。
    pub fn check_cancel(&self) -> Result<(), RejectReason> {
        if self.state == MarketState::Closed {
            Err(RejectReason::MarketClosed)
        } else {
            Ok(())
        }
    }
    /// 从关闭状态开启交易时段并进入开市前状态；调用方须先确认订单簿为空。
    pub fn start_session(&mut self, id: u64) -> Result<(), RejectReason> {
        if self.state != MarketState::Closed {
            return Err(RejectReason::InvalidMarketStateTransition);
        }
        self.state = MarketState::PreOpen;
        self.session_id = Some(id);
        self.session_active = true;
        Ok(())
    }
    /// 校验并标记当前时段结束；订单清理由调用方在同一候选中完成。
    pub fn end_session(&mut self, id: u64) -> Result<(), RejectReason> {
        if !self.session_active || self.session_id != Some(id) {
            return Err(RejectReason::InvalidSession);
        }
        self.session_active = false;
        Ok(())
    }
    /// 将匹配时段的开市前状态切换到连续撮合；调用方须先验证参考价和订单重校验。
    pub fn open(&mut self, id: u64) -> Result<(), RejectReason> {
        if self.state != MarketState::PreOpen {
            return Err(RejectReason::InvalidMarketStateTransition);
        }
        if self.session_id != Some(id) || !self.session_active {
            return Err(RejectReason::InvalidSession);
        }
        self.state = MarketState::Open;
        Ok(())
    }
    /// 人工暂停当前开放市场。
    pub fn suspend(&mut self) -> Result<(), RejectReason> {
        if self.state != MarketState::Open {
            return Err(RejectReason::InvalidMarketStateTransition);
        }
        self.state = MarketState::Suspended;
        Ok(())
    }
    /// 将活动市场切换为风控暂停；关闭和已停止的状态不能重复触发。
    pub fn halt(&mut self) -> Result<(), RejectReason> {
        if !matches!(
            self.state,
            MarketState::PreOpen | MarketState::Open | MarketState::Suspended
        ) {
            return Err(RejectReason::InvalidMarketStateTransition);
        }
        self.preopen_revalidation_required = self.state == MarketState::PreOpen;
        self.state = MarketState::Halted;
        Ok(())
    }
    /// 从风险暂停或人工暂停恢复连续撮合。
    pub fn resume(&mut self) -> Result<(), RejectReason> {
        self.resume_after_revalidation(false)
    }
    /// 从开市前熔断恢复时要求调用方确认已完成重校验。
    pub(crate) fn resume_after_revalidation(&mut self, complete: bool) -> Result<(), RejectReason> {
        if self.preopen_revalidation_required && !complete {
            return Err(RejectReason::InvalidMarketStateTransition);
        }
        if !matches!(self.state, MarketState::Halted | MarketState::Suspended) {
            return Err(RejectReason::InvalidMarketStateTransition);
        }
        if !self.session_active {
            return Err(RejectReason::InvalidSession);
        }
        self.state = MarketState::Open;
        self.preopen_revalidation_required = false;
        Ok(())
    }
    /// 将非关闭状态转换为关闭状态；调用方须同时原子清理市场。
    pub fn close(&mut self) -> Result<(), RejectReason> {
        if self.state == MarketState::Closed {
            return Err(RejectReason::InvalidMarketStateTransition);
        }
        self.state = MarketState::Closed;
        self.preopen_revalidation_required = false;
        self.session_active = false;
        Ok(())
    }
}
