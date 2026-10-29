//! 撤单替换的独立订单载荷、结构边界与拒绝清理结果。

use crate::{
    MarketId, OrderFlags, OrderId, OrderType, Price, Qty, RejectReason, ReservationAdjustmentToken,
    ReservationId, ReservationToken, Side, TimeInForce, UserId,
};

/// 当前撤单替换可承载的限价及冰山执行策略。
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ReplacementOrderKind {
    /// 普通限价订单；有效期和标志仍须经过结构校验。
    Limit {
        /// 新订单的有效期。
        tif: TimeInForce,
        /// 新订单的附加限制。
        flags: OrderFlags,
    },
    /// 冰山限价订单；显示上限必须严格小于总量。
    Iceberg {
        /// 每次可展示的最大基础数量。
        peak_size: Qty,
        /// 新订单的有效期。
        tif: TimeInForce,
        /// 新订单的附加限制。
        flags: OrderFlags,
    },
}

/// 完整独立的新订单输入，身份不得从旧单或命令自动补出。
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ReplacementOrder {
    /// 新订单所属市场。
    pub market_id: MarketId,
    /// 新订单所属用户。
    pub user_id: UserId,
    /// 替换订单自身的自成交防护分组身份，不继承旧订单配置。
    pub stp_key: crate::StpKey,
    /// 替换订单作为吃单时决定冲突处理的策略，不继承旧订单配置。
    pub stp_mode: crate::StpMode,
    /// 独立于旧订单的新身份。
    pub order_id: OrderId,
    /// 新订单买卖方向，可与旧单不同。
    pub side: Side,
    /// 新订单的严格正限价。
    pub price: Price,
    /// 新订单的严格正总基础数量。
    pub qty: Qty,
    /// 当前支持的执行策略。
    pub kind: ReplacementOrderKind,
}

/// 对旧预留的延续调整，或对独立新预留的接管请求。
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ReservationTransition {
    /// 保留旧预留身份；调整授权仍绑定旧订单。
    ReuseOld {
        /// 覆盖不足时所需的绝对目标授权。
        adjustment: Option<ReservationAdjustmentToken>,
    },
    /// 使用账户风控已提前锁定的独立新预留。
    ReplaceWithNew {
        /// 新订单的真实签名授权，必须由资金层验签和核验覆盖。
        new_reservation: ReservationToken,
    },
}

/// 先完整校验、再原子提交的撤单替换命令载荷。
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CancelReplaceOrder {
    /// 命令声明的市场身份。
    pub market_id: MarketId,
    /// 命令声明的用户身份。
    pub user_id: UserId,
    /// 待替换的旧订单身份。
    pub old_order_id: OrderId,
    /// 显式携带独立身份的新订单。
    pub replacement: ReplacementOrder,
    /// 新订单的资金预留过渡请求。
    pub reservation_transition: ReservationTransition,
}

/// 未被引擎接管的新预留清理身份，仅供账户服务幂等核对。
///
/// 该身份来自拒绝输入，不表示授权已验证、状态已登记或预留已释放。
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct UnadoptedReservation {
    /// 输入中声明的新预留身份。
    pub reservation_id: ReservationId,
}

/// 撤单替换的拒绝结果，与业务事件及引擎预留释放相互独立。
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CancelReplaceRejection {
    /// 稳定的命令拒绝原因。
    pub reason: RejectReason,
    /// 未接管的新预留；延续旧预留时为空。
    pub unadopted_reservation: Option<UnadoptedReservation>,
}

impl CancelReplaceOrder {
    /// 仅校验命令与新单身份及结构，不查询旧单活动位置或验证资金授权。
    ///
    /// 调用方仍须先核实旧单、执行市场、预留和簿相关条件，并处理活动新身份冲突。
    pub fn validate_structure(&self) -> Result<(), RejectReason> {
        if self.replacement.market_id != self.market_id || self.replacement.user_id != self.user_id
        {
            return Err(RejectReason::InvalidOrderCombination);
        }
        if self.replacement.order_id == self.old_order_id {
            return Err(RejectReason::DuplicateOrderId);
        }
        if let Some(reason) = self.replacement.stp_mode.submission_rejection() {
            return Err(reason);
        }
        match self.replacement.kind {
            ReplacementOrderKind::Limit { tif, flags } => {
                validate_replacement_combination(OrderType::Limit, tif, flags)
            }
            ReplacementOrderKind::Iceberg {
                peak_size,
                tif,
                flags,
            } => {
                validate_replacement_combination(OrderType::Iceberg, tif, flags)?;
                if peak_size >= self.replacement.qty {
                    return Err(RejectReason::InvalidIceberg);
                }
                Ok(())
            }
        }
    }

    /// 为任一验证拒绝保留确定性清理身份，不生成事件或修改状态。
    pub fn rejection(&self, reason: RejectReason) -> CancelReplaceRejection {
        let unadopted_reservation = match &self.reservation_transition {
            ReservationTransition::ReuseOld { .. } => None,
            ReservationTransition::ReplaceWithNew { new_reservation } => {
                Some(UnadoptedReservation {
                    reservation_id: new_reservation.reservation_id,
                })
            }
        };
        CancelReplaceRejection {
            reason,
            unadopted_reservation,
        }
    }
}

/// 校验外部输入的类型、有效期和标志组合；不承诺实际可执行性。
///
/// 市价与止损的所有数量和保护模式均拒绝。当前载荷不承载尚未实现的
/// 自成交防护、价格带管理、市场状态或路由隔离配置，上层不得将这些请求降级接入。
pub fn validate_replacement_combination(
    kind: OrderType,
    tif: TimeInForce,
    flags: OrderFlags,
) -> Result<(), RejectReason> {
    if flags.reduce_only || (flags.post_only && tif != TimeInForce::Gtc) {
        return Err(RejectReason::InvalidOrderCombination);
    }
    match (kind, tif) {
        (
            OrderType::Limit,
            TimeInForce::Gtc
            | TimeInForce::Ioc
            | TimeInForce::Fok
            | TimeInForce::Day { .. }
            | TimeInForce::Gtd { .. },
        )
        | (OrderType::Iceberg, TimeInForce::Gtc) => Ok(()),
        _ => Err(RejectReason::InvalidOrderCombination),
    }
}

/// 限制调用方已定位活动旧单的类型；不查询订单簿或证明旧单确实活动。
///
/// 两种计量模式的止损市价旧单均使用 StopMarket，且都不在当前支持范围。
pub fn validate_cancel_replace_old_type(kind: OrderType) -> Result<(), RejectReason> {
    match kind {
        OrderType::Limit | OrderType::Iceberg | OrderType::StopLimit => Ok(()),
        _ => Err(RejectReason::InvalidOrderCombination),
    }
}
