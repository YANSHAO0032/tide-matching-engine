//! 自成交防护的持久身份、吃单方策略和无副作用决策。

use crate::{RejectReason, UserId};

/// 由账户或网关固定到订单上的自成交防护分组身份。
///
/// `StpKey(0)` 不参与跨用户分组匹配；它不豁免同一用户的自成交防护。
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct StpKey(u64);

impl StpKey {
    /// 从持久化的无符号分组值构造身份。
    pub const fn new(value: u64) -> Self {
        Self(value)
    }

    /// 返回用于持久化和确定性比较的原始值。
    pub const fn get(self) -> u64 {
        self.0
    }

    /// 判断该身份是否参与跨用户的分组自成交防护。
    pub const fn participates_in_group_stp(self) -> bool {
        self.0 != 0
    }
}

/// 当前进入撮合的订单决定冲突处理方式的自成交防护策略。
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum StpMode {
    /// 当前吃单不执行自成交防护。
    None,
    /// 保留 maker 并终结吃单剩余量。
    CancelTaker,
    /// 终结当前 maker，并继续以原吃单余量寻找下一 maker。
    CancelMaker,
    /// 先终结当前 maker，再终结吃单剩余量。
    CancelBoth,
    /// 尚未冻结 venue 规则的模式，接收边界必须拒绝。
    DecrementAndCancel,
}

impl StpMode {
    /// 返回在命令进入订单簿前必须报告的稳定拒绝原因。
    pub const fn submission_rejection(self) -> Option<RejectReason> {
        match self {
            Self::DecrementAndCancel => Some(RejectReason::InvalidOrderCombination),
            Self::None | Self::CancelTaker | Self::CancelMaker | Self::CancelBoth => None,
        }
    }
}

/// 单个可成交 maker 与当前吃单订单之间的无副作用业务决定。
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum StpDecision {
    /// 不属于自成交身份，撮合器可以继续常规成交判断。
    NoConflict,
    /// 终结吃单剩余量。
    CancelTaker,
    /// 终结当前 maker 并继续扫描。
    CancelMaker,
    /// 终结当前 maker 和吃单剩余量。
    CancelBoth,
}

/// 仅依据吃单订单的策略和双方持久身份判断自成交防护动作。
///
/// 该函数不修改订单簿、预留或事件序列。只有同一用户，或双方均为非零且相同的
/// `StpKey` 才是冲突身份；maker 自己的 `StpMode` 不参与本次决策。
pub fn decide_stp(
    taker_user: UserId,
    taker_key: StpKey,
    taker_mode: StpMode,
    maker_user: UserId,
    maker_key: StpKey,
) -> StpDecision {
    assert!(
        !matches!(taker_mode, StpMode::DecrementAndCancel),
        "unsupported STP mode reached matching"
    );
    if matches!(taker_mode, StpMode::None) {
        return StpDecision::NoConflict;
    }

    let same_user = taker_user == maker_user;
    let same_non_zero_group = taker_key.participates_in_group_stp()
        && maker_key.participates_in_group_stp()
        && taker_key == maker_key;

    if !same_user && !same_non_zero_group {
        return StpDecision::NoConflict;
    }

    match taker_mode {
        StpMode::None => StpDecision::NoConflict,
        StpMode::CancelTaker => StpDecision::CancelTaker,
        StpMode::CancelMaker => StpDecision::CancelMaker,
        StpMode::CancelBoth => StpDecision::CancelBoth,
        StpMode::DecrementAndCancel => unreachable!("unsupported STP mode reached matching"),
    }
}
