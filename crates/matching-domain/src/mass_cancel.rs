//! 分片本地批量撤单的范围协议。
use crate::{MarketId, OriginSessionId, UserId};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
/// 分片内批量撤单的目标范围；跨分片路由与控制围栏不属于该值对象。
pub enum ShardCancelScope {
    /// 匹配指定用户的全部本地活动订单。
    User {
        /// 需要匹配的用户身份。
        user_id: UserId,
    },
    /// 匹配指定市场的全部本地活动订单。
    Market {
        /// 需要匹配的市场身份。
        market_id: MarketId,
    },
    /// 同时匹配指定用户和指定市场的本地活动订单。
    UserMarket {
        /// 需要匹配的用户身份。
        user_id: UserId,
        /// 需要匹配的市场身份。
        market_id: MarketId,
    },
    /// 按用户及订单接入来源会话精确匹配本地活动订单。
    Session {
        /// 需要匹配的用户身份。
        user_id: UserId,
        /// 下单时持久化的接入来源会话，而非 DAY 交易时段。
        session_id: OriginSessionId,
    },
    /// 匹配当前分片执行单元的全部活动订单。
    All,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
/// 分片内批量撤单命令的最小载荷。
pub struct MassCancelShard {
    /// 本次撤单使用的身份范围。
    pub scope: ShardCancelScope,
}

/// 分片内批量撤单的命令结果；逐订单细节由有序生命周期事件给出。
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct ShardMassCancelResult {
    /// 本次实际终结的活动订单数量。
    pub canceled_orders: u64,
}

/// 判断一张活动订单的持久化身份是否属于撤单范围。
///
/// 仅使用用户、市场和下单来源会话；不读取容器、价格、优先级或 TIF。
pub fn matches_shard_cancel_scope(
    scope: ShardCancelScope,
    user_id: UserId,
    market_id: MarketId,
    origin_session_id: OriginSessionId,
) -> bool {
    match scope {
        ShardCancelScope::User {
            user_id: scoped_user_id,
        } => user_id == scoped_user_id,
        ShardCancelScope::Market {
            market_id: scoped_market_id,
        } => market_id == scoped_market_id,
        ShardCancelScope::UserMarket {
            user_id: scoped_user_id,
            market_id: scoped_market_id,
        } => user_id == scoped_user_id && market_id == scoped_market_id,
        ShardCancelScope::Session {
            user_id: scoped_user_id,
            session_id,
        } => user_id == scoped_user_id && origin_session_id == session_id,
        ShardCancelScope::All => true,
    }
}
