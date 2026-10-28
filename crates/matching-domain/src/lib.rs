//! 领域标识符、整数数值、市场金额换算、订单载荷、执行结果与稳定错误码。
//!
//! 当前提供撮合所需的领域子集，不包含完整命令封装、资金账本或持久化协议。

/// 改单绝对目标、显式补丁与纯候选计算。
pub mod amend_order;
/// 撤单替换的独立新订单、资金过渡及拒绝结果。
pub mod cancel_replace;
/// 当前订单簿使用的最小生命周期与成交事件载荷。
pub mod engine_event;
/// 订单到期的稳定业务原因。
pub mod expire_reason;
/// 订单到期的最小业务结果。
pub mod expired_order;
/// 已验证显示量与总量关系的冰山限价订单输入。
pub mod iceberg_order;
/// 具有业务类型隔离的标识符包装。
pub mod ids;
/// IOC/FOK 的终结结果与当前基础订单组合校验。
pub mod limit_execution;
/// 限价身份、价格和数量载荷，执行策略由订单簿入口选择。
pub mod limit_gtc_order;
/// 不依赖系统时钟的逻辑时间值。
pub mod logical_time;
/// 市场整数金额换算的已验证配置与内部错误分类。
pub mod market_math;
/// 市价输入、执行结果与固定参考价的保护边界。
pub mod market_order;
/// 价格、数量和金额的整数值对象及带溢出检查的算术。
pub mod numbers;
/// 附加订单约束标志。
pub mod order_flags;
/// 订单生命周期状态标签。
pub mod order_state;
/// 订单类型标签。
pub mod order_type;
pub mod price_reference;
/// 市场本地先进先出优先级值对象。
pub mod queue_priority;
/// 稳定编号的拒绝原因。
pub mod reject_reason;
/// 资金预留调整令牌与规范签名载荷。
pub mod reservation_adjustment;
/// 订单终结时释放本地预留的稳定业务原因。
pub mod reservation_release_reason;
/// 独立新预留的规范签名授权载荷。
pub mod reservation_token;
/// 买卖方向。
pub mod side;
/// 未触发 Stop 的输入载荷和触发后 child 的数量模式。
pub mod stop_order;
/// 未触发 Stop 的市场本地触发优先级值。
pub mod stop_priority;
/// 有效期策略参数。
pub mod time_in_force;
/// 实际成交的最小载荷。
pub mod trade_event;
/// 未触发 Stop 独立于 child TIF 的存活期。
pub mod trigger_expiry;

/// 对外公开的集群、市场、用户、订单、命令、成交和预留身份。
pub use ids::{AssetId, ClusterId, CommandId, MarketId, OrderId, ReservationId, TradeId, UserId};

/// 改单载荷、数量模式及纯候选计算。
pub use amend_order::{
    AmendOrder, AmendQuantityMode, AmendedQuantity, MarketProtectionPatch, RegularAmendKind,
    TriggerExpiryPatch, classify_regular_amend, remaining_after_target_total,
    validate_amend_quantity_mode,
};
/// 撤单替换的最小稳定领域协议与纯结构校验。
pub use cancel_replace::{
    CancelReplaceOrder, CancelReplaceRejection, ReplacementOrder, ReplacementOrderKind,
    ReservationTransition, UnadoptedReservation, validate_cancel_replace_old_type,
    validate_replacement_combination,
};
/// 最小订单生命周期事件。
pub use engine_event::EngineEvent;
/// 订单到期的稳定业务原因。
pub use expire_reason::ExpireReason;
/// 到期订单的最小业务结果。
pub use expired_order::ExpiredOrder;
/// 包含总量与严格较小显示上限的冰山限价输入。
pub use iceberg_order::IcebergOrder;
/// 限价立即执行结果、稳定取消原因与基础订单组合校验。
pub use limit_execution::{
    CancelReason, LimitImmediateExecution, validate_basic_order_combination,
};
/// 保留历史类型名的最小限价订单载荷。
pub use limit_gtc_order::LimitGtcOrder;
/// 确定性逻辑时间值。
pub use logical_time::{LogicalClock, LogicalTime};
/// 整数价格、数量和金额值对象。
pub use numbers::{Price, Qty, QuoteAmount, QuoteBudget, SignedAmount};
/// 订单附加约束标志。
pub use order_flags::OrderFlags;
/// 订单生命周期状态标签。
pub use order_state::OrderState;
/// 订单类型标签。
pub use order_type::OrderType;
pub use price_reference::{PriceReference, PriceReferenceKind, PriceReferenceState};
/// 市场本地先进先出优先级值。
pub use queue_priority::QueuePriority;
/// 稳定编号的业务拒绝原因。
pub use reject_reason::RejectReason;
/// 按资产排序的目标预留与签名授权令牌。
pub use reservation_adjustment::{AssetReserve, ReservationAdjustmentToken};
/// 订单终结时释放本地预留的稳定原因。
pub use reservation_release_reason::ReservationReleaseReason;
/// 独立新资金预留的签名授权。
pub use reservation_token::ReservationToken;
/// 买卖方向。
pub use side::Side;
/// 未触发 Stop 的领域订单和数量模式。
pub use stop_order::{
    StopLimitOrder, StopMarketOrder, StopMarketQuantity, StopOrder, TriggeredOrderKind,
};
/// 未触发 Stop 的市场本地触发优先级。
pub use stop_priority::StopPriority;
/// 订单有效期策略。
pub use time_in_force::TimeInForce;
/// 一笔实际成交的载荷。
pub use trade_event::TradeEvent;
/// Stop 触发前的存活期。
pub use trigger_expiry::TriggerExpiry;

/// 市场整数金额换算的已验证配置与构造错误分类。
pub use market_math::{MarketMath, MathConfigError, MathError};

/// 市价输入、报价执行结果与保护参数的常用重导出。
pub use market_order::{
    MarketByBase, MarketByQuote, MarketByQuoteExecution, MarketProtection, ProtectionSource,
};
