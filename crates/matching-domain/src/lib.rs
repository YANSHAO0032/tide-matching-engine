//! 领域 ID、整数数值、市场金额换算、订单载荷、执行结果与稳定错误码。
//!
//! 当前提供撮合所需的领域子集，不包含完整命令 envelope、资金账本或持久化协议。

/// 当前订单簿使用的最小生命周期与成交事件载荷。
pub mod engine_event;
/// 具有业务类型隔离的标识符包装。
pub mod ids;
/// IOC/FOK 的终结结果与当前基础订单组合校验。
pub mod limit_execution;
/// 限价身份、价格和数量载荷，执行策略由订单簿入口选择。
pub mod limit_gtc_order;
/// 不依赖 wall clock 的逻辑时间值。
pub mod logical_time;
/// 市场整数金额换算的已验证配置与内部错误分类。
pub mod market_math;
/// 市价输入、执行结果与固定参考价的保护边界。
pub mod market_order;
/// 价格、数量和金额的整数值对象及 checked 算术。
pub mod numbers;
/// 附加订单约束标志。
pub mod order_flags;
/// 订单生命周期状态标签。
pub mod order_state;
/// 订单类型标签。
pub mod order_type;
/// 市场本地 FIFO priority 值对象。
pub mod queue_priority;
/// 稳定编号的拒绝原因。
pub mod reject_reason;
/// 买卖方向。
pub mod side;
/// 有效期策略参数。
pub mod time_in_force;
/// 实际成交的最小载荷。
pub mod trade_event;

/// 对外公开的集群、市场、用户、订单、命令、成交和预留身份。
pub use ids::{ClusterId, CommandId, MarketId, OrderId, ReservationId, TradeId, UserId};

/// 最小订单生命周期事件。
pub use engine_event::EngineEvent;
/// 限价立即执行结果、稳定取消原因与基础订单组合校验。
pub use limit_execution::{
    CancelReason, LimitImmediateExecution, validate_basic_order_combination,
};
/// 保留历史类型名的最小限价订单载荷。
pub use limit_gtc_order::LimitGtcOrder;
/// 确定性逻辑时间值。
pub use logical_time::LogicalTime;
/// 整数价格、数量和金额值对象。
pub use numbers::{Price, Qty, QuoteAmount, QuoteBudget, SignedAmount};
/// 订单附加约束标志。
pub use order_flags::OrderFlags;
/// 订单生命周期状态标签。
pub use order_state::OrderState;
/// 订单类型标签。
pub use order_type::OrderType;
/// 市场本地 FIFO priority 值。
pub use queue_priority::QueuePriority;
/// 稳定编号的业务拒绝原因。
pub use reject_reason::RejectReason;
/// 买卖方向。
pub use side::Side;
/// 订单有效期策略。
pub use time_in_force::TimeInForce;
/// 一笔实际成交的载荷。
pub use trade_event::TradeEvent;

/// 市场整数金额换算的已验证配置与构造错误分类。
pub use market_math::{MarketMath, MathConfigError, MathError};

/// 市价输入、报价执行结果与保护参数的常用重导出。
pub use market_order::{
    MarketByBase, MarketByQuote, MarketByQuoteExecution, MarketProtection, ProtectionSource,
};
