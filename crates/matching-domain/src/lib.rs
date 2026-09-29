//! 领域 ID、整数数值类型、订单与市场语义、命令、事件和错误模型的规划边界。

/// 当前参考生命周期使用的最小事件载荷。
pub mod engine_event;
/// 具有业务类型隔离的标识符包装。
pub mod ids;
/// 当前参考模型支持的最小 GTC 限价订单。
pub mod limit_gtc_order;
/// 不依赖 wall clock 的逻辑时间值。
pub mod logical_time;
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
/// 最小 GTC 限价订单值对象。
pub use limit_gtc_order::LimitGtcOrder;
/// 确定性逻辑时间值。
pub use logical_time::LogicalTime;
/// 整数价格、数量和金额值对象。
pub use numbers::{Price, Qty, QuoteAmount, SignedAmount};
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
