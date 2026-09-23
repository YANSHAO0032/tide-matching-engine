//! 领域 ID、整数数值类型、订单与市场语义、命令、事件和错误模型的规划边界。

pub mod engine_event;
pub mod ids;
pub mod limit_gtc_order;
pub mod logical_time;
pub mod numbers;
pub mod order_flags;
pub mod order_state;
pub mod order_type;
pub mod reject_reason;
pub mod side;
pub mod time_in_force;
pub mod trade_event;

pub use ids::{ClusterId, CommandId, MarketId, OrderId, ReservationId, TradeId, UserId};

pub use engine_event::EngineEvent;
pub use limit_gtc_order::LimitGtcOrder;
pub use logical_time::LogicalTime;
pub use numbers::{Price, Qty, QuoteAmount, SignedAmount};
pub use order_flags::OrderFlags;
pub use order_state::OrderState;
pub use order_type::OrderType;
pub use reject_reason::RejectReason;
pub use side::Side;
pub use time_in_force::TimeInForce;
pub use trade_event::TradeEvent;
