//! 订单簿的已验收参考模型与 P2 底层结构。
//!
//! `reference_order`、`incoming_order` 与 `reference_orderbook` 是 T02 的
//! `Vec + sort` 参考行为基线。T03 的 `order_index`、`order_node`、
//! `order_arena`、`price_level` 与 `queue_priority_allocator` 只提供
//! ProductionOrderBook 所需的槽位、intrusive FIFO 和 priority 基础结构；
//! 它们不构成完整订单命令、撮合、持久化或 market 管理实现。
//! T04 的 `production_orderbook` 已组合这些结构，提供只读查询、rest、
//! Cancel，以及完整 Limit GTC 的连续成交与成交后余量挂单。

/// T02：进入撮合流程但尚未 rest 的最小运行态订单。
pub mod incoming_order;
/// T03：保存稳定槽位与 LIFO free-list 的 Arena 容器。
pub mod order_arena;
/// T03：稳定但可复用的 Arena 槽位位置。
pub mod order_index;
/// T03：承载订单运行态和 intrusive 双向链接的 Arena 节点。
pub mod order_node;
/// T03：单价格档位的 FIFO 端点、聚合值与局部链表维护。
pub mod price_level;
/// T04：单市场生产簿容器组合、查询与完整 Limit GTC。
pub mod production_orderbook;
/// T03：market-local QueuePriority 的分配与 high-water 边界。
pub mod queue_priority_allocator;
/// T02：参考模型中持有剩余量与 priority 的 resting 订单。
pub mod reference_order;
/// T02：`Vec + sort` 的确定性参考订单簿与成交基线。
pub mod reference_orderbook;

/// T02 incoming 运行态订单。
pub use incoming_order::IncomingOrder;
/// T03 Arena 的槽位生命周期容器。
pub use order_arena::OrderArena;
/// T03 Arena 槽位位置，不是业务订单标识。
pub use order_index::OrderIndex;
/// T03 Arena 中的订单运行态节点。
pub use order_node::OrderNode;
/// T03 单价 FIFO 链表档位。
pub use price_level::PriceLevel;
/// T03 market-local priority policy、allocator 与初始化错误。
pub use queue_priority_allocator::{
    QueuePriorityAllocator, QueuePriorityInitError, QueuePriorityPolicy,
};
/// T02 参考 resting 订单。
pub use reference_order::ReferenceOrder;
/// T02 `Vec + sort` 参考订单簿。
pub use reference_orderbook::ReferenceOrderBook;

/// T04 生产簿组合，已接入挂单、撤单和完整 Limit GTC。
pub use production_orderbook::ProductionOrderBook;
