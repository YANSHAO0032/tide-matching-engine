//! 独立参考订单簿、生产订单簿及其 Arena/FIFO/priority 基础结构。
//!
//! Reference 使用 `Vec + sort`；Production 使用 BTreeMap 价格索引、HashMap
//! 身份定位和 Arena 内 intrusive FIFO。两者独立实现市价、限价 IOC/FOK/PostOnly；
//! Production 另有自动挂入余量的完整 Limit GTC 入口，Reference 的底层限价撮合不自动 rest。
//! 市价与 IOC/FOK 不挂单、不分配 priority；PostOnly 只在成功 rest 时分配。
//! 本 crate 不包含完整命令生命周期、资金闭环、持久化或 market 管理。

/// 进入撮合流程但尚未 rest 的最小运行态订单。
pub mod incoming_order;
/// Reference 与 Production 共享的只读市价计划数据；不含撮合算法。
mod market_plan;
/// 保存稳定槽位与 LIFO free-list 的 Arena 容器。
pub mod order_arena;
/// 稳定但可复用的 Arena 槽位位置。
pub mod order_index;
/// 承载订单运行态和 intrusive 双向链接的 Arena 节点。
pub mod order_node;
/// 单价格档位的 FIFO 端点、聚合值与局部链表维护。
pub mod price_level;
/// 单市场生产簿的查询、Cancel、限价和市价执行。
pub mod production_orderbook;
/// market-local QueuePriority 的分配与 high-water 边界。
pub mod queue_priority_allocator;
/// 参考模型中持有剩余量与 priority 的 resting 订单。
pub mod reference_order;
/// `Vec + sort` 的确定性参考订单簿与成交基线。
pub mod reference_orderbook;

/// incoming 运行态订单。
pub use incoming_order::IncomingOrder;
/// Arena 的槽位生命周期容器。
pub use order_arena::OrderArena;
/// Arena 槽位位置，不是业务订单标识。
pub use order_index::OrderIndex;
/// Arena 中的订单运行态节点。
pub use order_node::OrderNode;
/// 单价 FIFO 链表档位。
pub use price_level::PriceLevel;
/// market-local priority policy、allocator 与初始化错误。
pub use queue_priority_allocator::{
    QueuePriorityAllocator, QueuePriorityInitError, QueuePriorityPolicy,
};
/// 参考 resting 订单。
pub use reference_order::ReferenceOrder;
/// `Vec + sort` 参考订单簿。
pub use reference_orderbook::ReferenceOrderBook;

/// 生产簿的挂单、撤单、限价和市价执行入口。
pub use production_orderbook::ProductionOrderBook;
