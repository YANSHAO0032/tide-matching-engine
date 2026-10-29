//! 独立参考订单簿、生产订单簿及其 Arena/先进先出/优先级基础结构。
//!
//! 参考簿使用 `Vec + sort`；生产簿使用 BTreeMap 价格索引、HashMap
//! 身份定位和 Arena 内侵入式先进先出。两者独立实现市价、限价 IOC/FOK/PostOnly 与冰山可见片生命周期；
//! 两者都有带事件的 GTC 余量挂单入口，参考簿的底层限价撮合仍不自动挂单。
//! 市价与 IOC/FOK 的新到订单不挂单、不分配优先级；冰山挂单补量会取得新优先级。
//! PostOnly 只在成功挂单时分配；冰山每次补量移动到同价队尾。
//! 本库不包含完整命令生命周期、资金闭环、持久化或市场管理。

mod amend_execution_context;
/// 进入撮合流程但尚未挂单的最小运行态。
pub mod incoming_order;
mod market_registry;
mod trigger_registry_context;
pub use amend_execution_context::{AmendExecutionContext, AmendRegistryContext};
/// 资金接管后的市价订单及可选保护上下文。
pub use market_registry::{MarketRegistryContext, MarketRegistryOrder, MarketRegistryResult};
/// Stop 触发及预留归属预演所需的确定性上下文。
pub use trigger_registry_context::TriggerRegistryContext;
/// 参考簿与生产簿共享的只读市价计划数据；不含撮合算法。
mod market_plan;
/// 保存稳定槽位与后进先出空闲链表的 Arena 容器。
pub mod order_arena;
/// 稳定但可复用的 Arena 槽位位置。
pub mod order_index;
/// 承载订单运行态和侵入式双向链接的 Arena 节点。
pub mod order_node;
/// 单价格档位的先进先出端点、聚合值与局部链表维护。
pub mod price_level;
/// 单市场生产簿的查询、取消、限价和市价执行。
pub mod production_orderbook;
/// 市场内 QueuePriority 的分配与高水位边界。
pub mod queue_priority_allocator;
/// 参考模型中持有剩余量与优先级的挂单订单。
pub mod reference_order;
/// `Vec + sort` 的确定性参考订单簿与成交基线。
pub mod reference_orderbook;
/// 未触发止损订单、触发优先级及触发前到期索引。
pub mod stop_book;
/// 未触发 Stop 的独立市场本地优先级分配器。
pub mod stop_priority_allocator;

/// 新到订单运行态。
pub use incoming_order::IncomingOrder;
/// Arena 的槽位生命周期容器。
pub use order_arena::OrderArena;
/// Arena 槽位位置，不是业务订单标识。
pub use order_index::OrderIndex;
/// Arena 中的订单运行态节点。
pub use order_node::OrderNode;
/// 单价先进先出链表档位。
pub use price_level::PriceLevel;
/// 市场内优先级策略、分配器与初始化错误。
pub use queue_priority_allocator::{
    QueuePriorityAllocator, QueuePriorityInitError, QueuePriorityPolicy,
};
/// 参考挂单订单。
pub use reference_order::ReferenceOrder;
/// `Vec + sort` 参考订单簿。
pub use reference_orderbook::{ReferenceBookWithout, ReferenceMassCancelPlan, ReferenceOrderBook};
/// 未触发止损订单的市场本地容器。
pub use stop_book::StopBook;
/// StopPriority 的高水位保护分配器。
pub use stop_priority_allocator::{StopPriorityAllocator, StopPriorityInitError};

/// 生产簿的挂单、撤单、限价和市价执行入口。
pub use production_orderbook::{
    ProductionBookWithout, ProductionMassCancelPlan, ProductionOrderBook,
};
