/// 集群身份，用于区分独立的部署或复制域。
#[repr(transparent)]
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ClusterId(pub u16);

/// 市场身份；其有效范围和路由语义由上层市场目录定义。
#[repr(transparent)]
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct MarketId(pub u32);

/// 用户身份；它不是资金账户、订单或命令的替代标识。
#[repr(transparent)]
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct UserId(pub u64);

/// 订单身份
///
/// 数值顺序不表示 FIFO、创建时间或撮合优先级。
///
/// 不同业务 ID 不可互换：
///
/// ```compile_fail
/// use matching_domain::{OrderId, TradeId};
///
/// fn process_trade(id: TradeId) {}
///
/// let order_id = OrderId::new(1);
/// process_trade(order_id);
/// ```
#[repr(transparent)]
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct OrderId(pub u128);

/// 客户端命令身份，用于幂等请求及其审计关联。
///
/// 它不是订单、成交或 durable sequence，不能据数值顺序推导处理次序。
#[repr(transparent)]
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct CommandId(pub u128);

/// 成交身份，用于唯一标识一笔已经发生的交易。
///
/// 它不是订单优先级或可排序的业务时间。
#[repr(transparent)]
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct TradeId(pub u128);

/// 资金预留身份，用于关联风控层保存的预留记录。
#[repr(transparent)]
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ReservationId(pub u128);

// 仅为现有整数提供类型安全的封装和读取能力。
// 不提供 ID 分配、算术、非零约束或唯一性校验。
macro_rules! impl_id {
    ($name:ident, $inner:ty) => {
        impl $name {
            /// 从调用方已持有的原始数值构造 ID，不分配或验证其唯一性。
            pub const fn new(value: $inner) -> Self {
                Self(value)
            }

            /// 读取底层数值；该数值不携带额外的顺序语义。
            pub const fn get(self) -> $inner {
                self.0
            }
        }
    };
}

impl_id!(ClusterId, u16);
impl_id!(MarketId, u32);
impl_id!(UserId, u64);
impl_id!(OrderId, u128);
impl_id!(CommandId, u128);
impl_id!(TradeId, u128);
impl_id!(ReservationId, u128);

#[cfg(test)]
mod tests {
    //! 验证透明 ID 包装的值语义与整数边界保真。
    use super::*;
    use std::collections::HashSet;

    macro_rules! test_id {
        ($test_name:ident,$id:ident,$inner:ty) => {
            #[test]
            fn $test_name() {
                let zero = $id::new(0);
                let one = $id::new(1);
                let max = $id::new(<$inner>::MAX);

                assert_eq!(zero.get(), 0);
                assert_eq!(one.get(), 1);
                assert_eq!(max.get(), <$inner>::MAX);

                assert_eq!($id::new(1), one);
                assert_ne!(zero, one);
                assert_ne!(one, max);

                //自然数值排序
                let mut values = [max, one, zero];
                values.sort();
                assert_eq!(values, [zero, one, max]);

                //相等的ID在HashSet中视为同一个值
                let mut set = HashSet::new();
                assert!(set.insert(one));
                assert!(!set.insert($id::new(1)));
                assert_eq!(set.len(), 1);

                // Clone、Copy 和 Debug 均可用
                let copied = one;
                let cloned = one.clone();
                assert_eq!(copied, cloned);
                assert!(!format!("{one:?}").is_empty());

                // repr(transparent) 不引入额外存储大小
                assert_eq!(std::mem::size_of::<$id>(), std::mem::size_of::<$inner>());
            }
        };
    }

    test_id!(cluster_id_behavior, ClusterId, u16);
    test_id!(market_id_behavior, MarketId, u32);
    test_id!(user_id_behavior, UserId, u64);
    test_id!(order_id_behavior, OrderId, u128);
    test_id!(command_id_behavior, CommandId, u128);
    test_id!(trade_id_behavior, TradeId, u128);
    test_id!(reservation_id_behavior, ReservationId, u128);

    // 专门防止 u128 被错误地截断为 u64
    macro_rules! test_u128_high_bits {
        ($test_name:ident,$id:ident) => {
            #[test]
            fn $test_name() {
                let value = (1u128 << 127) | (1u128 << 96) | 42;
                let id = $id::new(value);
                assert_eq!(id.get(), value);
                assert!(id.get() > u64::MAX as u128);
            }
        };
    }

    test_u128_high_bits!(order_id_high_bits, OrderId);
    test_u128_high_bits!(command_id_high_bits, CommandId);
    test_u128_high_bits!(trade_id_high_bits, TradeId);
    test_u128_high_bits!(reservation_id_high_bits, ReservationId);
}
