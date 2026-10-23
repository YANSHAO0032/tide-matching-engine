/// 未触发 Stop 在同一 StopLevel 中的市场本地触发优先级。
///
/// 此值不代表普通订单簿的 QueuePriority；Stop 触发前绝不持有后者。
#[repr(transparent)]
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct StopPriority(u128);

impl StopPriority {
    /// 从已由市场分配器生成的原始数值构造优先级。
    pub const fn new(value: u128) -> Self {
        Self(value)
    }

    /// 读取优先级原始数值，仅用于审计、快照和不变量校验。
    pub const fn get(self) -> u128 {
        self.0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn stop_priority_preserves_order_and_value() {
        assert!(StopPriority::new(1) < StopPriority::new(2));
        assert_eq!(StopPriority::new(u128::MAX).get(), u128::MAX);
    }
}
