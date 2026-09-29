use matching_domain::RejectReason;

/// `OrderArena` 内稳定槽位位置的紧凑表示。
///
/// 此值只定位当前 Arena 的一个物理槽位：它不是业务 `OrderId`、
/// 不是 generation，也不表达 FIFO 或 `QueuePriority`。释放并复用槽位后，
/// 同一数值可能指向不同订单；持有者必须先解除所有旧引用。
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct OrderIndex(u32);

impl OrderIndex {
    /// 将当前进程可寻址的 `usize` 槽位位置收窄为紧凑的 `u32` 表示。
    ///
    /// 超出 `u32` 表示范围时返回既有的 `ArithmeticOverflow`，不截断。
    /// 该转换不检查对应槽位是否存在或仍然存活。
    pub fn try_from_usize(value: usize) -> Result<Self, RejectReason> {
        let index = u32::try_from(value).map_err(|_| RejectReason::ArithmeticOverflow)?;
        Ok(Self(index))
    }

    /// 返回底层槽位数值。
    ///
    /// 仅用于 Arena 下标转换或测试观察；调用它不验证槽位存在、occupied，
    /// 或属于某个 `PriceLevel`。
    pub const fn get(self) -> u32 {
        self.0
    }
}

#[cfg(test)]
mod tests {
    //! `usize`/`u32` 表示边界与既有错误映射的覆盖。

    use super::*;
    use matching_domain::RejectReason;

    #[test]
    fn valid_boundaries_round_trip() {
        for value in [0usize, 1, u32::MAX as usize] {
            let index = OrderIndex::try_from_usize(value).unwrap();

            assert_eq!(index.get(), value as u32);
        }
    }

    #[cfg(target_pointer_width = "64")]
    #[test]
    fn out_of_range_returns_arithmetic_overflow() {
        // 使用 checked_add 构造首个无法表示的索引。
        let first_invalid = (u32::MAX as usize).checked_add(1).unwrap();

        for value in [first_invalid, usize::MAX] {
            assert_eq!(
                OrderIndex::try_from_usize(value),
                Err(RejectReason::ArithmeticOverflow)
            );
        }
    }
}
