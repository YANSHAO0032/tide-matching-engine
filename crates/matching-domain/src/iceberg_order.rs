use crate::{LimitGtcOrder, Qty, RejectReason};

/// 冰山限价订单的总量载荷与显示上限。
///
/// 构造时保证 `0 < peak_size < order.qty`；字段保持私有，防止绕过该关系。
/// 价格、总量和显示上限的市场价格步长/数量步长对齐及本金由执行入口使用 MarketMath 校验。
/// GTC 与 PostOnly 由所调用的入口选择，不接受 IOC/FOK。
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct IcebergOrder {
    /// 冰山订单复用的基础限价订单载荷。
    order: LimitGtcOrder,
    /// 每次展示在订单簿深度中的严格正峰值数量。
    peak_size: Qty,
}

impl IcebergOrder {
    /// 拒绝显示上限等于或大于总量的输入；严格正值已由 Qty 保证。
    pub fn try_new(order: LimitGtcOrder, peak_size: Qty) -> Result<Self, RejectReason> {
        if peak_size >= order.qty {
            return Err(RejectReason::InvalidIceberg);
        }
        Ok(Self { order, peak_size })
    }

    /// 原始订单身份、限价与总下单量；不会随补量或成交改变。
    pub fn order(&self) -> &LimitGtcOrder {
        &self.order
    }

    /// 首次挂单及每次补量所允许的最大可见数量。
    pub fn peak_size(&self) -> Qty {
        self.peak_size
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{OrderId, Price, Side, UserId};

    fn order(total: u64) -> LimitGtcOrder {
        LimitGtcOrder {
            order_id: OrderId::new(1),
            user_id: UserId::new(2),
            side: Side::Sell,
            price: Price::try_new(100).unwrap(),
            qty: Qty::try_new(total).unwrap(),
        }
    }

    #[test]
    fn iceberg_requires_positive_peak_strictly_below_total() {
        assert_eq!(Qty::try_new(0), Err(RejectReason::InvalidQty));
        for (total, peak) in [(1, 1), (10, 10), (10, 11), (u64::MAX, u64::MAX)] {
            assert_eq!(
                IcebergOrder::try_new(order(total), Qty::try_new(peak).unwrap()),
                Err(RejectReason::InvalidIceberg)
            );
        }
        for (total, peak) in [(2, 1), (10, 3), (u64::MAX, u64::MAX - 1)] {
            let input = order(total);
            let iceberg =
                IcebergOrder::try_new(input.clone(), Qty::try_new(peak).unwrap()).unwrap();
            assert_eq!(iceberg.order(), &input);
            assert_eq!(iceberg.peak_size().get(), peak);
        }
        assert_eq!(RejectReason::InvalidIceberg as u16, 2011);
    }
}
