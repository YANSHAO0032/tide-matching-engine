/// 订单方向。
///
/// 它只表达买卖方向；价格排序、撮合资格和持仓影响由相应组件决定。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Side {
    /// 买入方向。
    Buy,
    /// 卖出方向。
    Sell,
}

#[cfg(test)]
mod tests {
    //! 验证方向枚举的基础值语义。
    use super::Side;

    #[test]
    fn buy_and_sell_are_distinct() {
        assert_eq!(Side::Buy, Side::Buy);
        assert_eq!(Side::Sell, Side::Sell);
        assert_ne!(Side::Buy, Side::Sell);
    }

    #[test]
    fn side_is_copyable_and_debuggable() {
        let buy = Side::Buy;
        let copied = buy;
        assert_eq!(buy, copied);
        assert_eq!(format!("{buy:?}"), "Buy");
        assert_eq!(format!("{:?}", Side::Sell), "Sell");
    }
}
