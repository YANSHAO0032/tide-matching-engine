#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Side {
    Buy,
    Sell,
}

#[cfg(test)]
mod tests {
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
