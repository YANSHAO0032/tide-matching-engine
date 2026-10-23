use super::*;

pub(super) fn price(value: i64) -> Price {
    Price::try_new(value).unwrap()
}

#[path = "tests/order_values.rs"]
mod order_values;
#[path = "tests/protection_context.rs"]
mod protection_context;
#[path = "tests/protection_price.rs"]
mod protection_price;
#[path = "tests/protection_source.rs"]
mod protection_source;
#[path = "tests/quote_execution.rs"]
mod quote_execution;
