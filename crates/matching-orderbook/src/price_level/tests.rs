//! 单档先进先出、局部故障时停止处理预检与独立结构模型的验证。
//!
//! 验证器和性质测试模型只在测试中存在，不是生产订单簿的不变量入口。

use super::*;
pub(super) use matching_domain::{OrderId, Qty, QueuePriority, Side};
pub(super) use proptest::prelude::*;
pub(super) use std::panic::{AssertUnwindSafe, catch_unwind};

#[path = "tests/support.rs"]
mod support;
use support::*;

#[path = "tests/model.rs"]
mod model;
#[path = "tests/partial_fill.rs"]
mod partial_fill;
#[path = "tests/pop_front.rs"]
mod pop_front;
#[path = "tests/push_back.rs"]
mod push_back;
#[path = "tests/unlink.rs"]
mod unlink;
