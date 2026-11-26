use matching_protocol::{LeaseAuthority, LeaseIdentity, LeaseOrderIdAllocator, OrderIdError};
use std::{cell::RefCell, rc::Rc};

struct State {
    /// 测试租约当前是否有效。
    valid: bool,
    /// 下一个可分配计数器；耗尽时为空。
    next: Option<u64>,
}
/// 模拟权威共享的编号分配状态。
struct Authority(Rc<RefCell<State>>);
impl LeaseAuthority for Authority {
    fn is_ready(&self, _: LeaseIdentity) -> bool {
        let state = self.0.borrow();
        state.valid && state.next.is_some()
    }
    fn next_counter(&mut self, _: LeaseIdentity) -> Result<u64, OrderIdError> {
        let mut state = self.0.borrow_mut();
        if !state.valid {
            return Err(OrderIdError::LeaseUnavailable);
        }
        let n = state.next.ok_or(OrderIdError::Exhausted)?;
        state.next = n.checked_add(1);
        Ok(n)
    }
}
#[test]
fn lease_order_ids_have_fixed_fields_and_exhaust_without_wrapping() {
    let state = Rc::new(RefCell::new(State {
        valid: true,
        next: Some(u64::MAX - 1),
    }));
    let identity = LeaseIdentity {
        cluster: 0x1234,
        node: 0x5678,
        epoch: 0x9abc_def0,
    };
    let mut a = LeaseOrderIdAllocator::new(identity, Authority(state.clone()));
    let mut b = LeaseOrderIdAllocator::new(identity, Authority(state));
    assert!(a.is_ready());
    assert_eq!(
        a.allocate().unwrap(),
        0x1234_5678_9abc_def0_ffff_ffff_ffff_fffe
    );
    assert_eq!(
        b.allocate().unwrap(),
        0x1234_5678_9abc_def0_ffff_ffff_ffff_ffff
    );
    assert_eq!(a.allocate(), Err(OrderIdError::Exhausted));
    assert!(!a.is_ready());
}
#[test]
fn expired_lease_never_allocates_or_consumes_counter() {
    let state = Rc::new(RefCell::new(State {
        valid: false,
        next: Some(4),
    }));
    let mut ids = LeaseOrderIdAllocator::new(
        LeaseIdentity {
            cluster: 1,
            node: 2,
            epoch: 3,
        },
        Authority(state.clone()),
    );
    assert!(!ids.is_ready());
    assert_eq!(ids.allocate(), Err(OrderIdError::LeaseUnavailable));
    assert_eq!(state.borrow().next, Some(4));
}

#[test]
fn restarted_allocator_uses_authoritys_retained_counter() {
    let state = Rc::new(RefCell::new(State {
        valid: true,
        next: Some(0),
    }));
    let identity = LeaseIdentity {
        cluster: 1,
        node: 2,
        epoch: 3,
    };
    let first = LeaseOrderIdAllocator::new(identity, Authority(state.clone()))
        .allocate()
        .unwrap();
    let next = LeaseOrderIdAllocator::new(identity, Authority(state))
        .allocate()
        .unwrap();
    assert_eq!(next, first.checked_add(1).unwrap());
}
