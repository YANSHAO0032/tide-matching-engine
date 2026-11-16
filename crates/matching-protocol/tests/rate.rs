use matching_protocol::{BucketConfig, RateError, RateLimiter};

#[test]
fn rate_limits_all_three_dimensions_without_partial_charge() {
    let config = BucketConfig {
        capacity: 1,
        refill_interval: 10,
    };
    let mut limiter = RateLimiter::new(config, config, config, 2).unwrap();
    limiter.check(1, 10, 0).unwrap();
    assert_eq!(limiter.check(2, 20, 0), Err(RateError::Limited));
    limiter.check(2, 20, 10).unwrap();
    assert_eq!(limiter.check(1, 10, 10), Err(RateError::Limited));
}

#[test]
fn table_capacity_and_clock_regression_are_explicit() {
    let config = BucketConfig {
        capacity: 2,
        refill_interval: 5,
    };
    let mut limiter = RateLimiter::new(config, config, config, 1).unwrap();
    limiter.check(1, 1, 10).unwrap();
    assert_eq!(limiter.check(2, 1, 10), Err(RateError::Capacity));
    assert_eq!(limiter.check(1, 1, 9), Err(RateError::ClockRegression));
    limiter.check(1, 1, 10).unwrap();
}

#[test]
fn key_and_user_limits_are_independent_and_failure_does_not_spend_global() {
    let global = BucketConfig {
        capacity: 3,
        refill_interval: 100,
    };
    let small = BucketConfig {
        capacity: 1,
        refill_interval: 100,
    };
    let mut limiter = RateLimiter::new(global, small, small, 4).unwrap();
    limiter.check(1, 10, 0).unwrap();
    assert_eq!(limiter.check(1, 20, 0), Err(RateError::Limited));
    assert_eq!(limiter.check(2, 10, 0), Err(RateError::Limited));
    limiter.check(2, 20, 0).unwrap();
    limiter.check(3, 30, 0).unwrap();
    assert_eq!(limiter.check(4, 40, 0), Err(RateError::Limited));
}

#[test]
fn staged_intents_cannot_accumulate_submit_burst_across_users() {
    use matching_protocol::GatewayRateLimiters;
    let config = BucketConfig {
        capacity: 1,
        refill_interval: 100,
    };
    let mut limits = GatewayRateLimiters::new(config, config, config, 4).unwrap();
    limits.check_prepare(1, 10, 0).unwrap();
    limits.check_prepare(2, 20, 100).unwrap();
    limits.check_submit(1, 10, 100).unwrap();
    assert_eq!(limits.check_submit(2, 20, 100), Err(RateError::Limited));
    limits.check_submit(2, 20, 200).unwrap();
}
