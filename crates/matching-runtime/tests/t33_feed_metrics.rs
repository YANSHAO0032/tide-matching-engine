//! 行情水位和源缺口的可观测性必须来自实际构建器。
use matching_domain::{ClusterId, MarketId, OrderId, Price, Qty, ShardId, Side};
use matching_runtime::{
    FeedBuilder, FeedMonitor, Health, HealthDependency, MboError, RouteTable, Shard, SourceEvent,
    SourcePayload,
};
use std::time::Duration;

#[test]
fn feed_watermarks_and_gaps_follow_real_builder_without_advancing_on_error() {
    let market = MarketId(1234);
    let mut feed = FeedMonitor::new(FeedBuilder::new(market));
    let mut shard = Shard::start(
        ShardId(1234),
        ClusterId(1234),
        RouteTable::new([]).unwrap(),
        vec![],
    )
    .unwrap();
    let observed = feed.telemetry();
    assert!(
        Health::new(vec![shard.handle()])
            .unwrap()
            .with_feeds(vec![observed.clone(), observed.clone()], 2)
            .is_err()
    );
    assert!(
        Health::new(vec![shard.handle()])
            .unwrap()
            .with_feeds(vec![observed.clone()], 0)
            .is_err()
    );
    let health = Health::new(vec![shard.handle()])
        .unwrap()
        .with_feeds(vec![observed], 1)
        .unwrap();
    for dependency in [
        HealthDependency::Recovery,
        HealthDependency::Persistence,
        HealthDependency::Control,
    ] {
        health.set_healthy(dependency, true);
    }
    let event = SourceEvent {
        market_id: market,
        sequence: 1,
        payload: SourcePayload::OrderAdded {
            order_id: OrderId(1),
            side: Side::Buy,
            price: Price::try_new(10).unwrap(),
            visible: Qty::try_new(2).unwrap(),
        },
    };
    feed.apply(&event).unwrap();
    assert!(health.ready());
    let text = health.prometheus().unwrap();
    assert!(text.contains("market_event_last_seq{market_id=\"1234\"} 1\n"));
    assert!(text.contains("market_data_feed_seq_last{market_id=\"1234\",feed=\"mbo\"} 1\n"));
    let gap = SourceEvent {
        sequence: 3,
        ..event
    };
    assert_eq!(feed.apply(&gap), Err(MboError::SourceGap));
    assert!(health.ready());
    let text = health.prometheus().unwrap();
    assert!(text.contains("market_event_sequence_gap_total{market_id=\"1234\"} 1\n"));
    assert!(text.contains("market_event_last_seq{market_id=\"1234\"} 1\n"));
    assert!(text.contains("market_data_builder_valid{market_id=\"1234\"} 0\n"));
    let next = SourceEvent { sequence: 2, ..gap };
    assert_eq!(feed.apply(&next), Err(MboError::InvalidSource));
    drop(feed);
    shard.shutdown(Duration::from_secs(1)).unwrap();
}

#[test]
fn dropped_builder_does_not_leave_a_stale_valid_metric() {
    let mut shard = Shard::start(
        ShardId(1235),
        ClusterId(1235),
        RouteTable::new([]).unwrap(),
        vec![],
    )
    .unwrap();
    let feed = FeedMonitor::new(FeedBuilder::new(MarketId(1235)));
    let health = Health::new(vec![shard.handle()])
        .unwrap()
        .with_feeds(vec![feed.telemetry()], 1)
        .unwrap();
    assert!(
        health
            .prometheus()
            .unwrap()
            .contains("market_data_builder_valid{market_id=\"1235\"} 1\n")
    );
    drop(feed);
    assert!(
        health
            .prometheus()
            .unwrap()
            .contains("market_data_builder_valid{market_id=\"1235\"} 0\n")
    );
    shard.shutdown(Duration::from_secs(1)).unwrap();
}

#[test]
fn multiple_markets_emit_each_metric_family_as_one_group() {
    let mut shard = Shard::start(
        ShardId(1236),
        ClusterId(1236),
        RouteTable::new([]).unwrap(),
        vec![],
    )
    .unwrap();
    let first = FeedMonitor::new(FeedBuilder::new(MarketId(1236)));
    let second = FeedMonitor::new(FeedBuilder::new(MarketId(1237)));
    let health = Health::new(vec![shard.handle()])
        .unwrap()
        .with_feeds(vec![first.telemetry(), second.telemetry()], 2)
        .unwrap();
    let text = health.prometheus().unwrap();
    let mut previous = "";
    let mut completed = std::collections::HashSet::new();
    for line in text.lines().filter(|line| line.starts_with("market_")) {
        let name = line.split('{').next().unwrap();
        if name != previous {
            assert!(!completed.contains(name), "指标族不连续：{name}");
            completed.insert(previous);
            previous = name;
        }
    }
    assert_eq!(completed.len(), 4);
    shard.shutdown(Duration::from_secs(1)).unwrap();
}

#[test]
fn feed_gap_metric_counts_real_cursor_gaps_not_duplicates_or_market_errors() {
    use matching_runtime::{FeedCursorMonitor, FeedKind, ResumeError};
    let mut shard = Shard::start(
        ShardId(1238),
        ClusterId(1238),
        RouteTable::new([]).unwrap(),
        vec![],
    )
    .unwrap();
    let market = MarketId(1238);
    let mut cursor = FeedCursorMonitor::new(market, 0, FeedKind::Trade);
    let mut other = FeedCursorMonitor::new(market, 0, FeedKind::Trade);
    let telemetry = cursor.telemetry();
    assert!(
        Health::new(vec![shard.handle()])
            .unwrap()
            .with_feed_cursors(vec![telemetry.clone(), telemetry.clone()], 2)
            .is_err()
    );
    assert!(
        Health::new(vec![shard.handle()])
            .unwrap()
            .with_feed_cursors(vec![telemetry.clone()], 0)
            .is_err()
    );
    let health = Health::new(vec![shard.handle()])
        .unwrap()
        .with_feed_cursors(vec![telemetry, other.telemetry()], 2)
        .unwrap();
    assert_eq!(cursor.accept(market, 1), Ok(true));
    assert_eq!(cursor.accept(market, 1), Ok(false));
    assert_eq!(cursor.accept(MarketId(1), 3), Err(ResumeError::Market));
    assert_eq!(cursor.accept(market, 3), Err(ResumeError::Gap));
    assert_eq!(cursor.accept(market, 2), Ok(true));
    assert_eq!(cursor.accept(market, 4), Err(ResumeError::Gap));
    assert_eq!(other.accept(market, 3), Err(ResumeError::Gap));
    let text = health.prometheus().unwrap();
    assert!(text.contains("market_data_feed_gap_total{feed=\"trade\"} 3\n"));
    assert!(!text.contains("market_data_feed_gap_total{feed=\"bbo\"}"));
    shard.shutdown(Duration::from_secs(1)).unwrap();
}
