//! 验证命令与事件封装携带的业务身份。
use matching_domain::{
    ClusterId, Command, CommandEnvelope, CommandId, EngineEvent, EventEnvelope, MarketId,
    MassCancelShard, RejectReason, ShardCancelScope, ShardId, UserId,
};

#[test]
fn command_and_event_keep_root_identity_and_order() {
    let command = CommandEnvelope {
        command_id: CommandId(12),
        session_id: 4,
        client_seq: 8,
        market_id: Some(MarketId(3)),
        user_id: Some(UserId(5)),
        route_epoch: Some(9),
        client_timestamp: Some(10),
        received_at_unix_ns: Some(11),
        payload: Command::MassCancelShard(MassCancelShard {
            scope: ShardCancelScope::UserMarket {
                user_id: UserId(5),
                market_id: MarketId(3),
            },
        }),
    };
    let event = EventEnvelope {
        cluster_id: ClusterId(1),
        shard_id: ShardId(2),
        shard_seq: 7,
        event_index: 0,
        command_id: command.command_id,
        engine_time: 6,
        market_id: command.market_id,
        payload: EngineEvent::CommandRejected {
            reason: RejectReason::InvalidQty,
        },
    };
    assert_eq!(event.command_id, CommandId(12));
    assert_eq!(
        (
            event.cluster_id,
            event.shard_id,
            event.shard_seq,
            event.event_index
        ),
        (ClusterId(1), ShardId(2), 7, 0)
    );
}

#[test]
fn existing_reason_codes_remain_fixed() {
    use matching_domain::{CancelReason, ExpireReason, ProtectionSource, ReservationReleaseReason};
    let reasons = [
        (RejectReason::MarketNotFound, 1001),
        (RejectReason::MigrationInProgress, 1008),
        (RejectReason::StaleRouteEpoch, 1009),
        (RejectReason::InvalidOrderCombination, 2001),
        (RejectReason::InvalidPrice, 2002),
        (RejectReason::InvalidQty, 2003),
        (RejectReason::InvalidTick, 2004),
        (RejectReason::InvalidLot, 2005),
        (RejectReason::DuplicateOrderId, 2008),
        (RejectReason::OrderNotFound, 2010),
        (RejectReason::InvalidIceberg, 2011),
        (RejectReason::InvalidExpireTime, 2013),
        (RejectReason::InvalidSession, 2014),
        (RejectReason::PostOnlyWouldCross, 2015),
        (RejectReason::FokInsufficientLiquidity, 2016),
        (RejectReason::PriceBandExceeded, 3001),
        (RejectReason::ReferencePriceUnavailable, 3002),
        (RejectReason::InvalidReferencePriceSource, 3003),
        (RejectReason::StaleReferencePriceUpdate, 3004),
        (RejectReason::MarketProtectionRequired, 3005),
        (RejectReason::MarketProtectionExceeded, 3006),
        (RejectReason::InvalidSlippageBps, 3007),
        (RejectReason::InsufficientReservation, 4001),
        (RejectReason::InvalidReservation, 4002),
        (RejectReason::FeeScheduleNotFound, 4003),
        (RejectReason::FeeScheduleMismatch, 4004),
        (RejectReason::ArithmeticOverflow, 5001),
        (RejectReason::EngineBusy, 5002),
        (RejectReason::PrioritySpaceExhaustion, 5006),
    ];
    for (reason, expected) in reasons {
        assert_eq!(reason as u16, expected);
    }
    for (reason, expected) in [
        (CancelReason::MassCancel, 1102),
        (CancelReason::SelfTradePreventionMaker, 1106),
        (CancelReason::SelfTradePreventionTaker, 1107),
        (CancelReason::SelfTradePreventionBoth, 1108),
        (CancelReason::CancelReplace, 1109),
        (CancelReason::IocRemainder, 1114),
    ] {
        assert_eq!(reason as u16, expected);
    }
    for (reason, expected) in [
        (ExpireReason::GtdReached, 1201),
        (ExpireReason::SessionEnded, 1202),
        (ExpireReason::StopTriggerGtdReached, 1203),
        (ExpireReason::StopTriggerSessionEnded, 1204),
        (ExpireReason::MarketClosed, 1205),
    ] {
        assert_eq!(reason as u16, expected);
    }
    for (reason, expected) in [
        (ReservationReleaseReason::FilledRemainder, 1301),
        (ReservationReleaseReason::UserCancel, 1302),
        (ReservationReleaseReason::MassCancel, 1303),
        (ReservationReleaseReason::TriggerRejected, 1305),
    ] {
        assert_eq!(reason as u16, expected);
    }
    for (source, expected) in [
        (ProtectionSource::User, 1),
        (ProtectionSource::MarketBand, 2),
        (ProtectionSource::Both, 3),
    ] {
        assert_eq!(source as u8, expected);
    }
}
