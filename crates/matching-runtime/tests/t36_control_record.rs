use matching_domain::{CommandId, ShardId, UserId};
use matching_runtime::cross_control::{
    ControlAck, ControlKind, ControlOperation, ControlPhase, ControlUpdate,
};

#[test]
fn retries_fix_the_shard_identity_and_require_the_entire_ack_set() {
    let shards = [ShardId(2), ShardId(1)];
    let create = |targets: &[ShardId]| {
        ControlOperation::prepared(
            CommandId(7),
            ControlKind::UserMassCancel(UserId(4)),
            targets,
        )
    };
    let mut record = create(&shards).unwrap();
    assert_eq!(
        record.targets,
        create(&[shards[1], shards[0]]).unwrap().targets
    );
    assert_ne!(record.targets[&shards[0]], record.targets[&shards[1]]);
    assert!(create(&[shards[0], shards[0]]).is_err());
    assert_eq!(
        record.transition(ControlUpdate::Abort).unwrap().phase,
        ControlPhase::Aborted
    );
    record = record.transition(ControlUpdate::Next).unwrap();
    record.epoch = 1;
    assert_eq!(
        record.transition(ControlUpdate::Abort).unwrap().phase,
        ControlPhase::Aborted
    );
    for _ in 0..2 {
        record = record.transition(ControlUpdate::Next).unwrap();
    }
    assert_eq!(record.phase, ControlPhase::AwaitingAcks);
    let ack = ControlAck {
        shard_seq: 6,
        canceled: 3,
    };
    record = record
        .transition(ControlUpdate::Ack(shards[0], ack))
        .unwrap();
    assert!(record.transition(ControlUpdate::Next).is_err());
    assert!(record.transition(ControlUpdate::Abort).is_err());
    assert!(
        record
            .transition(ControlUpdate::Ack(ShardId(9), ack))
            .is_err()
    );
    record = record
        .transition(ControlUpdate::Ack(shards[1], ack))
        .unwrap();
    for _ in 0..3 {
        record = record.transition(ControlUpdate::Next).unwrap();
    }
    assert_eq!(record.phase, ControlPhase::Done);
    assert_eq!(record.total_canceled().unwrap(), 6);
    assert!(record.transition(ControlUpdate::Next).is_err());
}

/// 向量来自独立 SHA-256 实现，固定四类意图的字段顺序、整数端序与截断方向。
#[test]
fn derived_command_ids_match_the_wire_vectors() {
    let cases = [
        (
            ControlKind::UserMassCancel(UserId(4)),
            0x7edbf8ba383e426f013dc3af56c9a770,
        ),
        (
            ControlKind::GlobalMassCancel,
            0x05763e130253c613a9fdbb2c6e1c1556,
        ),
        (
            ControlKind::UserKill {
                user: UserId(4),
                active: true,
                cancel_all: false,
            },
            0x261113a85a9090eeecb21dbb6a3fbfc1,
        ),
        (
            ControlKind::GlobalKill {
                active: false,
                cancel_all: false,
            },
            0x1072c892c614f5086cb9c884299d3168,
        ),
    ];
    for (kind, expected) in cases {
        assert_eq!(kind.derive(CommandId(7), ShardId(2)), CommandId(expected));
    }
}

#[test]
fn invalid_ack_or_arithmetic_overflow_never_advances_the_input_record() {
    let ids = [ShardId(1), ShardId(2)];
    let mut r =
        ControlOperation::prepared(CommandId(6), ControlKind::GlobalMassCancel, &ids).unwrap();
    r.phase = ControlPhase::AwaitingAcks;
    r.epoch = 1;
    let ack = ControlAck {
        shard_seq: 42,
        canceled: u64::MAX,
    };
    assert!(
        r.transition(ControlUpdate::Ack(
            ids[0],
            ControlAck {
                shard_seq: 0,
                canceled: 0
            }
        ))
        .is_err()
    );
    r = r.transition(ControlUpdate::Ack(ids[0], ack)).unwrap();
    assert_eq!(r.transition(ControlUpdate::Ack(ids[0], ack)).unwrap(), r);
    assert!(
        r.transition(ControlUpdate::Ack(
            ids[0],
            ControlAck {
                shard_seq: 43,
                canceled: 0
            }
        ))
        .is_err()
    );
    r = r
        .transition(ControlUpdate::Ack(
            ids[1],
            ControlAck {
                shard_seq: 1,
                canceled: 1,
            },
        ))
        .unwrap();
    assert!(r.transition(ControlUpdate::Next).is_err());
    r.phase = ControlPhase::Prepared;
    r.generation = u64::MAX;
    assert!(r.transition(ControlUpdate::Next).is_err());
}

/// 三位确认集合是独立参考模型。穷举五次投递的顺序，包含重复和从未应答的分片。
#[test]
fn partial_ack_retries_agree_with_independent_three_shard_model() {
    let ids = [ShardId(2), ShardId(8), ShardId(19)];
    let counts = [0, 3, 11];
    let base =
        ControlOperation::prepared(CommandId(9), ControlKind::GlobalMassCancel, &ids).unwrap();
    let mut ready = base.transition(ControlUpdate::Next).unwrap();
    ready.epoch = 1;
    ready = ready
        .transition(ControlUpdate::Next)
        .unwrap()
        .transition(ControlUpdate::Next)
        .unwrap();
    for tape in 0..3_usize.pow(5) {
        let mut choices = tape;
        let mut mask = 0_u8;
        let mut record = ready.clone();
        for _ in 0..5 {
            let i = choices % ids.len();
            choices /= ids.len();
            mask |= 1 << i;
            let ack = ControlAck {
                shard_seq: u64::from(ids[i].0),
                canceled: counts[i],
            };
            record = record.transition(ControlUpdate::Ack(ids[i], ack)).unwrap();
            let expected: u64 = counts
                .iter()
                .enumerate()
                .filter(|(i, _)| mask & (1 << i) != 0)
                .map(|(_, n)| n)
                .sum();
            assert_eq!(record.total_canceled().unwrap(), expected);
            assert_eq!(
                record.generation,
                ready.generation + u64::from(mask.count_ones())
            );
            assert_eq!(
                record.transition(ControlUpdate::Next).is_ok(),
                mask == 0b111
            );
            assert!(record.transition(ControlUpdate::Abort).is_err());
        }
    }
}
