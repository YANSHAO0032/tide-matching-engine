//! 验证全局控制和单笔撤单的持久编码及稳定原因编号。
use matching_domain::*;

/// 检查全局开关载荷可往返，并拒绝截断和未知动作。
#[test]
fn global_kill_wire_rejects_unknown_action_and_truncation() {
    let command = CommandEnvelope {
        command_id: CommandId(51),
        session_id: 2,
        client_seq: 1,
        market_id: None,
        user_id: None,
        route_epoch: None,
        client_timestamp: None,
        received_at_unix_ns: None,
        payload: Command::ApplyGlobalControlFenceShard(ApplyGlobalControlFenceShard {
            parent_command_id: CommandId(70),
            new_epoch: 3,
            action: UserFenceAction::Kill {
                active: true,
                cancel_all: true,
            },
            expected_primary_epoch: 9,
        }),
    };
    let mut bytes = encode_wal_command(&command).unwrap();
    assert_eq!(decode_wal_command(&bytes).unwrap(), command);
    for end in 0..bytes.len() {
        assert!(decode_wal_command(&bytes[..end]).is_err());
    }
    let action = bytes.len() - 9;
    bytes[action] = 255;
    assert!(decode_wal_command(&bytes).is_err());
}

/// 对照协议字段顺序、指令标记和稳定撤单/资金原因，识别截断或多余字节。
#[test]
fn cancel_order_wire_and_terminal_reasons_are_stable() {
    let command = CommandEnvelope {
        command_id: CommandId(5),
        session_id: 9,
        client_seq: 2,
        market_id: Some(MarketId(7)),
        user_id: Some(UserId(3)),
        route_epoch: Some(4),
        client_timestamp: None,
        received_at_unix_ns: None,
        payload: Command::Cancel(CancelOrder {
            market_id: MarketId(7),
            user_id: UserId(3),
            order_id: OrderId(11),
            client_order_id: Some("tag".to_owned()),
        }),
    };
    let mut expected = vec![1];
    expected.extend(5_u128.to_be_bytes());
    expected.extend(9_u64.to_be_bytes());
    expected.extend(2_u64.to_be_bytes());
    expected.push(1);
    expected.extend(7_u32.to_be_bytes());
    expected.push(1);
    expected.extend(3_u64.to_be_bytes());
    expected.push(1);
    expected.extend(4_u64.to_be_bytes());
    expected.extend([0, 0, 17]);
    expected.extend(7_u32.to_be_bytes());
    expected.extend(3_u64.to_be_bytes());
    expected.extend(11_u128.to_be_bytes());
    expected.push(1);
    expected.extend(3_u32.to_be_bytes());
    expected.extend(b"tag");
    assert_eq!(encode_wal_command(&command).unwrap(), expected);
    assert_eq!(decode_wal_command(&expected).unwrap(), command);
    for end in 0..expected.len() {
        assert!(decode_wal_command(&expected[..end]).is_err());
    }
    expected.push(0);
    assert!(decode_wal_command(&expected).is_err());
    assert_eq!(CancelReason::UserRequested as u16, 1101);
    assert_eq!(ReservationReleaseReason::UserCancel as u16, 1302);
}
