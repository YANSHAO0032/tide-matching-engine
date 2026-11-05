//! 校验可恢复命令记录的字段、截断和输入边界。
use matching_domain::*;

fn envelope(payload: Command) -> CommandEnvelope {
    CommandEnvelope {
        command_id: CommandId(1),
        session_id: 2,
        client_seq: 3,
        market_id: Some(MarketId(4)),
        user_id: Some(UserId(5)),
        route_epoch: Some(6),
        client_timestamp: Some(7),
        received_at_unix_ns: Some(8),
        payload,
    }
}
fn samples() -> Vec<CommandEnvelope> {
    let order = LimitOrder {
        order_id: OrderId(9),
        user_id: UserId(5),
        stp_key: StpKey::new(3),
        stp_mode: StpMode::CancelBoth,
        side: Side::Sell,
        price: Price::try_new(12).unwrap(),
        qty: Qty::try_new(13).unwrap(),
    };
    let reservation = ReservationToken {
        token_id: 14,
        command_id: CommandId(1),
        reservation_id: ReservationId(15),
        order_id: OrderId(9),
        user_id: UserId(5),
        market_id: MarketId(4),
        reserves: [AssetReserve {
            asset: AssetId(7),
            atoms: 42,
        }]
        .into_iter()
        .collect(),
        fee_schedule_version: 16,
        fee_tier_id: 17,
        risk_key_id: 18,
        signature: [19; 64],
    };
    let mut out = vec![
        envelope(Command::PlaceLimit(Box::new(PlaceLimitOrder {
            order,
            client_order_id: Some("客户-α".into()),
            user_control_epoch: 20,
            post_only: true,
            reservation,
        }))),
        envelope(Command::ApplyUserControlFenceShard(
            ApplyUserControlFenceShard {
                parent_command_id: CommandId(21),
                user_id: UserId(5),
                new_epoch: 22,
                action: UserFenceAction::FenceAndCancelAll,
                expected_primary_epoch: 23,
            },
        )),
    ];
    for scope in [
        ShardCancelScope::All,
        ShardCancelScope::User { user_id: UserId(5) },
        ShardCancelScope::Market {
            market_id: MarketId(4),
        },
        ShardCancelScope::UserMarket {
            user_id: UserId(5),
            market_id: MarketId(4),
        },
        ShardCancelScope::Session {
            user_id: UserId(5),
            session_id: OriginSessionId(24),
        },
    ] {
        out.push(envelope(Command::MassCancelShard(MassCancelShard {
            scope,
        })));
    }
    out
}
#[test]
fn complete_command_round_trip_and_truncation() {
    for value in samples() {
        let bytes = encode_wal_command(&value).unwrap();
        assert_eq!(decode_wal_command(&bytes).unwrap(), value);
        for end in 0..bytes.len() {
            assert!(decode_wal_command(&bytes[..end]).is_err());
        }
        let mut extra = bytes;
        extra.push(0);
        assert!(decode_wal_command(&extra).is_err());
    }
}
#[test]
fn malformed_version_and_tags_fail_closed() {
    let mut bytes = encode_wal_command(&samples()[0]).unwrap();
    bytes[0] = 0xff;
    assert!(decode_wal_command(&bytes).is_err());
}

#[test]
fn rejects_oversize_variable_fields() {
    let mut value = samples().remove(0);
    let Command::PlaceLimit(ref mut place) = value.payload else {
        unreachable!()
    };
    place.client_order_id = Some("x".repeat(16 * 1024 * 1024));
    assert!(encode_wal_command(&value).is_err());
}

#[test]
fn all_market_cancel_golden_encoding() {
    let command = CommandEnvelope {
        command_id: CommandId(0),
        session_id: 0,
        client_seq: 0,
        market_id: None,
        user_id: None,
        route_epoch: None,
        client_timestamp: None,
        received_at_unix_ns: None,
        payload: Command::MassCancelShard(MassCancelShard {
            scope: ShardCancelScope::All,
        }),
    };
    let expected: [u8; 40] = [
        1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
        0, 0, 0, 0, 0, 0, 0, 0, 1, 4,
    ];
    assert_eq!(encode_wal_command(&command).unwrap(), expected);
    assert_eq!(decode_wal_command(&expected).unwrap(), command);
}

#[test]
fn invalid_optional_and_command_tags_are_rejected() {
    let mut command = envelope(Command::MassCancelShard(MassCancelShard {
        scope: ShardCancelScope::All,
    }));
    command.market_id = None;
    command.user_id = None;
    command.route_epoch = None;
    command.client_timestamp = None;
    command.received_at_unix_ns = None;
    let bytes = encode_wal_command(&command).unwrap();
    for (offset, invalid) in [(33, 2), (38, 9), (39, 9)] {
        let mut malformed = bytes.clone();
        malformed[offset] = invalid;
        assert!(decode_wal_command(&malformed).is_err());
    }
}

#[test]
fn place_corrupt_values_and_lengths_are_rejected() {
    let bytes = encode_wal_command(&samples()[0]).unwrap();
    // 前缀、完整订单、客户端身份之后依次为控制版本和只挂单标志。
    for (offset, value) in [(107, 255), (108, 255), (130, 255), (147, 2)] {
        let mut damaged = bytes.clone();
        damaged[offset] = value;
        assert!(decode_wal_command(&damaged).is_err(), "offset {offset}");
    }
    for offset in [109, 117] {
        let mut damaged = bytes.clone();
        damaged[offset..offset + 8].fill(0);
        assert!(decode_wal_command(&damaged).is_err(), "offset {offset}");
    }
    let mut damaged = bytes.clone();
    damaged[224..228].copy_from_slice(&u32::MAX.to_be_bytes());
    assert!(decode_wal_command(&damaged).is_err());
}

#[test]
fn decoder_is_total_and_reencodes_accepted_bytes_identically() {
    let seed = encode_wal_command(&samples()[0]).unwrap();
    let mut state = 0x673a_92de_u64;
    for i in 0..3000 {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        let mut candidate = seed.clone();
        let index = (state as usize) % candidate.len();
        candidate[index] ^= (i as u8).wrapping_add(1);
        if let Ok(value) = decode_wal_command(&candidate) {
            assert_eq!(encode_wal_command(&value).unwrap(), candidate);
        }
    }
}

#[test]
fn all_stp_modes_sides_and_optional_client_id_round_trip() {
    for mode in [
        StpMode::None,
        StpMode::CancelTaker,
        StpMode::CancelMaker,
        StpMode::CancelBoth,
        StpMode::DecrementAndCancel,
    ] {
        for side in [Side::Buy, Side::Sell] {
            let mut value = samples().remove(0);
            let Command::PlaceLimit(ref mut place) = value.payload else {
                unreachable!()
            };
            place.order.stp_mode = mode;
            place.order.side = side;
            place.client_order_id = None;
            place.reservation.reserves.clear();
            assert_eq!(
                decode_wal_command(&encode_wal_command(&value).unwrap()).unwrap(),
                value
            );
        }
    }
}

/// 将固定十六进制证据转换为字节，不依赖被测编解码器。
fn golden_bytes(hex: &str) -> Vec<u8> {
    hex.as_bytes()
        .as_chunks::<2>()
        .0
        .iter()
        .map(|pair| u8::from_str_radix(std::str::from_utf8(pair).unwrap(), 16).unwrap())
        .collect()
}

#[test]
fn place_and_fence_have_stable_complete_golden_encoding() {
    let fence = golden_bytes(concat!(
        "010000000000000000000000000000000100000000000000020000000000000003",
        "010000000401000000000000000501000000000000000601000000000000000701",
        "000000000000000800000000000000000000000000000000150000000000000005",
        "0000000000000016010000000000000017"
    ));
    let place = golden_bytes(concat!(
        "010000000000000000000000000000000100000000000000020000000000000003",
        "010000000401000000000000000501000000000000000601000000000000000701",
        "000000000000000802000000000000000000000000000000090000000000000005",
        "00000000000000030301000000000000000c000000000000000d0100000009e5ae",
        "a2e688b72dceb10000000000000014010000000000000000000000000000000e00",
        "0000000000000000000000000000010000000000000000000000000000000f0000",
        "000000000000000000000000000900000000000000050000000400000001000000",
        "070000000000000000000000000000002a00000000000000100000001100000012",
        "1313131313131313131313131313131313131313131313131313131313131313",
        "1313131313131313131313131313131313131313131313131313131313131313"
    ));
    let samples = samples();
    for (command, expected) in [(&samples[0], &place), (&samples[1], &fence)] {
        assert_eq!(&encode_wal_command(command).unwrap(), expected);
        assert_eq!(&decode_wal_command(expected).unwrap(), command);
    }
}
