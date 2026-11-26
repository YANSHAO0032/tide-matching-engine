//! 在冻结前缀与所有权切换两侧重新打开实际路由日志、清单、快照和业务日志。
use super::*;

#[test]
fn combined_archive_over_limit_rejects_before_freezing_either_shard() {
    let first = TestDirectory::new();
    let second = TestDirectory::new();
    let control = TestDirectory::new();
    let archive = TestDirectory::new();
    let routes = RouteTable::new([(MOVE, A), (STAY, A), (OTHER, B)].map(
        |(market_id, shard_id)| MarketRoute {
            market_id,
            shard_id,
            route_epoch: 1,
            state: RouteState::Active,
        },
    ))
    .unwrap();
    let cluster = ClusterId(1038);
    let mut a = start(cluster, A, &first.0, &[MOVE, STAY], &routes);
    let mut b = start(cluster, B, &second.0, &[OTHER], &routes);
    let store =
        RouteControlStore::create_for_cluster(&control.0, cluster, routes.clone(), 8, 8).unwrap();
    let mut coordinator = RouteCoordinator::new(store, vec![a.handle(), b.handle()]).unwrap();
    // 迁移对调双方的空市场数，两张镜像均可单独编码，行情和归档的封装超过剩余空间。
    let max_bytes = snap(&a)
        .encode_snapshot(LIMIT)
        .unwrap()
        .len()
        .checked_add(snap(&b).encode_snapshot(LIMIT).unwrap().len())
        .unwrap();
    let storage = MigrationStorage {
        archive: archive.0.clone(),
        source_snapshot: (first.0.join("snapshots"), 71),
        target_snapshot: (second.0.join("snapshots"), 72),
        max_bytes,
        timeout: WAIT,
    };
    let id = MigrationId {
        cluster,
        market: MOVE,
        source: A,
        target: B,
        old_epoch: 1,
    };
    let error = storage
        .migrate(&mut coordinator, id, Some(&FeedBuilder::new(MOVE)))
        .err()
        .unwrap();
    assert_eq!(error.to_string(), "迁移交接超过容量");
    assert_eq!(coordinator.verify_authority().unwrap(), 1);
    for (shard, market) in [(&a, MOVE), (&b, OTHER)] {
        let command = CommandEnvelope {
            command_id: CommandId(75),
            session_id: 75,
            client_seq: 1,
            market_id: Some(market),
            user_id: None,
            route_epoch: Some(1),
            client_timestamp: None,
            received_at_unix_ns: None,
            payload: Command::MassCancelShard(MassCancelShard {
                scope: ShardCancelScope::Market { market_id: market },
            }),
        };
        assert_eq!(run(shard, &command).unwrap().seq, 1);
    }
    a.shutdown(WAIT).unwrap();
    b.shutdown(WAIT).unwrap();
}

fn restart(
    root: &Path,
    cluster: ClusterId,
    shard: ShardId,
    markets: &[MarketId],
    routes: &RouteTable,
) -> Shard {
    let mut trusted = restore(shard, markets, routes);
    trusted.cluster = cluster;
    Shard::recover_group_local_with_genesis(
        root.join("wal"),
        root.join("manifest"),
        root.join("snapshots"),
        &trusted,
        &policies(markets),
        (1, 1000),
    )
    .unwrap()
}

#[test]
fn restarts_before_bundle_and_before_cutover_use_the_same_frozen_prefix() {
    for with_snapshot in [false, true] {
        let first = TestDirectory::new();
        let second = TestDirectory::new();
        let authority = TestDirectory::new();
        let archive = TestDirectory::new();
        let cluster = ClusterId(if with_snapshot { 1037 } else { 1036 });
        let routes = RouteTable::new([(MOVE, A), (STAY, A), (OTHER, B)].map(
            |(market_id, shard_id)| MarketRoute {
                market_id,
                shard_id,
                route_epoch: 1,
                state: RouteState::Active,
            },
        ))
        .unwrap();
        let start = |root: &Path, shard, markets: &[MarketId]| {
            fs::create_dir(root.join("snapshots")).unwrap();
            Shard::start_with_durable_place_support(
                shard,
                cluster,
                routes.clone(),
                policies(markets),
                (ReservationRegistry::new([]).unwrap(), LogicalTime::new(0)),
                (IdempotencyConfig::default(), 1, 16, config(markets)),
                (root.join("wal"), root.join("manifest")),
            )
            .unwrap()
        };
        let mut a = start(&first.0, A, &[MOVE, STAY]);
        let mut b = start(&second.0, B, &[OTHER]);
        // 权威文件只允许初始和冻结两次登记，使第一次切换在双方安装后确定地失败。
        let store =
            RouteControlStore::create_for_cluster(&authority.0, cluster, routes, 8, 2).unwrap();
        let mut coordinator = RouteCoordinator::new(store, vec![a.handle(), b.handle()]).unwrap();
        let mut feed = FeedBuilder::new(MOVE);
        let mut hash = EventHasher::new();
        record(
            &run(&a, &place(30, Side::Buy, 1, 1)).unwrap(),
            &mut hash,
            &mut feed,
        );
        if with_snapshot {
            let mut writer = matching_runtime::SnapshotWriter::start(
                a.handle(),
                first.0.join("snapshots"),
                LIMIT,
            )
            .unwrap();
            writer
                .submit(50, snap(&a))
                .unwrap()
                .recv_timeout(WAIT)
                .unwrap()
                .unwrap();
            writer.shutdown().unwrap();
        }
        record(
            &run(&a, &place(31, Side::Buy, 1, 1)).unwrap(),
            &mut hash,
            &mut feed,
        );
        let digest = snap(&a).market_state_digest(MOVE, LIMIT).unwrap();
        let id = MigrationId {
            cluster,
            market: MOVE,
            source: A,
            target: B,
            old_epoch: 1,
        };
        let storage = MigrationStorage {
            archive: archive.0.clone(),
            source_snapshot: (first.0.join("snapshots"), 51),
            target_snapshot: (second.0.join("snapshots"), 52),
            max_bytes: LIMIT,
            timeout: WAIT,
        };
        // 阻断归档临时文件的创建，模拟冻结已提交但交接尚未落盘时中断。
        let operation = archive.0.join(format!(
            "market-{}-{}-{}-{}-{}",
            id.cluster.0, id.market.0, id.source.0, id.target.0, id.old_epoch
        ));
        let archive_write = operation.join("snapshot-1.migration.tmp");
        fs::create_dir_all(&archive_write).unwrap();
        assert!(storage.migrate(&mut coordinator, id, Some(&feed)).is_err());
        fs::remove_dir(archive_write).unwrap();
        a.shutdown(WAIT).unwrap();
        b.shutdown(WAIT).unwrap();
        drop(coordinator);

        let store = RouteControlStore::open_for_cluster(&authority.0, cluster, 8, 2).unwrap();
        let (rev, table) = store.read().unwrap();
        assert_eq!(rev, 2);
        let frozen = table.clone();
        assert!(matches!(
            frozen.get(MOVE).unwrap().state,
            RouteState::Migrating {
                source: A,
                target: B
            }
        ));
        let mut a = restart(&first.0, cluster, A, &[MOVE, STAY], &frozen);
        let mut b = restart(&second.0, cluster, B, &[OTHER], &frozen);
        let mut coordinator = RouteCoordinator::new(store, vec![a.handle(), b.handle()]).unwrap();
        assert_eq!(snap(&a).market_state_digest(MOVE, LIMIT).unwrap(), digest);
        assert_eq!(snap(&a).snapshot_seq(), 2);
        assert!(storage.migrate(&mut coordinator, id, Some(&feed)).is_err());
        assert!(storage.source_snapshot.0.join("snapshot-51.bin").is_file());
        assert!(storage.target_snapshot.0.join("snapshot-52.bin").is_file());
        a.shutdown(WAIT).unwrap();
        b.shutdown(WAIT).unwrap();
        drop(coordinator);

        let store = RouteControlStore::open_for_cluster(&authority.0, cluster, 8, 3).unwrap();
        assert_eq!(store.read().unwrap(), (2, &frozen));
        let mut a = restart(&first.0, cluster, A, &[STAY], &frozen);
        let mut b = restart(&second.0, cluster, B, &[MOVE, OTHER], &frozen);
        let mut coordinator = RouteCoordinator::new(store, vec![a.handle(), b.handle()]).unwrap();
        assert_eq!(snap(&b).market_state_digest(MOVE, LIMIT).unwrap(), digest);
        assert!(snap(&a).order(MOVE, OrderId(30)).is_err());
        // 两次所有权 CAS 均可成功，单独使切换确认后的管理文件写入失败。
        let marker = operation.join("snapshot-2.migration.tmp");
        fs::create_dir(&marker).unwrap();
        assert!(storage.migrate(&mut coordinator, id, None).is_err());
        assert_eq!(coordinator.verify_authority().unwrap(), 3);
        assert!(storage.read_boundary(id).is_err());
        fs::remove_dir(marker).unwrap();
        a.shutdown(WAIT).unwrap();
        b.shutdown(WAIT).unwrap();
        drop(coordinator);

        let store = RouteControlStore::open_for_cluster(&authority.0, cluster, 8, 3).unwrap();
        let (rev, table) = store.read().unwrap();
        assert_eq!(rev, 3);
        assert_eq!(table.get(MOVE).unwrap().shard_id, B);
        let mut a = restart(&first.0, cluster, A, &[STAY], table);
        let mut b = restart(&second.0, cluster, B, &[MOVE, OTHER], table);
        let mut coordinator = RouteCoordinator::new(store, vec![a.handle(), b.handle()]).unwrap();
        // 已切换但调用者没有收到回复，重试不可再次安装起点或分配目标业务序号。
        let accepted = run(&b, &place(32, Side::Buy, 1, 2)).unwrap();
        let after = snap(&b).state_digest(LIMIT).unwrap();
        assert_eq!(accepted.seq, 1);
        assert_eq!(
            storage
                .migrate(&mut coordinator, id, None)
                .unwrap()
                .market_digest,
            digest
        );
        assert_eq!(
            storage.read_boundary(id).unwrap(),
            feed.migration_boundary(A, B, 2)
        );
        assert_eq!(snap(&b).state_digest(LIMIT).unwrap(), after);
        a.shutdown(WAIT).unwrap();
        b.shutdown(WAIT).unwrap();
        drop(coordinator);
    }
}
