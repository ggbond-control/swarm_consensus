# Swarm 共识

设备端固定成员 Raft 实现，是新系列唯一的 Leader 和多数派权限来源。节点入口为 `swarm_consensus_node`；不调用旧选举包，不操作 TaskHub。

每轮通过 `/swarm/consensus/configure` 获取平台名单和 `round_epoch`。多数派始终为初始人数的过半数；隔离成员不会降低分母。支持预投票、持久任期和投票、日志复制、读屏障、冲突修复及快照传递。

进程重启后不能自行恢复旧轮执行或投票身份；须确认停止并使用新轮次。正常 Leader 故障且仍有多数派时，其余副本接续已提交任务。没有多数派不能提交终态，也不能续发执行许可。

主要参数为 `robot_id`、`data_root`、`election_timeout`、`heartbeat_period`、`quorum_timeout`。调度方经 `/swarm/consensus/propose` 提案，从 `/swarm/consensus/state` 获取任期、权限和提交水位，再经 `/swarm/consensus/fetch` 与 `/swarm/consensus/entries` 增量应用本机已提交记录。

落后设备按批次追赶。快照保留本轮已提交历史，磁盘增长待进行长时间实机验证。 [协调器 README](../swarm_mission_coordinator/README.md) · [TEST_LOG](TEST_LOG.md)
