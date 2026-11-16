# Swarm 共识

设备端固定成员 Raft 实现，是新系列唯一的 Leader 和多数派权限来源。节点入口为 `swarm_consensus_node`；不调用旧选举包，不操作 TaskHub。

每轮通过 `/swarm/consensus/configure` 获取平台名单和 `round_epoch`。多数派始终为初始人数的过半数；隔离成员不会降低分母。支持预投票、持久任期和投票、日志复制、读屏障、冲突修复及快照传递。

进程重启后不能自行恢复旧轮执行或投票身份；须确认停止并使用新轮次。正常 Leader 故障且仍有多数派时，其余副本接续已提交任务。没有多数派不能提交终态，也不能续发执行许可。

主要参数：`robot_id`、`data_root`、`election_timeout`、`heartbeat_period`、`quorum_timeout`。持久化目录不得在活动任务中删除。调度方经 `/swarm/consensus/propose` 提案，只按 `/swarm/consensus/state` 中的已提交记录执行业务。

这是独立开发实现，未经形式化验证。当前快照仍保存全量已提交历史，长时增长及 8 MiB 消息上限需要进一步工程化。完整用法见 [协调器 README](../swarm_mission_coordinator/README.md)，故障验证见 [TEST_LOG](TEST_LOG.md)。
