"""Deterministic randomized delivery; committed prefixes must never diverge."""

import copy
import json
import random
import time

import pytest
from swarm_consensus.simulation import Cluster
from swarm_protocol.contracts import MAX_MESSAGE_BYTES, canonical, envelope


@pytest.mark.parametrize("seed", range(5))
def test_reordering_duplicates_drops_and_partitions_preserve_committed_prefix(seed):
    rng = random.Random(seed)
    cluster = Cluster(5)
    history = {}
    for step in range(240):
        cluster.now += 0.05
        if step == 100:
            cluster.isolate(cluster.leader().robot)
        if step == 170:
            cluster.blocked.clear()
        for robot, node in cluster.nodes.items():
            node.tick(cluster.now)
        if step % 8 == 0:
            leaders = [n for n in cluster.nodes.values() if n.authority(cluster.now)]
            if len(leaders) == 1:
                leaders[0].propose(
                    {"type": "TEST", "id": f"{seed}-{step}"}, cluster.now
                )
        # Delay leftover messages and deliver a random bounded subset, including duplicates.
        batch = list(cluster.messages)
        cluster.messages.clear()
        rng.shuffle(batch)
        for a, b, message in batch:
            if rng.random() < 0.05 or (a, b) in cluster.blocked:
                continue
            if rng.random() < 0.10:
                cluster.messages.append((a, b, message))
                continue
            cluster.nodes[b].receive(a, message, cluster.now)
            if rng.random() < 0.1:
                cluster.nodes[b].receive(a, copy.deepcopy(message), cluster.now)
        for node in cluster.nodes.values():
            for entry in node.committed():
                index = entry["index"]
                if index in history:
                    assert history[index] == entry
                else:
                    history[index] = entry
    cluster.blocked.clear()
    cluster.run(3)
    leader = cluster.leader()
    assert history
    assert all(n.committed() == leader.committed() for n in cluster.nodes.values())


@pytest.mark.parametrize("size", [3, 4, 5, 6])
def test_one_way_partition_keeps_commits_and_removes_old_authority(size):
    cluster = Cluster(size)
    cluster.run()
    leader = cluster.leader()
    index = leader.propose({"type": "TEST", "id": "before"}, cluster.now)
    cluster.drain()
    cluster.run(0.5)
    # The Leader can still transmit but can no longer hear confirmations.
    cluster.blocked.update((peer, leader.robot) for peer in cluster.ids if peer != leader.robot)
    cluster.run(4)
    assert not leader.authority(cluster.now)
    successor = cluster.leader()
    assert successor.robot != leader.robot
    assert successor.entry(index)["command"]["id"] == "before"
    successor.propose({"type": "TEST", "id": "after"}, cluster.now)
    cluster.drain()
    cluster.blocked.clear()
    cluster.run(2)
    assert all(node.committed() == cluster.leader().committed() for node in cluster.nodes.values())


def test_six_replicas_catch_up_large_committed_history():
    cluster = Cluster(6)
    cluster.run()
    leader = cluster.leader()
    lagging = next(robot for robot in cluster.ids if robot != leader.robot)
    cluster.isolate(lagging)
    for index in range(20):
        leader.propose({
            "type": "BENCH", "id": str(index),
            "tasks": [{"id": f"p{i}", "route_evidence": "x" * 4500} for i in range(100)],
        }, cluster.now)
        cluster.drain()
    leader.compact()
    assert len(canonical(leader.committed()).encode()) > MAX_MESSAGE_BYTES
    started = time.process_time()
    for _ in range(20):
        encoded = canonical(leader.state(cluster.now)).encode()
    cpu = time.process_time() - started

    cluster.blocked.clear()
    cluster.messages.clear()
    start = time.perf_counter()
    leader.broadcast(cluster.now)
    max_packet = frames = 0
    while cluster.messages:
        sender, recipient, message = cluster.messages.popleft()
        if recipient == lagging:
            wire = envelope("swarm", sender, sender + "-boot", recipient, "raft", message, 1)
            size = len(canonical(wire).encode())
            max_packet = max(max_packet, size)
            frames += 1
            assert size < MAX_MESSAGE_BYTES and frames < 100
        cluster.nodes[recipient].receive(sender, message, cluster.now)
    assert all(node.committed() == leader.committed() for node in cluster.nodes.values())
    assert "entries" not in leader.state(cluster.now) and len(encoded) < 1024
    print(json.dumps({
        "replicas": 6, "points_per_revision": 100, "revisions": 20,
        "aggregate_state_bytes": len(encoded), "state_encode_cpu_seconds_20": cpu,
        "lag_max_message_bytes": max_packet, "lag_messages": frames,
        "catchup_wall_seconds": time.perf_counter() - start,
    }))
