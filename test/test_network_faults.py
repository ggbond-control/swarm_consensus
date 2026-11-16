"""Deterministic randomized delivery; committed prefixes must never diverge."""

import copy
import random

import pytest
from swarm_consensus.simulation import Cluster


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
