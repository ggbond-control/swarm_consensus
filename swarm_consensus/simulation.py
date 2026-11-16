"""Deterministic fault-injection harness exercising the real consensus core."""

import copy
import random
from collections import deque

from .raft import MemoryStore, Raft


class Cluster:
    def __init__(self, count, epoch=1):
        self.ids = [f"R{i}" for i in range(count)]
        self.now = 0.0
        self.messages = deque()
        self.blocked, self.crashed = set(), set()
        self.stores = {r: MemoryStore() for r in self.ids}
        self.nodes = {r: self.make(r) for r in self.ids}
        for node in self.nodes.values():
            node.configure(epoch, self.ids, self.now)

    def make(self, robot, boot=None):
        return Raft(
            robot,
            boot or robot + "-boot",
            self.stores[robot],
            lambda target, data: self.messages.append(
                (robot, target, copy.deepcopy(data))
            ),
            election_timeout=0.8,
            heartbeat=0.05,
            quorum_timeout=0.4,
            rng=random.Random(robot),
        )

    def drain(self):
        iterations = 0
        while self.messages:
            a, b, payload = self.messages.popleft()
            if (
                a not in self.crashed
                and b not in self.crashed
                and (a, b) not in self.blocked
            ):
                self.nodes[b].receive(a, payload, self.now)
            iterations += 1
            if iterations > 10000:
                raise RuntimeError("network failed to quiesce")

    def step(self, seconds=0.05):
        self.now += seconds
        for r, node in self.nodes.items():
            if r not in self.crashed:
                node.tick(self.now)
        self.drain()

    def run(self, seconds=3.0):
        for _ in range(int(seconds / 0.025)):
            self.step(0.025)

    def leader(self):
        leaders = [
            n
            for r, n in self.nodes.items()
            if r not in self.crashed and n.authority(self.now)
        ]
        if len(leaders) != 1:
            raise ValueError(f"expected one leader, got {len(leaders)}")
        return leaders[0]

    def isolate(self, robot):
        self.blocked.update(
            (a, b) for a in self.ids for b in self.ids if a == robot or b == robot
        )
