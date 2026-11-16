import pytest
from swarm_consensus.raft import MemoryStore, Raft
from swarm_consensus.simulation import Cluster
from swarm_protocol.storage import DurableFile


@pytest.mark.parametrize("count", [1, 2, 3, 4, 5])
def test_elect_commit_and_compact(count):
    c = Cluster(count)
    c.run()
    leader = c.leader()
    index = leader.propose({"type": "TEST", "id": "one", "value": 4}, c.now)
    c.drain()
    c.run(0.3)
    assert all(n.disk["commit"] == index for n in c.nodes.values())
    assert len({str(n.committed()) for n in c.nodes.values()}) == 1
    for n in c.nodes.values():
        n.compact()
        assert n.base == index
        assert not n.disk["log"]


def test_leader_loss_inherits_commit_and_rejects_old_leader():
    c = Cluster(5)
    c.run()
    old = c.leader()
    old.propose({"type": "ARRIVAL", "id": "arrival-1", "task": "p1"}, c.now)
    c.drain()
    c.run(0.2)
    c.isolate(old.robot)
    c.run(3)
    new = c.leader()
    assert new.robot != old.robot
    assert any(e["command"]["id"] == "arrival-1" for e in new.committed())
    assert not old.authority(c.now)
    with pytest.raises(ValueError):
        old.propose({"id": "illegal"}, c.now)


@pytest.mark.parametrize("count,remaining", [(2, 1), (3, 1), (4, 2), (5, 2)])
def test_fixed_quorum_never_reduces(count, remaining):
    c = Cluster(count)
    c.run()
    c.crashed.update(c.ids[remaining:])
    c.run(3)
    assert not any(n.authority(c.now) for r, n in c.nodes.items() if r not in c.crashed)


def test_lagging_log_cannot_win_and_snapshot_catches_up():
    c = Cluster(3)
    c.run()
    leader = c.leader()
    peer = next(r for r in c.ids if r != leader.robot)
    c.isolate(peer)
    for i in range(12):
        leader.propose({"type": "TEST", "id": f"t{i}"}, c.now)
        c.drain()
    c.run(0.2)
    leader.compact()
    c.blocked.clear()
    c.run(0.5)
    assert c.nodes[peer].committed() == leader.committed()
    assert c.nodes[peer].term == leader.term


def test_uncommitted_conflicting_tail_is_replaced():
    c = Cluster(3)
    c.run()
    old = c.leader()
    c.isolate(old.robot)
    old.propose({"type": "TEST", "id": "uncommitted"}, c.now)
    c.run(3)
    new = c.leader()
    new.propose({"type": "TEST", "id": "legitimate"}, c.now)
    c.drain()
    c.blocked.clear()
    c.run(1)
    assert c.nodes[old.robot].committed() == new.committed()
    assert not any(e["command"]["id"] == "uncommitted" for e in new.committed())


def test_old_ack_cannot_refresh_authority_or_complete_read():
    c = Cluster(3)
    c.run()
    leader = c.leader()
    c.isolate(leader.robot)
    leader.read_barrier("lease", c.now)
    withheld = list(c.messages)
    c.messages.clear()
    c.run(1.0)
    assert not leader.authority(c.now)
    for a, b, message in withheld:
        if message["kind"] == "append":
            leader.receive(
                b,
                {
                    "sender": b,
                    "boot": b + "-boot",
                    "epoch": 1,
                    "term": leader.term,
                    "kind": "append_reply",
                    "rpc": message["rpc"],
                    "success": True,
                    "match": leader.last,
                },
                c.now,
            )
    assert not leader.authority(c.now)
    assert not leader.read_ready


def test_majority_read_and_duplicate_proposals():
    c = Cluster(3)
    c.run()
    leader = c.leader()
    command = {"type": "TEST", "id": "same"}
    i = leader.propose(command, c.now)
    c.drain()
    assert leader.propose(command, c.now) == i
    with pytest.raises(ValueError):
        leader.propose({**command, "value": "conflict"}, c.now)
    assert leader.read_barrier("renew-1", c.now)
    c.drain()
    assert leader.read_ready[0]["request_id"] == "renew-1"


def test_bound_rebooted_or_excluded_peer_cannot_vote():
    c = Cluster(3)
    c.run()
    bindings = {r: r + "-boot" for r in c.ids}
    for n in c.nodes.values():
        n.bind_identities(bindings)
    leader = c.leader()
    peer = next(r for r in c.ids if r != leader.robot)
    term = leader.term
    leader.receive(
        peer,
        {
            "epoch": 1,
            "sender": peer,
            "boot": "new-boot",
            "term": term + 99,
            "kind": "vote",
            "last_index": 999,
            "last_term": 999,
        },
        c.now,
    )
    assert leader.term == term
    leader.exclude(peer)
    assert leader.quorum == 2


def test_restart_is_nonparticipating_until_new_round(tmp_path):
    store = DurableFile(tmp_path / "raft.json")
    node = Raft("A", "boot1", store, lambda *x: None)
    node.configure(3, ["A"], 0)
    node.tick(20)
    assert node.authority(20)
    restored = Raft("A", "boot2", store, lambda *x: None)
    restored.tick(100)
    assert not restored.authority(100)
    assert restored.committed() == node.committed()
    restored.configure(4, ["A"], 100)
    restored.tick(120)
    assert restored.authority(120)
    (tmp_path / "raft.json").write_text('{"sha256":"bad","data":{}}')
    with pytest.raises(RuntimeError):
        Raft("A", "boot3", store, lambda *x: None)


def test_disk_failure_never_emits_positive_ack():
    class Broken(MemoryStore):
        def save(self, value):
            raise OSError("disk full")

    node = Raft("A", "boot", Broken(), lambda *x: pytest.fail("must not send"))
    with pytest.raises(OSError):
        node.configure(1, ["A"], 0)
    assert not node.authority(100)
