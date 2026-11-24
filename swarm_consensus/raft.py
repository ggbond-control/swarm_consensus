"""Independent fixed-configuration Raft, driven by injected monotonic time.

Persistence precedes every positive vote/append reply. A current-term no-op
and recent correlated majority ACKs are required before issuing new decisions.
Snapshots retain committed commands (small mission fleets); they compact the
replication tail, not historical audit. No wall-clock leases cross the network.
"""

from __future__ import annotations

import copy
import random
import uuid

from swarm_protocol.contracts import digest, positive_int
from swarm_protocol.replication import entry_batch, validate_command_size


class MemoryStore:
    def __init__(self):
        self.value = None

    def load(self, default=None):
        return copy.deepcopy(self.value if self.value is not None else default)

    def save(self, value):
        self.value = copy.deepcopy(value)


class Raft:
    def __init__(
        self,
        robot,
        boot,
        store,
        send,
        *,
        election_timeout=5.0,
        heartbeat=0.4,
        quorum_timeout=3.0,
        rng=None,
    ):
        if not 0 < heartbeat < quorum_timeout < election_timeout:
            raise ValueError("require heartbeat < quorum_timeout < election_timeout")
        self.robot, self.boot, self.store, self.send = robot, boot, store, send
        self.election_timeout, self.heartbeat, self.quorum_timeout = (
            election_timeout,
            heartbeat,
            quorum_timeout,
        )
        self.rng = rng or random.Random()
        self.disk = store.load(
            {
                "epoch": 0,
                "voters": [],
                "term": 0,
                "vote": None,
                "snapshot": [],
                "log": [],
                "commit": 0,
                "excluded": [],
                "identities": {},
            }
        )
        self.role = "follower"
        self.leader = ""
        self.leader_boot = ""
        self.last_contact = -float("inf")
        self.last_sent = -float("inf")
        self.deadline = 0.0
        self.votes, self.prevotes = set(), set()
        self.next_index, self.match = {}, {}
        self.proofs, self.rpcs, self.reads = {}, {}, {}
        self.read_ready = []
        self.pre_term = 0
        self.recovering = bool(self.disk["epoch"])
        self.error = ""

    @property
    def term(self):
        return self.disk["term"]

    @property
    def quorum(self):
        return len(self.disk["voters"]) // 2 + 1

    @property
    def base(self):
        return len(self.disk["snapshot"])

    @property
    def last(self):
        return self.base + len(self.disk["log"])

    def entry(self, index):
        if index == 0:
            return {"term": 0}
        return (
            self.disk["snapshot"][index - 1]
            if index <= self.base
            else self.disk["log"][index - self.base - 1]
        )

    def committed(self, after=0):
        return copy.deepcopy([self.entry(i) for i in range(after + 1, self.disk["commit"] + 1)])

    def committed_batch(self, after):
        return entry_batch(self.entry, after + 1, self.disk["commit"])

    def _save(self):
        try:
            self.store.save(self.disk)
        except Exception:
            self.error = "durable persistence failed"
            self.role, self.leader = "follower", ""
            raise

    def _deadline(self, now):
        self.deadline = now + self.election_timeout * self.rng.uniform(1.0, 1.8)

    def configure(self, epoch, voters, now):
        positive_int(epoch, "round_epoch")
        voters = sorted(voters)
        if not voters or len(voters) != len(set(voters)) or self.robot not in voters:
            raise ValueError("invalid voting configuration")
        if epoch < self.disk["epoch"]:
            raise ValueError("stale epoch")
        if epoch == self.disk["epoch"]:
            if voters != self.disk["voters"]:
                raise ValueError("conflicting fixed configuration")
            return
        self.disk = {
            "epoch": epoch,
            "voters": voters,
            "term": self.term,
            "vote": None,
            "snapshot": [],
            "log": [],
            "commit": 0,
            "excluded": [],
            "identities": {},
        }
        self._save()
        self.recovering = False
        self.role, self.leader, self.leader_boot = "follower", "", ""
        self.last_contact = -float("inf")
        self.proofs.clear()
        self.rpcs.clear()
        self.reads.clear()
        self._deadline(now)

    def bind_identities(self, identities):
        old = self.disk["identities"]
        if old and old != identities:
            raise ValueError("boot bindings are immutable within a round")
        if set(identities) != set(self.disk["voters"]):
            raise ValueError("boot bindings must cover voters")
        self.disk["identities"] = copy.deepcopy(identities)
        self._save()

    def exclude(self, robot):
        if robot not in self.disk["excluded"]:
            self.disk["excluded"].append(robot)
            self._save()
        self.proofs.pop(robot, None)
        if robot == self.robot:
            self.role, self.leader = "follower", ""

    def eligible(self):
        return bool(
            self.disk["epoch"]
            and not self.error
            and not self.recovering
            and self.robot not in self.disk["excluded"]
        )

    def authority(self, now):
        if not self.eligible() or self.role != "leader":
            return False
        if self.entry(self.disk["commit"])["term"] != self.term:
            return False
        peers = sum(
            now - t < self.quorum_timeout
            for p, t in self.proofs.items()
            if p not in self.disk["excluded"]
        )
        return 1 + peers >= self.quorum

    def _wire(self, destination, kind, **payload):
        self.send(
            destination,
            {
                "epoch": self.disk["epoch"],
                "term": self.term,
                "sender": self.robot,
                "boot": self.boot,
                "kind": kind,
                **payload,
            },
        )

    def _peers(self):
        return set(self.disk["voters"]) - set(self.disk["excluded"]) - {self.robot}

    def _step_down(self, term, now, leader="", boot=""):
        if term > self.term:
            self.disk["term"], self.disk["vote"] = term, None
            self._save()
        self.role, self.leader, self.leader_boot = "follower", leader, boot
        self.reads.clear()
        self.rpcs.clear()
        self.proofs.clear()
        self._deadline(now)

    def tick(self, now):
        if not self.eligible():
            return
        if self.role == "leader":
            # Establishing quorum after election is allowed for one grace period.
            if now - self.elected_at > self.quorum_timeout and not self.authority(now):
                self._step_down(self.term, now)
                return
            if now - self.last_sent >= self.heartbeat:
                self.broadcast(now)
        elif now >= self.deadline:
            self.role, self.pre_term, self.prevotes = (
                "pre_candidate",
                self.term + 1,
                {self.robot},
            )
            self._deadline(now)
            for peer in self._peers():
                self._wire(
                    peer,
                    "pre_vote",
                    proposed_term=self.pre_term,
                    last_index=self.last,
                    last_term=self.entry(self.last)["term"],
                )
            if len(self.prevotes) >= self.quorum:
                self._campaign(now)
        self.rpcs = {
            k: v
            for k, v in self.rpcs.items()
            if now - v["sent"] < self.election_timeout
        }
        self.reads = {
            k: v
            for k, v in self.reads.items()
            if now - v["started"] < self.quorum_timeout
        }

    def _campaign(self, now):
        self.disk["term"] += 1
        self.disk["vote"] = self.robot
        self._save()
        self.role, self.votes = "candidate", {self.robot}
        self._deadline(now)
        for peer in self._peers():
            self._wire(
                peer,
                "vote",
                last_index=self.last,
                last_term=self.entry(self.last)["term"],
            )
        if len(self.votes) >= self.quorum:
            self._become_leader(now)

    def _become_leader(self, now):
        self.role, self.leader, self.leader_boot = "leader", self.robot, self.boot
        self.elected_at = now
        self.next_index = {p: self.last + 1 for p in self._peers()}
        self.match = {self.robot: self.last}
        self.proofs.clear()
        self._append({"type": "NOOP", "id": f"noop:{self.term}:{self.boot}"})
        self.broadcast(now)

    def _append(self, command):
        self.disk["log"].append(
            {
                "index": self.last + 1,
                "term": self.term,
                "command": copy.deepcopy(command),
            }
        )
        self._save()
        self.match[self.robot] = self.last
        self._advance()

    def propose(self, command, now):
        if not self.authority(now):
            raise ValueError("not current quorum-backed leader")
        if not isinstance(command, dict) or not isinstance(command.get("id"), str):
            raise ValueError("command id required")
        for i in range(1, self.last + 1):
            old = self.entry(i)["command"]
            if old["id"] == command["id"]:
                if digest(old) != digest(command):
                    raise ValueError("conflicting command identity")
                self.broadcast(now)
                return i
        validate_command_size(command)
        self._append(command)
        self.broadcast(now)
        return self.last

    def read_barrier(self, request, now):
        if not self.authority(now):
            return False
        if not isinstance(request, str) or not 0 < len(request) <= 128:
            return False
        if request not in self.reads:
            if len(self.reads) >= 128:
                return False
            self.reads[request] = {
                "started": now,
                "acks": {self.robot},
                "index": self.disk["commit"],
            }
        self._finish_reads()
        self.broadcast(now)
        return True

    def _finish_reads(self):
        for key, read in list(self.reads.items()):
            if len(read["acks"] - set(self.disk["excluded"])) >= self.quorum:
                self.read_ready.append(
                    {
                        "request_id": key,
                        "term": self.term,
                        "commit_index": read["index"],
                    }
                )
                del self.reads[key]

    def broadcast(self, now):
        self.last_sent = now
        for peer in self._peers():
            self._replicate(peer, now)

    def _replicate(self, peer, now):
        rpc = uuid.uuid4().hex
        first = self.next_index.get(peer, self.last + 1)
        entries = entry_batch(self.entry, first, self.last)
        reads = list(self.reads)
        last = first - 1 + len(entries)
        self.rpcs[rpc] = {"peer": peer, "sent": now, "reads": reads, "last": last}
        self._wire(
            peer, "append", rpc=rpc, prev=first - 1,
            prev_term=self.entry(first - 1)["term"], entries=entries,
            commit=self.disk["commit"], reads=reads,
        )

    def _advance(self):
        indexes = [
            self.match.get(p, 0) if p not in self.disk["excluded"] else 0
            for p in self.disk["voters"]
        ]
        match = sorted(indexes, reverse=True)[self.quorum - 1]
        if match > self.disk["commit"] and self.entry(match)["term"] == self.term:
            self.disk["commit"] = match
            self._save()

    def compact(self):
        count = self.disk["commit"] - self.base
        if count > 0:
            self.disk["snapshot"].extend(self.disk["log"][:count])
            del self.disk["log"][:count]
            self._save()

    def receive(self, sender, message, now):
        if (
            not self.eligible()
            or sender not in self._peers()
            or message.get("sender") != sender
            or message.get("epoch") != self.disk["epoch"]
            or not isinstance(message.get("boot"), str)
        ):
            return
        bindings = self.disk["identities"]
        if bindings and bindings.get(sender) != message["boot"]:
            return
        term, kind = message.get("term"), message.get("kind")
        if type(term) is not int or term < 0:
            return
        if kind == "pre_vote":
            up_to_date = (message["last_term"], message["last_index"]) >= (
                self.entry(self.last)["term"],
                self.last,
            )
            recent = now - self.last_contact < self.election_timeout or self.authority(
                now
            )
            self._wire(
                sender,
                "pre_vote_reply",
                proposed_term=message["proposed_term"],
                granted=message["proposed_term"] > self.term
                and up_to_date
                and not recent,
            )
            return
        if term > self.term:
            self._step_down(term, now)
        if kind == "pre_vote_reply":
            if (
                self.role == "pre_candidate"
                and message.get("proposed_term") == self.pre_term
                and message.get("granted") is True
            ):
                self.prevotes.add(sender)
                if len(self.prevotes) >= self.quorum:
                    self._campaign(now)
            return
        if term < self.term:
            self._wire(sender, "term_reject")
            return
        if kind == "vote":
            up_to_date = (message["last_term"], message["last_index"]) >= (
                self.entry(self.last)["term"],
                self.last,
            )
            granted = up_to_date and self.disk["vote"] in {None, sender}
            if granted:
                self.disk["vote"] = sender
                self._save()
                self._deadline(now)
            self._wire(sender, "vote_reply", granted=granted)
        elif (
            kind == "vote_reply"
            and self.role == "candidate"
            and message.get("granted") is True
        ):
            self.votes.add(sender)
            if len(self.votes) >= self.quorum:
                self._become_leader(now)
        elif kind in {"append", "snapshot"}:
            self._step_down(term, now, sender, message["boot"])
            self.last_contact = now
            if kind == "snapshot":
                entries = message["entries"]
                size = len(entries)
                valid = all(e.get("index") == i + 1 for i, e in enumerate(entries))
                if not valid or size < self.disk["commit"]:
                    self._wire(
                        sender,
                        "append_reply",
                        rpc=message["rpc"],
                        success=False,
                        match=self.last,
                    )
                    return
                # Never accept a snapshot contradicting any committed local prefix.
                if any(
                    entries[i - 1] != self.entry(i)
                    for i in range(1, self.disk["commit"] + 1)
                ):
                    raise RuntimeError("snapshot conflicts with committed history")
                tail = (
                    [self.entry(i) for i in range(size + 1, self.last + 1)]
                    if size <= self.last
                    and (not size or self.entry(size)["term"] == entries[-1]["term"])
                    else []
                )
                self.disk["snapshot"], self.disk["log"], self.disk["commit"] = (
                    copy.deepcopy(entries),
                    tail,
                    size,
                )
                self._save()
                match = size
            else:
                prev = message["prev"]
                if (
                    prev < 0
                    or prev > self.last
                    or self.entry(prev)["term"] != message["prev_term"]
                ):
                    self._wire(
                        sender,
                        "append_reply",
                        rpc=message["rpc"],
                        success=False,
                        match=max(self.base, min(prev - 1, self.last)),
                    )
                    return
                entries = message["entries"]
                if any(e.get("index") != prev + i + 1 for i, e in enumerate(entries)):
                    return
                for incoming in entries:
                    index = incoming["index"]
                    if index <= self.last and self.entry(index) != incoming:
                        if index <= self.disk["commit"]:
                            raise RuntimeError("attempt to overwrite committed history")
                        del self.disk["log"][index - self.base - 1 :]
                    if index > self.last:
                        self.disk["log"].append(copy.deepcopy(incoming))
                match = prev + len(entries)
                self.disk["commit"] = max(
                    self.disk["commit"], min(message["commit"], match)
                )
                self._save()
            self._wire(
                sender, "append_reply", rpc=message["rpc"], success=True, match=match
            )
        elif kind == "append_reply" and self.role == "leader":
            rpc = self.rpcs.pop(message.get("rpc"), None)
            if not rpc or rpc["peer"] != sender:
                return
            if message.get("success") is True:
                if message.get("match") != rpc["last"]:
                    return
                self.match[sender] = max(
                    self.match.get(sender, 0), rpc["last"]
                )
                self.next_index[sender] = self.match[sender] + 1
                # An old delayed ACK cannot renew quorum authority.
                self.proofs[sender] = max(
                    self.proofs.get(sender, -float("inf")), rpc["sent"]
                )
                for key in rpc["reads"]:
                    if key in self.reads and now - rpc["sent"] < self.quorum_timeout:
                        self.reads[key]["acks"].add(sender)
                self._advance()
                self._finish_reads()
                if self.next_index[sender] <= self.last:
                    self._replicate(sender, now)
            else:
                self.next_index[sender] = max(
                    self.match.get(sender, 0) + 1,
                    min(
                        self.next_index.get(sender, self.last + 1) - 1,
                        message["match"] + 1,
                    ),
                )
                self._replicate(sender, now)

    def state(self, now):
        return {
            "round_epoch": self.disk["epoch"],
            "term": self.term,
            "role": self.role,
            "leader_id": self.leader,
            "leader_boot": self.leader_boot,
            "authority": self.authority(now),
            "commit_index": self.disk["commit"],
            "last_index": self.last,
            "voters": self.disk["voters"],
            "excluded": self.disk["excluded"],
            "recovering": self.recovering,
            "error": self.error,
            "leader_contact_recent": bool(
                self.leader and now - self.last_contact < self.quorum_timeout
            ),
        }
