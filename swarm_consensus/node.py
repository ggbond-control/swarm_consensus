"""ROS String adapter; never opens a network socket or controls a route."""

import time
import uuid

from rclpy.node import Node
from rclpy.qos import DurabilityPolicy, QoSProfile
from std_msgs.msg import String
from swarm_protocol.contracts import decode
from swarm_protocol.ros import main_node, publish, runtime_path
from swarm_protocol.storage import DurableFile, ProcessLock

from .raft import Raft


class ConsensusNode(Node):
    def __init__(self):
        super().__init__("swarm_consensus")
        for key, value in [
            ("robot_id", ""),
            ("election_timeout", 5.0),
            ("heartbeat_period", 0.4),
            ("quorum_timeout", 3.0),
        ]:
            self.declare_parameter(key, value)
        self.robot = self.get_parameter("robot_id").value
        if not self.robot:
            raise ValueError("robot_id is required")
        root = runtime_path(self, "consensus")
        self.lock = ProcessLock(root / "process.lock")
        self.boot = uuid.uuid4().hex
        self.out = self.create_publisher(String, "/swarm/consensus/outbound", 100)
        qos = QoSProfile(depth=1, durability=DurabilityPolicy.TRANSIENT_LOCAL)
        self.state_pub = self.create_publisher(String, "/swarm/consensus/state", qos)
        self.read_pub = self.create_publisher(
            String, "/swarm/consensus/read_confirmed", 100
        )
        self.engine = Raft(
            self.robot,
            self.boot,
            DurableFile(root / "raft.json"),
            self.send,
            election_timeout=self.get_parameter("election_timeout").value,
            heartbeat=self.get_parameter("heartbeat_period").value,
            quorum_timeout=self.get_parameter("quorum_timeout").value,
        )
        for topic, callback in [
            ("/swarm/consensus/configure", self.configure),
            ("/swarm/consensus/inbound", self.inbound),
            ("/swarm/consensus/propose", self.propose),
            ("/swarm/consensus/read", self.read),
        ]:
            self.create_subscription(
                String, topic, lambda msg, cb=callback: self.safe(cb, msg), 100
            )
        self.identity_applied = 0
        self.create_timer(0.05, self.tick)
        self.signature = None

    def safe(self, callback, message):
        try:
            callback(decode(message.data))
        except Exception as error:
            self.get_logger().error(
                f"共识拒绝消息／持久化失败：{type(error).__name__}: {error}"
            )

    def send(self, peer, payload):
        publish(self.out, {"to": peer, "data": payload})

    def configure(self, value):
        self.engine.configure(
            value["round_epoch"], value["robot_ids"], time.monotonic()
        )
        self.identity_applied = 0

    def inbound(self, value):
        self.engine.receive(value["sender"], value["data"], time.monotonic())

    def propose(self, value):
        self.engine.propose(value, time.monotonic())

    def read(self, value):
        self.engine.read_barrier(value["request_id"], time.monotonic())

    def tick(self):
        try:
            self.engine.tick(time.monotonic())
            for entry in self.engine.committed()[self.identity_applied :]:
                command = entry["command"]
                if command["type"] == "ACTIVATE":
                    self.engine.bind_identities(
                        {r: s["consensus_boot"] for r, s in command["sessions"].items()}
                    )
                elif command["type"] == "EXCLUDE":
                    self.engine.exclude(command["robot_id"])
                self.identity_applied = entry["index"]
            if len(self.engine.disk["log"]) > 256:
                self.engine.compact()
            state = self.engine.state(time.monotonic())
            state.update(
                robot_id=self.robot, boot_id=self.boot, entries=self.engine.committed()
            )
            publish(self.state_pub, state)
            for read in self.engine.read_ready:
                publish(self.read_pub, read)
            self.engine.read_ready.clear()
            signature = (
                state["term"],
                state["role"],
                state["leader_id"],
                state["authority"],
                state["commit_index"],
            )
            if signature != self.signature:
                self.get_logger().info(
                    f"共识状态：robot={self.robot} epoch={state['round_epoch']} "
                    f"term={state['term']} role={state['role']} leader={state['leader_id']} "
                    f"quorum={state['authority']} commit={state['commit_index']}"
                )
                self.signature = signature
        except Exception as error:
            self.engine.error = f"fail-closed: {type(error).__name__}"
            self.get_logger().error(f"共识停止授权：{error}")

    def destroy_node(self):
        self.lock.close()
        return super().destroy_node()


def main(args=None):
    main_node(ConsensusNode, args)
