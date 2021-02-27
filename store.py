import json
from threading import Lock
import requests
from datetime import datetime


class Client:
    def replicate(self, replica, idVal):
        r = requests.post(replica, json=idVal)
        if r.status_code != 200:
            return False
        return True

    def cluster_info(self, replica):
        pass

    def get(self, replica, id):
        pass

    def put(self, replica, idVal):
        pass

    def snapshot(self, replica):
        pass


class FileStore:
    def __init__(
        self,
        log_file_path,
        snapshot_file_path,
        max_snapshot_size,
    ) -> None:
        self.log_file_path = log_file_path
        self.snapshot_file_path = snapshot_file_path
        self.lock = Lock()
        self.data = {}
        self.timestamp = 0
        self.prev = -1
        self.max_snapshot_size = max_snapshot_size

        def recover(self):
            try:
                self.snapshot_file = open(self.snapshot_file_path, "a+")
                self.snapshot_file.seek(0)
                snapshot = self.snapshot_file.readline()
                if snapshot:
                    snapshot_data = json.loads(snapshot)
                    self.timestamp = snapshot_data["timestamp"]
                    self.data = snapshot_data["data"]

                self.log_file = open(self.log_file_path, "a+")
                self.log_file.seek(0)
                line = self.log_file.readline()
                while line:
                    self.prev = self.log_file.tell()
                    event = json.loads(line)
                    self.data[event["id"]] = event["val"]
                    self.timestamp += 1
                    line = self.log_file.readline()
            except Exception as e:
                if self.snapshot_file:
                    self.snapshot_file.close()
                if self.log_file:
                    self.log_file.close()
                raise e

        recover(self)

    def get(self, id):
        return self.data[id]

    def put(self, idVal):
        with self.lock:
            idVal["timestamp"] = self.timestamp
            idVal["prev"] = self.prev
            self.prev = self.log_file.tell()
            self.log_file.write(json.dumps(idVal) + "\n")
            self.data[idVal["id"]] = idVal["val"]

            if self.log_file.tell() > self.max_snapshot_size:
                self.snapshot_file.seek(0)
                self.prev = -1
                self.snapshot_file.truncate(0)
                self.snapshot_file.write(
                    json.dumps(
                        {
                            "data": self.data,
                            "timestamp": self.timestamp,
                        }
                    )
                    + "\n"
                )
                self.snapshot_file.flush()
                self.log_file.seek(0)
                self.log_file.truncate(0)

            self.timestamp += 1
            self.log_file.flush()
        return True

    def reset(self, snapshot):
        with self.lock:
            self.data = snapshot["data"]
            self.timestamp = snapshot["timestamp"]
            self.log_file.truncate(0)
            self.prev = -1
            self.snapshot_file.truncate(0)
            self.snapshot_file.write(json.dumps(snapshot) + "\n")
            self.snapshot_file.flush()

    def close(self):
        self.log_file.close()
        self.snapshot_file.close()


# fstore = FileStore("/tmp/log", "/tmp/snap", 1024)
# for i in range(500):
# fstore.put({"id": 1000 + i, "val": 1000 + i})
# fstore.put({"id": 201, "val": 201})
# fstore.reset({"snapshot":{1:1, 2:2}, "timestamp": 2})
# fstore.close()


class ClusterStore:
    def __init__(self, file_store: FileStore, client: Client, name, seeds) -> None:
        self.client = client
        self.file_store = file_store
        self.name = name

    def get(self, id):
        return self.file_store.get(id)

    def put(self, idVal):
        self.file_store.put(idVal)
        self.broadcast(idVal)

    def broadcast(self, idVal):
        count = 0
        for replica in self.replicas:
            if self.client.replicate(replica, idVal):
                count += 1
        if count < int(len(self.replicas) / 2) + 1:
            raise Exception("Not enough replicas")

    def elect_leader(self):
        pass

    def join(self, seeds):
        for seed in seeds:
            cluster = self.client.cluster_info(seed)
            if cluster:
                self.leader = cluster["leader"]
                self.replicas = cluster["replicas"]
                break

        if self.leader is None:
            self.elect_leader()
            return False

        if self.leader != self.name:
            self.file_store.reset(self.client.snapshot(self.leader))
        return True

    def snapshot(self):
        return {
            "data": self.file_store.data,
            "timestamp": self.file_store.timestamp,
        }


class Paxos:
    def prepare(self):
        pass

    def promise(self):
        pass

    def accept(self):
        pass

    def accepted(self):
        pass


class Raft:
    def __init__(self, replicas, leader_timeout, name) -> None:
        self.term = 0
        self.sqn = 0
        self.replicas = replicas
        self.leader_timeout = leader_timeout
        self.last_leader_heartbeat = None
        self.name = name

    def watch_leader(self):
        if self.leader == self.name:
            return
        if self.status == "candidate":
            return
        if self.last_leader_heartbeat is None or (
            (datetime.now() - self.last_leader_heartbeat).total_seconds() * 1000
            > self.leader_timeout
        ):
            self.proposeAsCandidate()

    def make_proposal(self):
        return {
            "command": "requestVote",
            "term": self.term + 1,
            "sqn": self.sqn,
            "leader": self.name,
        }

    def requestVote(self):
        proposal = self.make_proposal()
        results = self.broadcast(proposal)

    def vote(self, proposal):
        if proposal["term"] < self.term:
            return self.make_proposal()
        if proposal["term"] == self.term:
            if proposal["sqn"] < self.sqn:
                return self.make_proposal()
        self.term = proposal["term"]
        self.sqn = proposal["sqn"]
        return proposal

    def write(self):
        pass


class Raft:
    def __init__(self, name, replicas, client):
        self.state = "follower"
        self.name = name
        self.replicas = replicas
        self.client = client
        self.term = -1
        self.sqn = 0

    def run_voting(self):
        votes = 1
        voters = 1
        for rep in self.replicas:
            if rep == self.name:
                continue
            try:
                vote = self.client.request_vote(
                    rep,
                    {
                        "candidate": self.name,
                        "term": self.term + 1,
                        "sqn": self.sqn,
                    },
                )
                voters += 1
                if vote["leader"] == self.name:
                    votes += 1
                elif vote["term"] > self.term or (
                    vote["term"] == self.term and self.sqn <= vote["sqn"]
                ):
                    self.on_better_leader(vote)
                    return
            except:
                continue
        if votes > int(voters / 2) + 1:
            self.on_mayority_votes()

    def candidate(self):
        self.state = "candidate"
        self.run_voting()

    def follower(self):
        self.state = "follower"

    def leader(self):
        self.state = "leader"

    def on_leader_timeout(self):
        if self.state == "follower":
            self.candidate()
        if self.state == "candidate":
            self.candidate()

    def on_mayority_votes(self):
        if self.state == "candidate":
            self.leader()

    def on_better_leader(self):
        if self.state == "leader":
            self.follower()

    def on_new_term(self):
        if self.state == "candidate":
            self.follower()

    def on_new_leader(self):
        if self.state == "candidate":
            self.follower()
