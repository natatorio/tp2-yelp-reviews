from datetime import datetime, timedelta
import random
from threading import RLock
from scheduler import Scheduler
import requests
import logging

HEARBEAT_TIMEOUT = 10000

logger = logging.getLogger("raft")


def generate_election_timeout():
    return random.randint(200, 300) * 100


class Follower:
    def __init__(self, context):
        self.context = context
        self.election_timeout = generate_election_timeout()
        self.last_message_time = datetime.now()
        self.context.schedule(self.election_timeout, self.on_election_timeout)

    def on_election_timeout(self):
        elapsed_time = datetime.now() - self.last_message_time
        if elapsed_time.total_seconds() * 1000 >= self.election_timeout:
            self.context.as_candidate()
        else:
            self.context.schedule(self.election_timeout, self.on_election_timeout)

    def append_entries(self, req):
        self.last_message_time = datetime.now()
        if self.context.voted_for == req["leader_id"]:
            return self.context.__append_entries__(req)
        # TODO raft spec says don't respond,
        # instead responding false
        return {
            "success": False,
            "term": self.context.current_term,
        }

    def request_vote(self, req):
        self.last_message_time = datetime.now()
        return self.context.__request_vote__(req)

    def append_entry(self, req):
        return {"success": False, "redirect": self.context.voted_for}


class Candidate:
    def __init__(self, context):
        self.context = context
        self.found_better_leader = False
        self.context.schedule(0, self.start_election)

    def start_election(self):
        logger.info(f"{self.context.name} started election")
        self.context.__vote__(self.context.name, self.context.current_term + 1)
        votes = 1
        for replica in self.context.replicas:
            if self.found_better_leader:
                return
            if replica == self.context.name:
                continue

            res = self.context.fetch(
                replica,
                "request_vote",
                {
                    "term": self.context.current_term,
                    "candidate_id": self.context.name,
                    "last_log_index": len(self.context.entries) - 1,
                    "last_log_term": self.context.entries[-1]["term"],
                },
            )

            if res and res["vote_granted"]:
                votes += 1
        logger.info(f"votes {votes}")
        if votes >= int(len(self.context.replicas) / 2) + 1:
            self.context.as_leader()
        elif not self.found_better_leader:
            self.election_timeout = generate_election_timeout()
            self.context.schedule(self.election_timeout, self.start_election)

    def append_entries(self, req):
        res = self.context.__append_entries__(req)
        if res["success"]:
            self.found_better_leader = True
            self.context.as_follower()
        return res

    def request_vote(self, req):
        res = self.context.__request_vote__(req)
        if res["vote_granted"]:
            self.found_better_leader = True
            self.context.as_follower()
        return res

    def append_entry(self, req):
        logger.info("candidate")
        return {"success": False}


def sort_by_mayority(match_index_values):
    N = len(match_index_values)
    count = {}
    for s in match_index_values:
        for (k, _) in count.items():
            if k <= s:
                count[k] += 1
        if count.get(s) is None:
            count[s] = 1
    return list(
        map(
            lambda it: it[0],
            sorted(
                filter(lambda it: it[1] >= int(N / 2) + 1, count.items()),
                key=lambda it: it[1],
            ),
        )
    )


class Leader:
    def __init__(self, context):
        self.context = context
        self.heartbeat_timeout = HEARBEAT_TIMEOUT
        self.next_index = {
            replica: max(len(self.context.entries) - 1, 0) + 1
            for replica in self.context.replicas
        }
        self.match_index = {replica: 0 for replica in self.context.replicas}
        self.context.schedule(self.heartbeat_timeout, self.heartbeat)
        # TODO commit blank to force replication sync

    def heartbeat(self):
        if self.context.voted_for == self.context.name:
            commited = self.update_replicas()
            self.context.__update_commit_index__(commited)
            self.context.schedule(self.heartbeat_timeout, self.heartbeat)

    def append_entries(self, req):
        return self.context.__append_entries__(req)

    def request_vote(self, req):
        return self.context.__request_vote__(req)

    def update_replicas(self):
        for replica in self.context.replicas:
            if replica == self.context.name:
                self.match_index[replica] = len(self.context.entries)
                continue

            prev_log_index = self.next_index[replica] - 1
            prev_log_term = self.context.entries[prev_log_index]["term"]
            res = self.context.fetch(
                replica,
                "append_entries",
                {
                    "term": self.context.current_term,
                    "leader_id": self.context.name,
                    "prev_log_index": prev_log_index,
                    "prev_log_term": prev_log_term,
                    "entries": self.context.entries[(prev_log_index + 1) :],
                    "leader_commit": self.context.commit_index,
                },
            )
            if res and res["success"]:
                self.next_index[replica] = len(self.context.entries)
                self.match_index[replica] = len(self.context.entries) - 1
            else:
                self.next_index[replica] -= 1

        commited_sorted_by_mayority = sort_by_mayority(self.match_index.values())
        logger.info(f"commited: {commited_sorted_by_mayority}")
        for commited in commited_sorted_by_mayority:
            if commited <= self.context.commit_index:
                break
            if self.context.current_term == self.context.entries[commited]["term"]:
                # TODO concurrency over context.commit_index
                return commited
        return self.context.commit_index

    def append_entry(self, req):
        logger.info("acá")
        entry_index = len(self.context.entries)
        self.context.__append_entry__(
            entry_index,
            {
                "term": self.context.current_term,
                "data": req,
            },
        )
        commited = self.update_replicas()
        self.context.__update_commit_index__(commited)
        if commited == entry_index:
            return {
                "success": True,
                "id": entry_index,
            }
        return {
            "success": False,
            "id": entry_index,
        }


class Raft:
    def __init__(self, name, replicas, machine) -> None:
        self.name = name
        self.replicas = replicas
        self.machine = machine

        self.current_term = 0
        self.voted_for = None
        self.entries = [{"term": 0, "data": None}]

        self.commit_index = 0
        self.last_applied = 0

        self.lock = RLock()
        self.scheduler = Scheduler()

        self.as_follower()

    def append_entry(self, req):
        return self.state.append_entry(req)

    def append_entries(self, req):
        return self.state.append_entries(req)

    def request_vote(self, req):
        return self.state.request_vote(req)

    def __append_entry__(self, index, req):
        logger.info(f"append_entry: {req} {index}")
        if len(self.entries) >= index:
            self.entries += [req]
        else:
            self.entries[index] = req

    def __vote__(self, voted_for, term):
        with self.lock:
            self.voted_for = voted_for
            self.current_term = term

    def __append_entries__(self, req):
        #  {  "term": 0,
        #     "leader_id": "",
        #     "prev_log_index": 0,
        #     "prev_log_term": 0,
        #     "entries": [],
        #     "leader_commit": 0 }
        if req["term"] < self.current_term:
            return {
                "term": self.current_term,
                "success": False,
            }
        if req["prev_log_index"] >= len(self.entries):
            return {
                "term": self.current_term,
                "success": False,
            }
        else:
            if self.entries[req["prev_log_index"]]["term"] != req["prev_log_term"]:
                return {
                    "term": self.current_term,
                    "success": False,
                }
                # TODO should do consistency check for differing entries,
                # instead just override with new entries from leader
        for i in range(0, len(req["entries"])):
            self.__append_entry__(req["prev_log_index"] + 1 + i, req["entries"][i])

        if req["leader_commit"] > self.commit_index:
            self.__update_commit_index__(
                min(
                    req["leader_commit"],
                    len(self.entries) - 1,
                )
            )

        return {
            "term": self.current_term,
            "success": True,
        }

    def __request_vote__(self, req):
        # {"term":0,
        #  "canditate_id":"",
        #  "last_log_index": 0,
        #  "last_log_term":0, }
        def upto_date(self, req):
            last_log_term = self.entries[-1]["term"]
            return last_log_term < req["last_log_term"] or (
                last_log_term == req["last_log_term"]
                and len(self.entries) - 1 <= req["last_log_index"]
            )

        if req["term"] >= self.current_term:
            if (
                self.voted_for is None
                or self.voted_for == req["candidate_id"]
                or self.voted_for == self.name
            ):
                if upto_date(self, req):
                    self.__vote__(req["candidate_id"], req["term"])
                    return {
                        "term": self.current_term,
                        "vote_granted": True,
                    }
        return {
            "term": self.current_term,
            "vote_granted": False,
        }

    def __update_commit_index__(self, commited):
        prev_index = self.commit_index
        self.commit_index = commited
        if self.machine:
            self.machine.run(self.entries[prev_index : commited + 1])

    def as_candidate(self):
        with self.lock:
            logger.info(f"{self.name} as candidate")
            self.state = None
            self.state = Candidate(self)

    def as_leader(self):
        with self.lock:
            logger.info(f"{self.name} as leader")
            self.state = None
            self.state = Leader(self)

    def as_follower(self):
        with self.lock:
            logger.info(f"{self.name} as follower")
            self.state = None
            self.state = Follower(self)

    def fetch(self, replica, service, data):
        try:
            response = requests.post(f"http://{replica}/{service}", json=data)
            if response.status_code == 200:
                return response.json()
            logger.info(
                f"{response.headers['Content-type']}, {response.status_code}, {response.text}"
            )
        except Exception:
            logger.exception(f"Calling {replica}/{service}")
        return None

    def schedule(self, delaymillis, func):
        # logger.info(self.name, " schedule ", delaymillis)
        self.scheduler.schedule(
            timedelta(milliseconds=delaymillis),
            func,
        )


from flask import Flask, request
import os

if __name__ == "__main__":

    app = Flask(__name__)
    logging.basicConfig(level=logging.DEBUG)
    raft = Raft(os.environ["NAME"], os.environ["REPLICAS"].split(","), None)

    @app.route("/request_vote", methods=["POST"])
    def request_vote():
        data = request.get_json()
        return raft.request_vote(data)

    @app.route("/append_entries", methods=["POST"])
    def append_entries():
        data = request.get_json()
        return raft.append_entries(data)

    @app.route("/append_entry", methods=["POST"])
    def append_entry():
        data = request.get_json()
        return raft.append_entry(data)

    @app.route("/show")
    def show():
        res = {
            "entries": raft.entries,
            "voted_for": raft.voted_for,
            "current_term": raft.current_term,
            "commit_index": raft.commit_index,
            "replicas": raft.replicas,
            "name": raft.name,
            "state": raft.state.__class__.__name__,
        }

        # raft.state = Leader(raft)
        return res

    app.run(host="0.0.0.0", port=80, threaded=True)

response = requests.post("http://localhost:8082/append_entry", json={"last": 2})
