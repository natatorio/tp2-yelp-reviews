from datetime import datetime
import sched
import time
import random
from threading import Lock

RAFT_SCHED = sched.scheduler(time.time, time.sleep)
HEARBEAT_TIMEOUT = 50


def generate_election_timeout():
    return random.randint(100, 300)


class Follower:
    def __init__(self, context):
        self.context = context
        self.election_timeout = generate_election_timeout()
        self.last_message_time = datetime.now()
        RAFT_SCHED.enter(self.context.election_timeout, 1, self.on_election_timeout)

    def on_election_timeout(self):
        elapsed_time = datetime.now() - self.last_message_time
        if elapsed_time.total_seconds() * 1000 < self.election_timeout:
            self.context.as_candidate()
        else:
            RAFT_SCHED.enter(self.election_timeout, 1, self.on_election_timeout)

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
        return {"success": False, "redirect": self.context.vote_for}


class Candidate:
    def __init__(self, context):
        self.context = context
        self.found_better_leader = False
        self.start_election()

    def start_election(self):
        self.context.__vote_self__()
        votes = 1
        for replica in self.context.replicas:
            if self.found_better_leader:
                return
            if replica == self.context.name:
                continue
            res = replica.request_vote(
                {
                    "term": self.context.current_term,
                    "canditate_id": self.context.name,
                    "last_log_index": len(self.context.entries) - 1,
                    "last_log_term": self.context.entries[-1],
                }
            )

            if res["vote_granted"]:
                votes += 1

        if votes >= int(len(self.context.replicas) / 2) + 1:
            self.context.as_leader()
        elif not self.found_better_leader:
            self.election_timeout = generate_election_timeout()
            RAFT_SCHED.enter(self.election_timeout, 1, self.start_election)

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
            replica: len(self.context.entries) for replica in self.context.replicas
        }
        self.match_index = {replica: 0 for replica in self.context.replicas}
        RAFT_SCHED.enter(self.heartbeat_timeout, 1, self.heartbeat)
        # TODO commit blank to force replication sync

    def heartbeat(self):
        if self.context.vote_for == self.context.name:
            commited = self.update_replicas()
            self.context.__update_commit_index__(commited)
            RAFT_SCHED.enter(self.heartbeat_timeout, 1, self.heartbeat)

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
            res = replica.append_entries(
                {
                    "term": self.context.current_term,
                    "leader_id": self.context.name,
                    "prev_log_index": prev_log_index,
                    "prev_log_term": prev_log_term,
                    "entries": self.context.entries[(prev_log_index + 1) :],
                    "leader_commit": self.context.commit_index,
                }
            )
            if res["success"]:
                self.next_index[replica] = len(self.context.entries)
                self.match_index[replica] = len(self.context.entries)
            else:
                self.next_index[replica] -= 1

        for commited in sort_by_mayority(self.match_index.values()):
            if commited <= self.context.commit_index:
                break
            if self.context.current_term == self.context.entries[commited]["term"]:
                # TODO concurrency over context.commit_index
                return commited
        return self.context.commit_index

    def append_entry(self, req):
        entry_index = len(self.context.entries)
        self.context.__append_entry__(
            entry_index,
            {
                "term": self.context.current_term,
                "data": req,
            },
        )
        self.context.entries += req
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
        self.entries = []

        self.commit_index = -1
        self.last_applied = -1

        self.lock = Lock()
        self.state = Follower(self)

    def append_entry(self, req):
        return self.state.append_entry(req)

    def __append_entry__(self, index, req):
        self.entries[index] = req

    def append_entries(self, req):
        return self.state.append_entries(req)

    def __vote_self__(self):
        with self.lock:
            self.voted_for = self.name
            self.current_term += 1

    def __append_entries__(self, req):
        #  {  "term": 0,
        #     "leader_id": "",
        #     "prev_log_index": 0,
        #     "prev_log_term": 0,
        #     "entries": [],
        #     "leader_commit": 0 }
        with self.lock:
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

    def request_vote(self, req):
        return self.state.request_vote(req)

    def __request_vote__(self, req):
        # {"term":0,
        #  "canditate_id":"",
        #  "last_log_index": 0,
        #  "last_log_term":0, }
        def upto_date(self, req):
            last_log_term = self.entries[-1]["term"]
            return (
                last_log_term == req["last_log_term"]
                and len(self.entries) - 1 <= req["last_log_index"]
            ) or (last_log_term < req["last_log_term"])

        with self.lock:
            if req["term"] >= self.current_term:
                if self.voted_for is None or self.voted_for == req["candidate_id"]:
                    if upto_date(self, req):
                        self.voted_for = req["candidate_id"]
                        self.current_term = req["term"]
                        return {
                            "term": self.current_term,
                            "vote_granted": True,
                        }
            return {
                "term": self.current_term,
                "vote_granted": False,
            }

    def __update_commit_index__(self, commited):
        with self.lock:
            prev_index = self.commit_index
            self.commit_index = commited
            self.machine.run(self.entries[prev_index : commited + 1])

    def as_candidate(self):
        with self.lock:
            self.state = Candidate(self)

    def as_leader(self):
        with self.lock:
            self.state = Leader(self)

    def as_follower(self):
        with self.lock:
            self.state = Follower(self)
