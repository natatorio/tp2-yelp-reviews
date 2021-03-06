from datetime import datetime, timedelta
import json
import os
import random
from threading import RLock

from scheduler import Scheduler
import requests
import logging

HEARBEAT_TIMEOUT = int(os.environ.get("HEARBEAT_TIMEOUT", "2000"))
FINAL_ELECTION_TIMEOUT = int(os.environ.get("ELECTION_TIMEOUT", "5000"))
HOUSEKEEPING_TIMEOUT = int(os.environ.get("HOUSEKEEPING_TIMEOUT", 10 * 1000))
HOUSEKEEPING_MAX_SIZE = int(os.environ.get("HOUSEKEEPING_MAX_SIZE", 128)) * 1024 * 1024

logger = logging.getLogger("raft")
logger.setLevel(logging.INFO)


def create_if_not_exists(file_path):
    if not os.path.exists(file_path):
        f = open(file_path, "w+")
        f.close()
    return file_path


def swap(src, dest, open_file=""):
    if os.path.exists(src):
        os.remove(src)
    os.rename(dest, src)
    if open_file:
        return open(src, open_file)
    return None


def write_json_line(f, data):
    f.write(json.dumps(data) + "\n")


ELECTION_TIMEOUT = 100


def generate_election_timeout():
    global ELECTION_TIMEOUT
    timeout = random.randint(10, 20) / 10 * ELECTION_TIMEOUT
    ELECTION_TIMEOUT = FINAL_ELECTION_TIMEOUT
    return timeout


class Follower:
    def __init__(self, context):
        self.context = context
        self.election_timeout = generate_election_timeout()
        self.last_message_time = datetime.now()
        self.context.schedule(self.election_timeout, self.on_election_timeout)

    def on_election_timeout(self):
        if self.context.state != self:
            return
        elapsed_time = datetime.now() - self.last_message_time
        if elapsed_time.total_seconds() * 1000 >= self.election_timeout:
            self.context.as_candidate()
        else:
            self.context.schedule(self.election_timeout, self.on_election_timeout)

    def append_entries(self, req):
        if self.context.voted_for is None:
            self.context.voted_for = req["leader_id"]

        if req["leader_id"] == self.context.voted_for:
            self.last_message_time = datetime.now()
            return self.context.__append_entries__(req)

        return {
            "success": False,
            "snapshot_version": self.context.snapshot_version,
            "term": self.context.current_term,
        }

    def request_vote(self, req):
        self.last_message_time = datetime.now()
        return self.context.__request_vote__(req)

    def append_entry(self, req):
        return {"success": False, "redirect": self.context.voted_for}

    def results(self, query):
        return {"success": False, "redirect": self.context.voted_for}

    def snapshot(self):
        return {"success": False, "redirect": self.context.voted_for}


class Candidate:
    def __init__(self, context):
        self.context = context
        self.context.__vote__(self.context.name, self.context.current_term + 1)
        self.context.schedule(0, self.start_election)

    def start_election(self):
        logger.info(f"{self.context.name} started election")
        votes = 1
        max_term = 0
        for replica in self.context.replicas:
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
                    "snapshot_version": self.context.snapshot_version,
                },
            )
            logger.info("%s responded %s", replica, res)
            if res:
                if res["vote_granted"]:
                    votes += 1
                max_term = max(max_term, res["term"])
        logger.info(f"votes {votes} {self.context.replicas}")
        if votes >= int(len(self.context.replicas) / 2) + 1:
            self.context.as_leader()
        else:
            self.context.__vote__(None, max(max_term, self.context.current_term))
            self.context.as_follower()

    def append_entries(self, req):
        return {
            "term": self.context.current_term,
            "success": False,
            "snapshot_version": self.context.snapshot_version,
        }

    def request_vote(self, req):
        return {
            "term": self.context.current_term,
            "vote_granted": False,
            "snapshot_version": self.context.snapshot_version,
        }

    def append_entry(self, req):
        return {"success": False}

    def results(self, query):
        return {"success": False}

    def snapshot(self):
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


class HouseKeeper(Scheduler):
    def __init__(self, leader) -> None:
        super().__init__()
        self.owner = leader
        self.schedule(timedelta(milliseconds=HOUSEKEEPING_TIMEOUT), self.housekeeping)

    def housekeeping_needed(self):
        return self.owner.context.log.tell() > HOUSEKEEPING_MAX_SIZE

    def housekeeping(self):
        logger.info("housekeeping start")
        if self.owner.context.state != self.owner:
            logger.info("housekeeping not leader")
            return
        if self.housekeeping_needed():
            logger.info("running snapshot")
            self.owner.snapshot()
        self.schedule(timedelta(milliseconds=HOUSEKEEPING_TIMEOUT), self.housekeeping)
        logger.info("housekeeping done")


class Leader:
    def __init__(self, context):
        self.context = context
        self.heartbeat_timeout = HEARBEAT_TIMEOUT
        self.next_index = {
            replica: max(len(self.context.entries) - 1, 0) + 1
            for replica in self.context.replicas
        }
        self.snapshot_index = {
            replica: self.context.snapshot_version for replica in self.context.replicas
        }
        self.match_index = {replica: 0 for replica in self.context.replicas}
        self.context.schedule(self.heartbeat_timeout, self.heartbeat)
        self.last_timestamp = datetime.now()
        self.house_keeper = HouseKeeper(self)

    def heartbeat(self):
        if self.context.state != self:
            return
        elapsed_time = datetime.now() - self.last_timestamp
        if elapsed_time.total_seconds() * 1000 < self.heartbeat_timeout - 50:
            self.context.schedule(self.heartbeat_timeout, self.heartbeat)

        if self.context.voted_for == self.context.name:
            commited = self.update_replicas()
            if commited > self.context.commit_index:
                self.context.__update_commit_index__(commited)
            self.context.schedule(self.heartbeat_timeout, self.heartbeat)

    def append_entries(self, req):
        if req["term"] > self.context.current_term:
            self.context.__vote__(req["leader_id"], req["term"])
            self.house_keeper.shutdown()
            self.context.as_follower()
        return self.context.__append_entries__(req)

    def request_vote(self, req):
        voted = self.context.__request_vote__(req)
        if voted["vote_granted"]:
            self.house_keeper.shutdown()
            self.context.as_follower()
        return voted

    def update_replicas(self):
        for replica in self.context.replicas:
            if replica == self.context.name:
                self.match_index[replica] = len(self.context.entries) - 1
                self.next_index[replica] = len(self.context.entries)
                continue
            res = None
            try:
                if self.snapshot_index[replica] != self.context.snapshot_version:
                    logger.debug(f"snapshot update to {replica}")
                    res = self.context.fetch(
                        replica,
                        "append_entries",
                        {
                            "term": self.context.current_term,
                            "leader_id": self.context.name,
                            "snapshot": self.context.machine.snapshot(),
                            "snapshot_version": self.context.snapshot_version,
                        },
                    )
                    self.next_index[replica] = max(len(self.context.entries) - 1, 0) + 1
                    self.match_index[replica] = 0
                else:
                    logger.debug(
                        f"heartbeat/update data to {replica} {self.next_index[replica]}"
                    )
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
                            "snapshot_version": self.context.snapshot_version,
                        },
                    )
                logger.debug(f"{replica} response {res}")
                if res is None:
                    continue
                if res["success"]:
                    self.next_index[replica] = len(self.context.entries)
                    self.match_index[replica] = len(self.context.entries) - 1
                    continue
                if (
                    res.get("snapshot_version", self.context.snapshot_version)
                    < self.context.snapshot_version
                ):
                    self.next_index[replica] = 1
                    self.snapshot_index[replica] = res.get(
                        "snapshot_version", self.context.snapshot_version
                    )
                    continue
                if self.next_index[replica] > 1:
                    self.next_index[replica] -= 1
            except:
                logger.exception(f"Replica response: {res}")

        commited_sorted_by_mayority = sort_by_mayority(self.match_index.values())
        logger.debug(f"commited: {commited_sorted_by_mayority}")
        for commited in commited_sorted_by_mayority:
            if commited <= self.context.commit_index:
                break
            if self.context.current_term == self.context.entries[commited]["term"]:
                # TODO concurrency over context.commit_index
                return commited
        return self.context.commit_index

    def append_entry(self, req):
        self.last_timestamp = datetime.now()
        entry_index = len(self.context.entries)
        self.context.__append_entry__(
            entry_index,
            [
                {
                    "term": self.context.current_term,
                    "data": req,
                }
            ],
        )
        commited = self.update_replicas()
        if commited > self.context.commit_index:
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

    def results(self, query):
        return {
            "success": True,
            "data": self.context.machine.results(query),
        }

    def snapshot(self):
        self.context.__snapshot__()
        return {"success": True}


class NopVM:
    def set_context(self, context):
        return

    def load_file(self, context, file_name):
        self.set_context(context)
        if os.path.exists(file_name):
            with open(file_name, "r+") as snapshot_file:
                line = snapshot_file.readline()
                if line:
                    snapshot = json.loads(line)
                    self.reset(context, snapshot)

    def save_snapshot(self, file_name):
        with open(file_name, "w+") as f:
            json.dump(self.snapshot(), f)

    def reset(self, context, snapshot):
        return {}

    def snapshot(self):
        return {}

    def run(self, commands):
        return {}

    def results(self, query):
        return []


class Raft:
    def __fix_backups__(self, base_path):
        if os.path.exists(base_path + ".log.tmp"):
            if os.path.exists(base_path + ".conf.tmp"):
                os.remove(base_path + ".conf")
                os.rename(base_path + ".conf.tmp", base_path + ".conf")
            if os.path.exists(base_path + ".snapshot.tmp"):
                os.remove(base_path + ".snapshot")
                os.rename(base_path + ".snapshot.tmp", base_path + ".snapshot")
            os.remove(base_path + ".log")
            os.rename(base_path + ".log.tmp", base_path + ".log")
        else:
            if os.path.exists(base_path + ".conf.tmp"):
                os.remove(base_path + ".conf.tmp")
            if os.path.exists(base_path + ".snapshot.tmp"):
                os.remove(base_path + ".snapshot.tmp")

    def __init__(
        self,
        name,
        replicas,
        machine=NopVM(),
        base_path="/tmp/raft",
        housekeep=False,
    ) -> None:
        self.__fix_backups__(base_path)

        self.name = name
        self.replicas = replicas
        self.machine = machine
        self.voted_for = None
        self.housekeep = housekeep
        self.config = open(create_if_not_exists(base_path + ".conf"), "r+")
        self.config.seek(0, 0)
        line = self.config.readline()
        if line:
            conf = json.loads(line)
            self.commit_index = conf.get("commit_index", 0)
            self.last_applied = conf.get("last_applied", 0)
            self.snapshot_version = conf.get("snapshot_version", 0)
            self.voted_for = conf.get("voted_for", None)
            self.current_term = conf.get("current_term", 0)
        else:
            self.snapshot_version = 0
            self.commit_index = 0
            self.last_applied = 0

        self.snapshot_file_name = base_path + ".snapshot"
        self.machine.load_file(self, self.snapshot_file_name)

        self.log = open(create_if_not_exists(base_path + ".log"), "r+")
        self.log.seek(0, 0)
        self.entries = [{"term": 0, "data": None}]
        self.seek = [-1]
        while True:
            seek = self.log.tell()
            line = self.log.readline()
            if not line:
                break
            self.seek.append(seek)
            self.entries.append(json.loads(line))

        self.current_term = 0
        if len(self.entries) > 1:
            self.machine.run(self.entries[1 : self.commit_index + 1])

        self.lock = RLock()
        self.scheduler = Scheduler()
        self.housekeeper = Scheduler()
        self.as_follower()

    def append_entry(self, req):
        with self.lock:
            return self.state.append_entry(req)

    def append_entries(self, req):
        with self.lock:
            return self.state.append_entries(req)

    def request_vote(self, req):
        with self.lock:
            return self.state.request_vote(req)

    def __append_entry__(self, start, reqs):
        if start < len(self.entries):
            self.log.truncate(self.seek[start])
            self.entries = self.entries[:start]
            self.seek = self.seek[:start]
        self.entries += reqs
        for req in reqs:
            self.seek.append(self.log.tell())
            write_json_line(self.log, req)
        self.log.flush()

    def __vote__(self, voted_for, term):
        self.voted_for = voted_for
        self.current_term = term

    def save_config(self):
        self.config.seek(0, 0)
        write_json_line(
            self.config,
            {
                "commit_index": self.commit_index,
                "snapshot_version": self.snapshot_version,
                "last_index": self.last_applied,
                "voted_for": self.voted_for,
                "current_term": self.current_term,
            },
        )
        self.config.flush()

    def __append_entries__(self, req):
        logger.debug("append_entries: %s", req)
        snapshot = req.get("snapshot")
        if snapshot is not None and req["snapshot_version"] >= self.snapshot_version:
            if req["snapshot_version"] > self.snapshot_version:
                self.machine.reset(self, snapshot)
                self.save_snapshot(req["snapshot_version"], [])
            return {
                "term": self.current_term,
                "success": True,
                "snapshot_version": self.snapshot_version,
            }
        if req["snapshot_version"] != self.snapshot_version:
            return {
                "term": self.current_term,
                "success": False,
                "snapshot_version": self.snapshot_version,
            }
        if req["term"] < self.current_term:
            return {
                "term": self.current_term,
                "success": False,
                "snapshot_version": self.snapshot_version,
            }
        if req["prev_log_index"] >= len(self.entries):
            return {
                "term": self.current_term,
                "success": False,
                "snapshot_version": self.snapshot_version,
            }
        else:
            if self.entries[req["prev_log_index"]]["term"] != req["prev_log_term"]:
                return {
                    "term": self.current_term,
                    "success": False,
                    "snapshot_version": self.snapshot_version,
                }

        if len(req["entries"]) > 0:
            self.__append_entry__(req["prev_log_index"] + 1, req["entries"])

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
            "snapshot_version": self.snapshot_version,
        }

    def __request_vote__(self, req):
        logger.debug(f"request_vote: {req}")

        def upto_date(self, req):
            last_log_term = self.entries[-1]["term"]
            return (
                last_log_term < req["last_log_term"]
                or (
                    last_log_term == req["last_log_term"]
                    and len(self.entries) - 1 <= req["last_log_index"]
                )
            ) and self.snapshot_version <= req["snapshot_version"]

        if req["term"] > self.current_term:
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
                        "snapshot_version": self.snapshot_version,
                    }
        return {
            "term": self.current_term,
            "vote_granted": False,
            "snapshot_version": self.snapshot_version,
        }

    def __update_commit_index__(self, commited):
        prev_index = self.commit_index
        self.commit_index = commited
        self.machine.run(self.entries[prev_index + 1 : commited + 1])
        self.save_config()

    def __snapshot__(self):
        snapshot_version = self.snapshot_version + self.commit_index
        entries = self.entries[self.commit_index + 1 :]
        self.save_snapshot(snapshot_version, entries)

    def save_snapshot(self, snapshot_version, entries):
        logger.info("snapshot start")
        self.machine.save_snapshot(self.snapshot_file_name + ".tmp")
        logger.info("snaphot machine done")
        with open(self.config.name + ".tmp", "w+") as f:
            write_json_line(
                f,
                {
                    "commit_index": self.commit_index,
                    "snapshot_version": self.snapshot_version,
                    "last_index": self.last_applied,
                    "voted_for": self.voted_for,
                    "current_term": self.current_term,
                },
            )

        seek = []
        with open(self.log.name + ".tmp", "w+") as f:
            for e in entries:
                seek.append(f.tell())
                write_json_line(f, e)

        with self.lock:
            swap(
                self.snapshot_file_name,
                self.snapshot_file_name + ".tmp",
            )
            self.config = swap(self.config.name, self.config.name + ".tmp", "r+")
            self.log = swap(self.log.name, self.log.name + ".tmp", "r+")

            self.snapshot_version = snapshot_version
            self.seek = [-1] + seek
            self.entries = [{"term": self.current_term, "data": None}] + entries
            self.commit_index = 0
            self.last_applied = 0
        logger.info("Snapshot done")

    def as_candidate(self):
        logger.info(f"{self.name} as candidate")
        self.state = Candidate(self)

    def as_leader(self):
        logger.info(f"{self.name} as leader")
        self.state = Leader(self)

    def as_follower(self):
        logger.info(f"{self.name} as follower")
        self.state = Follower(self)

    def fetch(self, replica, service, data):
        try:
            response = requests.post(f"http://{replica}/{service}", json=data)
            if response.status_code == 200:
                return response.json()
            logger.info(f"{response.status_code}, {response.text}")
        except Exception as e:
            logger.error(f"Calling {replica}/{service} " + str(e))
        return None

    def schedule(self, delaymillis, func):
        def wrapper():
            with self.lock:
                func()

        self.scheduler.schedule(
            timedelta(milliseconds=delaymillis),
            wrapper,
        )

    def results(self, query):
        return self.state.results(query)

    def snapshot(self):
        return self.state.snapshot()


class RaftTest(Raft):
    def __init__(self, name, replicas) -> None:
        self.state = None
        super().__init__(name, replicas)

    def fetch(self, replica, service, data):
        print(data)
        return {
            "success": True,
            "snapshot_version": 0,
            "vote_granted": True,
            "term": 0,
        }

    def schedule(self, delaymillis, func):
        return func()


raft = RaftTest("1", ["1", "2", "3"])
