from random import random
import time
from typing import Any, Dict, Union
import requests
import logging
from flask import request

from raft import Follower, Leader, NopVM, Raft
import logging

logging.basicConfig(level=logging.ERROR)
logger = logging.getLogger("kevasto")
logger.setLevel(logging.INFO)


class Log:
    def __init__(self, data):
        self.data = data
        if not data.get("entries"):
            self.data["entries"] = []
            self.data["base"] = 0

    def translate(self, i):
        virtual_size = self.data["base"] + len(self.data["entries"])
        if i > virtual_size:
            raise Exception(f"Illegal index {i} > {virtual_size}")
        index = i - self.data["base"]
        if index < 0:
            raise Exception(f"Illegal index {i} < {self.data['base']}")
        return index

    def list(self, start):
        return self.data["entries"][self.translate(start) :]

    def drop(self, i):
        if i is None:
            self.data["base"] = 0
            self.data["entries"] = []
        else:
            index = self.translate(i)
            self.data["base"] = i
            self.data["entries"] = self.data["entries"][index:]

    def concat(self, values):
        self.data["entries"].extend(values)


class KeyValueVM(NopVM):
    data = {"keyvalue": {}, "log": {}}

    def reset(self, context, snapshot):
        self.data = snapshot

    def snapshot(self):
        return self.data

    def run(self, commands):
        for command in commands:
            command = command["data"]
            bucket_key = command["bucket"]
            bucket = self.data[command["store"]].get(bucket_key)
            if bucket is None:
                bucket = {}
                self.data[command["store"]][bucket_key] = bucket

            op = command["op"]
            if command["store"] == "log":
                log = Log(bucket)
                if op == "drop":
                    log.drop(command["start"])
                elif op == "append":
                    log.concat(command["val"])
            else:
                if op == "+":
                    bucket[command["key"]] = command["val"]
                elif op == "-":
                    bucket.pop(command["key"], None)
        return None

    def results(self, query):
        bucket = self.data[query["store"]].get(query["bucket"])
        if bucket:
            if query["store"] == "log":
                return Log(bucket).list(query["start"])
            else:
                if query.get("key"):
                    return bucket.get(query["key"])
                elif query.get("keys"):
                    return [bucket.get(key) for key in query["keys"]]
        return None


def add_raft_routes(app, raft: Raft):
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
        leader = {}
        if isinstance(raft.state, Leader):
            leader = {
                "match_index": raft.state.match_index,
                "next_index": raft.state.next_index,
                "snapshot_index": raft.state.snapshot_index,
            }
        follower = {}
        if isinstance(raft.state, Follower):
            follower = {"election_timeout": raft.state.election_timeout}
        res = {
            "follower": follower,
            "entries": len(raft.entries),
            "voted_for": raft.voted_for,
            "current_term": raft.current_term,
            "commit_index": raft.commit_index,
            "replicas": raft.replicas,
            "name": raft.name,
            "snapshot_version": raft.snapshot_version,
            "state": raft.state.__class__.__name__,
            "index": leader,
            "log": raft.log.tell(),
            "machine": len(raft.machine.data),
        }
        return res

    def response(data):
        if data and data.get("success"):
            return (data, 200)
        else:
            return (data, 500)

    @app.route("/snapshot")
    def snapshot():
        return response(raft.snapshot())

    @app.route("/keyvalue/<bucket>/<key>", methods=["DELETE", "GET"])
    def get_key(bucket, key):
        if request.method == "DELETE":
            return response(
                raft.append_entry(
                    {
                        "op": "-",
                        "key": key,
                        "bucket": bucket,
                        "store": "keyvalue",
                    }
                )
            )
        else:
            return response(
                raft.results(
                    {
                        "key": key,
                        "bucket": bucket,
                        "store": "keyvalue",
                    }
                )
            )

    @app.route("/keyvalue/<bucket>/<key>", methods=["PUT"])
    def put_key(bucket, key):
        return response(
            raft.append_entry(
                {
                    "op": "+",
                    "bucket": bucket,
                    "key": key,
                    "val": request.get_json(),
                    "store": "keyvalue",
                }
            )
        )

    @app.route("/log/<bucket>/<start>", methods=["POST"])
    def log_post(bucket, start):
        return response(
            raft.append_entry(
                {
                    "op": "append",
                    "bucket": bucket,
                    "start": int(start),
                    "val": request.get_json(),
                    "store": "log",
                }
            )
        )

    @app.route("/log/<bucket>/<start>", methods=["DELETE", "GET"])
    def log_get(bucket, start):
        if request.method == "DELETE":
            return response(
                raft.append_entry(
                    {
                        "op": "drop",
                        "start": int(start) if start != "None" else None,
                        "bucket": bucket,
                        "store": "log",
                    }
                )
            )
        else:
            return response(
                raft.results(
                    {
                        "bucket": bucket,
                        "start": int(start),
                        "store": "log",
                    }
                )
            )

    @app.route("/health", methods=["GET"])
    def healthcheck():
        return ("", 204)

    return raft


def retry(times, func) -> Union[Dict, None]:
    i = 0
    ex = None
    res = None
    while i < times:
        try:
            done, res = func()
            if done:
                return res
        except Exception as e:
            logger.exception("Retry")
            ex = e

        i += 1
        secs = pow(2, i % 10) / 100 + random()
        logger.error(f"retry {i} in {secs} {str(ex)} {res} ")
        time.sleep(secs)
    if ex is not None:
        raise ex
    return None


class Client:
    def __init__(self, host="tp3_kevasto_1") -> None:
        self.host = host

    def delete(self, bucket, key):
        def __delete__(url):
            res = requests.delete(url)
            content = res.json()
            if res.status_code == 200:
                return (True, None)
            elif content.get("redirect"):
                self.host = content["redirect"]
            return (False, res.text)

        return retry(
            10, lambda: __delete__(f"http://{self.host}:80/keyvalue/{bucket}/{key}")
        )

    def get(self, bucket, key) -> Union[None, Any]:
        def __get__(url):
            res = requests.get(url)
            content = res.json()
            if res.status_code == 200:
                return (True, content["data"])
            elif content.get("redirect"):
                self.host = content["redirect"]
            return (False, res.text)

        return retry(
            10, lambda: __get__(f"http://{self.host}:80/keyvalue/{bucket}/{key}")
        )

    def put(self, bucket, key, data):
        def __put__(url, data):
            res = requests.put(url, json=data)
            content = res.json()
            if res.status_code == 200:
                return (True, None)
            elif content.get("redirect"):
                self.host = content["redirect"]
            return (False, res.text)

        return retry(
            10, lambda: __put__(f"http://{self.host}:80/keyvalue/{bucket}/{key}", data)
        )

    def log_append(self, bucket, start, data):
        def __post__(url, data):
            res = requests.post(url, json=data)
            content = res.json()
            if res.status_code == 200:
                return (True, None)
            if content.get("redirect"):
                self.host = content["redirect"]
            return (False, res.text)

        return retry(
            10, lambda: __post__(f"http://{self.host}:80/log/{bucket}/{start}", data)
        )

    def log_drop(self, bucket, start):
        def __delete__(url):
            res = requests.delete(url)
            content = res.json()
            if res.status_code == 200:
                return (True, None)
            elif content.get("redirect"):
                self.host = content["redirect"]
            return (False, res.text)

        return retry(
            10, lambda: __delete__(f"http://{self.host}:80/log/{bucket}/{start}")
        )

    def log_fetch(self, bucket, start):
        def __get__(url):
            res = requests.get(url)
            content = res.json()
            if res.status_code == 200:
                return (True, None)
            elif content.get("redirect"):
                self.host = content["redirect"]
            return (False, res.text)

        return retry(10, lambda: __get__(f"http://{self.host}:80/log/{bucket}/{start}"))
