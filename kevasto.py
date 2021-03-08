import os
from random import random
import time
from typing import Dict, Union
import requests
import logging
import docker
from flask import Flask, request

from raft import Leader, NopVM, Raft


class KeyValueVM(NopVM):
    data = {}

    def reset(self, context, snapshot):
        self.data = snapshot

    def snapshot(self):
        return self.data

    def run(self, commands):
        for command in commands:
            command = command["data"]
            bucket_key = command["bucket"]
            bucket = self.data.get(bucket_key)
            if bucket is None:
                bucket = {}
                self.data[bucket_key] = bucket

            op = command.get("op", "put")
            if op == "+":
                bucket[command["key"]] = command["val"]
            elif op == "-":
                bucket.pop(command["key"], None)
            elif op == "drop":
                bucket["log"] = bucket.get("log", [])[: command["start"]]
            elif op == "append":
                bucket["log"] = (
                    bucket.get("log", [])[: command["start"]] + command["val"]
                )

        return None

    def results(self, query):
        bucket = self.data.get(query["bucket"])
        if bucket:
            if query.get("log"):
                return bucket.get("log", [])[bucket["start"] :]
            if query.get("key"):
                return bucket.get(query["key"])
            elif query.get("keys"):
                return [bucket.get(key) for key in query["keys"]]
        return {}


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
        res = {
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
                    }
                )
            )
        else:
            return response(
                raft.results(
                    {
                        "key": key,
                        "bucket": bucket,
                    }
                )
            )

    @app.route("/bulk/<bucket>/<keys>", methods=["DELETE", "GET"])
    def bulk_delete(bucket, keys):
        if request.method == "DELETE":
            return response(
                raft.append_entry(
                    [
                        {
                            "op": "-",
                            "key": key,
                            "bucket": bucket,
                        }
                        for key in keys.split(",")
                    ]
                )
            )
        else:
            return response(
                raft.results(
                    {
                        "bucket": bucket,
                        "keys": keys.split(","),
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
                    "start": start,
                    "val": request.get_json(),
                }
            )
        )

    @app.route("/log/<bucket>/<start>", methods=["DELETE", "GET"])
    def log_get_delete(bucket, start):
        if request.method == "DELETE":
            return response(
                raft.append_entry(
                    [
                        {
                            "op": "drop",
                            "start": start,
                            "bucket": bucket,
                        }
                    ]
                )
            )
        else:
            return response(
                raft.results(
                    {
                        "bucket": bucket,
                        "log": start,
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
            logging.exception("Retry")
            ex = e

        i += 1
        secs = pow(2, i % 10) / 100 + random()
        print(f"retry {i} in {secs} {str(ex)} {res} ")
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

    def get(self, bucket, key) -> Union[Dict, None]:
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
            elif content.get("redirect"):
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


# def manual_test():
#     response = requests.post("http://localhost:8083/append_entry", json={"a": "a"})
#     for i in range(10, 15):
#         requests.put(f"http://localhost:8081/database/{i}", json={"index": i})
#     response = requests.get("http://localhost:8083/database/1")


def get_name(str):
    s = str.split("_")
    if len(s) == 3:
        return s[1] + "_" + s[2]
    else:
        return s[0]


def get_replicas():
    replicas = []
    i = 0
    while len(replicas) < int(os.environ["N_REPLICAS"]):
        replicas = [
            c.name for c in client.containers.list() if os.environ["NAME"] in c.name
        ]
        time.sleep(0.5)
        i += 1
    return replicas


if __name__ == "__main__":
    client = docker.from_env()
    container = client.containers.get(os.environ["HOSTNAME"])
    name = container.name
    replicas = get_replicas()
    print(name, replicas)
    logging.basicConfig(level=logging.INFO)
    logging.getLogger("werkzeug").setLevel(logging.ERROR)

    app = Flask(__name__)
    raft = Raft(
        name,
        replicas,
        KeyValueVM(),
        housekeep=True,
    )
    add_raft_routes(app, raft)
    app.run(host="0.0.0.0", port=80, threaded=True)
