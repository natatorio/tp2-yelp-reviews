import os
from random import random
import time
import requests
import logging
import docker
from flask import Flask, request

from raft import NopVM, Raft


class KeyValueVM(NopVM):
    data = {}

    def reset(self, context, snapshot):
        self.data = snapshot

    def snapshot(self):
        return self.data

    def run(self, commands):
        print(commands)
        for command in commands:
            command = command["data"]

            bucket_key = command["bucket"]
            bucket = self.data.get(bucket_key)
            if bucket is None:
                bucket = {}
                self.data[bucket_key] = bucket

            key = command["key"]
            op = command.get("op", "put")
            if op == "+":
                val = command["val"]
                bucket[key] = val
            elif op == "-":
                bucket.pop(key, None)

        return None

    def results(self, query):
        bucket = self.data.get(query["bucket"])
        if bucket:
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
        res = {
            "entries": raft.entries,
            "voted_for": raft.voted_for,
            "current_term": raft.current_term,
            "commit_index": raft.commit_index,
            "replicas": raft.replicas,
            "name": raft.name,
            "snapshot_version": raft.snapshot_version,
            "state": raft.state.__class__.__name__,
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

    @app.route("/db/<bucket>/<key>", methods=["DELETE", "GET"])
    def results(bucket, key):
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

    @app.route("/db/<bucket>/<key>", methods=["PUT"])
    def save(bucket, key):
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

    @app.route("/health", methods=["GET"])
    def healthcheck():
        return ("", 204)

    return raft


def retry(times, func):
    i = 0
    ex = None
    while i < times:
    # try:
        res = func()
        if res is not None:
            return res
    # except Exception as e:
        logging.exception("Retry")
        # ex = e

    time.sleep(random() / 2 + 0.5)
    i += 1
# if ex is not None:
#     raise ex
    return None


class Client:
    def __init__(self, host) -> None:
        self.retry_times = 50
        self.host = host

    def delete(self, bucket, key):
        def __delete__(url):
            res = requests.delete(url)
            content = res.json()
            if res.status_code == 200:
                return content["data"]
            elif content.get("redirect"):
                self.host = content["redirect"]
            return None

        return retry(10, lambda: __delete__(f"http://{self.host}:80/db/{bucket}/{key}"))

    def get(self, bucket, key):
        print("PRINTTTTTTTTTTTTTT")
        print("HOST:{}".format(self.host))
        def __get__(url):
            res = requests.get(url)
            content = res.json()
            print(content)
            if res.status_code == 200:
                return content["data"]
            elif content.get("redirect"):
                self.host = content["redirect"]
            return None

        return retry(10, lambda: __get__(f"http://{self.host}:80/db/{bucket}/{key}"))

    def put(self, bucket, key, data):
        def __put__(url, data):
            res = requests.put(url, json=data)
            content = res.json()
            if res.status_code == 200:
                return content["data"]
            elif content.get("redirect"):
                self.host = content["redirect"]
            return None
        return retry(10, lambda: __put__(f"http://{self.host}:80/db/{bucket}/{key}", data))


def manual_test():
    response = requests.post("http://localhost:8083/append_entry", json={"a": "a"})
    for i in range(10, 15):
        requests.put(f"http://localhost:8081/database/{i}", json={"index": i})
    response = requests.get("http://localhost:8083/database/1")


def get_name(str):
    s = str.split("_")
    if len(s) == 3:
        return s[1] + "_" + s[2]
    else:
        return s[0]


if __name__ == "__main__":
    client = docker.from_env()
    container = client.containers.get(os.environ["HOSTNAME"])
    replicas = []
    while len(replicas) < int(os.environ["N_REPLICAS"]):
        replicas = [
            c.name for c in client.containers.list() if os.environ["NAME"] in c.name
        ]
        time.sleep(0.5)
    name = container.name
    print(name, replicas)
    app = Flask(__name__)
    logging.basicConfig(level=logging.DEBUG)
    raft = Raft(
        name,
        replicas,
        KeyValueVM(),
        housekeep=True,
    )
    add_raft_routes(app, raft)
    app.run(host="0.0.0.0", port=80, threaded=True)
