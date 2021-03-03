import os
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
            key = command["key"]
            val = command.get("val")
            op = command.get("op", "put")
            if op == "+":
                self.data[key] = val
            elif op == "-":
                self.data.pop(key, None)

        return None

    def results(self, query):
        return self.data.get(query)


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

    @app.route("/snapshot")
    def snapshot():
        raft.snapshot()
        return {}

    @app.route("/database/<key>", methods=["DELETE", "GET"])
    def results(key):
        if request.method == "DELETE":
            return raft.append_entry(
                {
                    "op": "-",
                    "key": key,
                }
            )
        else:
            return raft.results(key)

    @app.route("/database/<key>", methods=["PUT"])
    def save(key):
        return raft.append_entry(
            {
                "op": "+",
                "key": key,
                "val": request.get_json(),
            }
        )

    return raft


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
