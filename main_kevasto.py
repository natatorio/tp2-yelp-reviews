import os
import time
import logging
import docker
from flask import Flask

from kevasto import *
import bjoern

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


def get_replicas(client):
    replicas = []
    i = 0
    while len(replicas) < int(os.environ["N_REPLICAS"]):
        replicas = [
            c.name for c in client.containers.list() if os.environ["NAME"] in c.name
        ]
        time.sleep(0.5)
        i += 1
    return replicas


def main():
    client = docker.from_env()
    container = client.containers.get(os.environ["HOSTNAME"])
    name = container.name
    replicas = get_replicas(client)
    logger.info("%s %s", name, replicas)
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

    bjoern.run(app, "0.0.0.0", 80)


if __name__ == "__main__":
    main()
