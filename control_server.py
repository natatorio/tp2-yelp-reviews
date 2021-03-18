from typing import Dict, cast
from flask import Flask, make_response, jsonify
import threading
import requests
import os
import logging
import bjoern
import docker
import subprocess
from kevasto import Client
from health_server import HealthServer
from pipe import Pipe


logging.basicConfig()
logger = logging.getLogger("Control")
logger.setLevel(logging.INFO)


def serialize_set(set):
    return ','.join([str(i) for i in list(set)])

def deserialize_set(str):
    return set([i for i in str.split(',')])

class ControlServer(HealthServer):

    def __init__(self):
        self.batchControlChannel = Pipe(
            exchange="control",
            routing_key="control",
            queue="",
        )
        self.name = "control"
        self.db = Client()
        self.__retrive_initial_state()
        self.allPids = self.__get_all_pids()
        self.app = Flask(__name__)
        log = logging.getLogger("werkzeug")
        log.setLevel(logging.ERROR)
        self.__route_control_endpoints()
        super().run_server()

        # Assumption: requestId is unique
    def __route_control_endpoints(self):
        @self.app.route("/request/<requestId>", methods = ["POST"])
        def client_request_handler(requestId):
            logger.info(f"New client request with id={requestId}")
            logger.info(f"actual batch id {self.actualBatchId}")
            logger.info(f"donePids: {self.donePids}")
            if requestId in self.attendedRequests:
                return make_response({"ok":"ok"}, 200)
            if self.donePids == self.allPids and int(requestId) == self.actualBatchId:
                self.batchControlChannel.send({"batch_id": requestId})
                self.donePids = set()
                self.attendedRequests.add(requestId)
                self.__persist_state()
                return make_response({"ok":"ok"}, 200)
            return make_response({"error":"error"}, 500)

        @self.app.route("/batch/<batchId>/<pid>", methods = ["POST"])
        def batch_done_handler(batchId, pid):
            logger.info(f"actual batch id {self.actualBatchId}")
            logger.info(f"new done batch id {batchId} signal from {pid}")
            if int(batchId) == self.actualBatchId:
                self.donePids.add(pid)
                logger.info(f"donePids: {self.donePids}")
                if self.donePids == self.allPids:
                    self.actualBatchId += 1
                    logger.info(f"batch {batchId} completed next batch {self.actualBatchId}")
                self.__persist_state()
            return make_response(
                {}, 200
            )

    def __retrive_initial_state(self):
        self.donePids = self.__get_all_pids()
        self.attendedRequests = set()
        self.actualBatchId = 0
        state = cast(Dict, self.db.get(self.name, "state"))
        if state:
            self.donePids = deserialize_set(state.get("done_pids"))
            self.actualBatchId = state.get("actual_batch_id", 0)
            self.attendedRequests = deserialize_set(state.get("attended_requests"))
        else:
            self.__persist_state()

    def __persist_state(self):
        self.db.put(
            self.name,
            "state",
            {
                "done_pids" : serialize_set(self.donePids),
                "actual_batch_id" : self.actualBatchId,
                "attended_requests" : serialize_set(self.attendedRequests),
            },
        )

    def __get_all_pids(self) -> set:
        pids = set()
        for processKey in [
            "ROUTER",
            "STARS5",
            "COMMENT",
            "BUSSINESS",
            "USERS",
            "HISTOGRAM",
            "FUNNY",
            "STARS5_MAPPER",
            "COMMENT_MAPPER",
            "HISTOGRAM_MAPPER",
            "FUNNY_MAPPER",
        ]:
            nReplicas = int(os.environ.get("N_" + processKey, 1))
            processIp = os.environ["IP_" + processKey]
            for i in range(nReplicas):
                ip = os.environ["IP_PREFIX"] + "_" + processIp + "_" + str(i + 1)
                pids.add(ip)
        return pids

    def stop(self):
        self.batchControlChannel.close()
        exit(0)

def main():

    # This goes in another thread or node
    # def build_summary(summary, data):
    #     key, value = data
    #     summary[key] = value
    #     return summary
    #
    # summaryMaker = Filter(pipe.reports())
    # reducer = Persistent(
    #     cursor=Reducer(
    #         step_fn=build_summary,
    #         pipe_out=Formatted(
    #             pipe.reports(),
    #             lambda summary: ("summary", summary),
    #         ),
    #     ),
    #     name="summary",
    #     client=Client(),
    # )
    # summaryMaker.run(reducer)
    #################

    try:
        controlServer = ControlServer()
    except Exception as e:
        logger.exception("")
        raise e
    finally:
        controlServer.stop()


if __name__ == "__main__":
    main()
