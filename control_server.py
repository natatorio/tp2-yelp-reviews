from typing import Dict, cast
from flask import Flask, make_response, jsonify
import requests
import logging
from kevasto import Client
from health_server import HealthServer
import pipe
from dedup import ControlDedup

logging.basicConfig()
logger = logging.getLogger("Control")
logger.setLevel(logging.INFO)


class ControlClient:
    def __init__(self):
        self.baseUrl = "http://tp3_control_1:80"

    def request(self, requestId):
        answer = False
        while not answer:
            try:
                res = requests.post(f"{self.baseUrl}/request/{requestId}")
                answer = True
            except:
                pass
        return res

    def batch_done(self, batchId, pid):
        answer = False
        while not answer:
            try:
                res = requests.post(f"{self.baseUrl}/batch/{batchId}/{pid}")
                answer = True
            except:
                pass
        return res


class ControlServer(HealthServer):
    def __init__(self):
        self.batchControlChannel = pipe.pub_sub_control()
        self.controlDedup = ControlDedup("control")
        self.app = Flask(__name__)
        log = logging.getLogger("werkzeug")
        log.setLevel(logging.ERROR)
        self.__route_control_endpoints()
        super().run_server()

        # Assumption: requestId is unique

    def __route_control_endpoints(self):
        @self.app.route("/request/<requestId>", methods=["POST"])
        def client_request_handler(requestId):
            logger.info(f"New client request with id={requestId}")
            if self.controlDedup.is_batch_processed(requestId):
                return make_response({"error": "duplicated request id"}, 500)
            if self.controlDedup.is_request_attended(requestId):
                return make_response({"ok": "request alredy attended"}, 200)
            if self.controlDedup.are_all_pids_done():
                self.batchControlChannel.send({"session_id": requestId})
                self.controlDedup.clear_pids_done()
                self.controlDedup.set_request_attended(requestId)
                self.controlDedup.persist_state()
                return make_response({"ok": "properly received request"}, 200)
            return make_response(
                {"error": "server unavailable to attend requests"}, 500
            )

        @self.app.route("/batch/<batchId>/<pid>", methods=["POST"])
        def batch_done_handler(batchId, pid):
            logger.info(f"new done batch id {batchId} signal from {pid}")
            if not self.controlDedup.is_batch_processed(
                batchId
            ) and self.controlDedup.is_request_attended(batchId):
                self.controlDedup.set_pid_done(pid)
                if self.controlDedup.are_all_pids_done():
                    self.controlDedup.set_processed_batch(batchId)
                    logger.info(f"batch {batchId} completed")
                self.controlDedup.persist_state()
            return make_response({}, 200)

    def stop(self):
        self.batchControlChannel.close()
        exit(0)


def main():
    try:
        controlServer = ControlServer()
    except Exception as e:
        logger.exception("Control server stopping.....")
        raise e
    finally:
        controlServer.stop()


if __name__ == "__main__":
    main()
