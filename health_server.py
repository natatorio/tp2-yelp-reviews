import threading
from flask import Flask, make_response, jsonify
import os
import logging
import bjoern


class HealthServer:
    def __init__(self):
        self.app = Flask(__name__)
        log = logging.getLogger("werkzeug")
        log.setLevel(logging.ERROR)
        self.__run_thread()
        self.t.setDaemon(True)
        self.t.start()

    def __run_thread(self):
        self.t = threading.Thread(target=self.run_server)

    def run_server(self):
        self.__route_health_endpoint()
        bjoern.run(self.app, "0.0.0.0", 80)

    def __route_health_endpoint(self):
        @self.app.route("/health", methods=["GET"])
        def healthcheck():
            return ("", 204)

    def stop(self):
        exit(0)


class LeaderServer(HealthServer):
    def __init__(self, iAmLeader):
        self.app = Flask(__name__)
        log = logging.getLogger("werkzeug")
        log.setLevel(logging.ERROR)
        self.__run_thread(iAmLeader)
        self.t.setDaemon(True)
        self.t.start()

    def __run_thread(self, iAmLeader):
        self.t = threading.Thread(target=self.run_server, args=(iAmLeader,))

    def run_server(self, iAmLeader):
        self.iAmLeader = iAmLeader
        self.__route_leader_endpoints()
        super().run_server()

    def __route_leader_endpoints(self):
        @self.app.route("/id")
        def get_id():
            return make_response(
                jsonify(id=os.environ["HOSTNAME"], leader=self.iAmLeader[0]), 200
            )
