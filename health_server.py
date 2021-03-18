import threading
from flask import Flask, make_response, jsonify
import requests
import os
import logging
import bjoern
import docker
import subprocess

logging.basicConfig()
logger = logging.getLogger("WatchdogSideCar")
logger.setLevel(logging.INFO)


def get_my_ip():
    client = docker.from_env()
    container = client.containers.get(os.environ["HOSTNAME"])
    return container.name


def revive(ip):
    logger.info("Process %s has died. Starting it up again...", ip)
    result = subprocess.run(
        ["docker", "start", ip],
        check=False,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    logger.info(
        "Process %s is up again. Result=%s. Output=%s. Error=%s",
        ip,
        result.returncode,
        result.stdout,
        result.stderr,
    )


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

    def __enter__(self):
        return self

    def __exit__(self, ex_type, ex, trace):
        self.stop()


class LeaderServer(HealthServer):
    def __init__(self, poolId):
        self.leaderIp = [None]
        self.resolved = threading.Event()
        self.requested = threading.Event()
        self.poolId = poolId
        self.myIp = get_my_ip()
        self.app = Flask(__name__)
        log = logging.getLogger("werkzeug")
        log.setLevel(logging.INFO)
        self.__run_thread()
        self.t.setDaemon(True)
        self.t.start()

    def __run_thread(self):
        self.t = threading.Thread(
            target=self.run_server,
            args=(
                self.leaderIp,
                self.resolved,
                self.requested,
            ),
        )

    def run_server(self, leaderIp, resolved, requested):
        self.leaderIp = leaderIp
        self.resolved = resolved
        self.requested = requested
        self.__route_leader_endpoints()
        super().run_server()

    def __route_leader_endpoints(self):
        @self.app.route("/election", methods=["POST"])
        def election_msg():
            self.requested.set()
            return make_response({}, 200)

        @self.app.route("/cordinator/<leader_ip>", methods=["POST"])
        def set_leader(leader_ip):
            self.leaderIp.pop(0)
            self.leaderIp.append(leader_ip)
            self.resolved.set()
            logger.info(f"Now {self.get_leader_ip()} is the leader")
            return make_response({}, 200)

    def wait_for_election_resolution(self):
        if self.requested.isSet():
            self.trigger_election()
        self.resolved.wait()

    def trigger_election(self):
        self.requested.clear()
        self.resolved.clear()
        anyOkResponse = False
        logger.info(f"New election for candidates: {self.get_candidates_ips()}")
        for ip in self.get_candidates_ips():
            try:
                response = requests.post(f"http://{ip}:80/election")
                if response.ok:
                    anyOkResponse = True
            except:
                logger.exception("Process %s did not answered election msg", ip)
        if not anyOkResponse:
            logger.info(
                f"Nobody answered my election msg, I should become leader",
            )
            self.proclam_myself_leader()

    def healthcheck_workers(self):
        logger.info("[LEADER] Checking processes health...")
        for ip in self.get_worker_ips():
            try:
                requests.get(f"http://{ip}:80/health")
            except:
                revive(ip)

    def healthcheck_leader(self):
        logger.info("[WORKER] Checking leader health...")
        try:
            requests.get(f"http://{self.get_leader_ip()}:80/health")
            # logger.info('http://' + leaderIp + ':' + port + '/health')
        except:
            logger.info("Leader has died. Running leader election")
            self.trigger_election()

    def proclam_myself_leader(self):
        for ip in self.__get_replicas_ips():
            try:
                requests.post(f"http://{ip}:80/cordinator/{self.myIp}")
            except:
                logger.exception("Process %s did not answered cordinator msg", ip)

    def i_am_leader(self):
        return self.leaderIp[0] == self.myIp

    def get_leader_ip(self):
        return self.leaderIp[0]

    def get_worker_ips(self):
        return [ip for ip in self.__get_replicas_ips() if ip != self.get_leader_ip()]

    def get_candidates_ips(self):
        return [ip for ip in self.__get_replicas_ips() if ip > self.myIp]

    def __get_replicas_ips(self):
        ips = []
        for i in range(int(os.environ["N_REPLICAS"])):
            ips.append(os.environ["IP_PREFIX"] + "_" + self.poolId + "_" + str(i + 1))
        return ips
