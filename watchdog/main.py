import requests
import os
import time
import subprocess
from health_server import *

import logging

logger = logging.getLogger("WatchdogSideCar")
logger.setLevel(logging.ERROR)


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


def run_election():
    candidates = {}
    for ip in get_watchdogs_ips():
        try:
            response = requests.get(f"http://{ip}:80/id").json()
            id = response.get("id")
            logger.info(
                "Process %s says its id is %s and its %s that it is the leader",
                ip,
                id,
                str(response.get("leader")),
            )
            if response.get("leader"):
                return ip, id
            candidates[ip] = id
        except:
            logger.exception("Process %s did not answered", ip)
    leaderIp = max(candidates, key=candidates.get)
    leaderId = candidates[leaderIp]
    logger.info("Now %s is leader", leaderIp)
    return leaderIp, leaderId


def i_am_leader(leaderId):
    return leaderId == os.environ["HOSTNAME"]


def get_watchdogs_ips():
    ips = []
    for i in range(int(os.environ["N_REPLICAS"])):
        ips.append(os.environ["IP_PREFIX"] + "_watchdog_" + str(i + 1))
    # logger.info('WATCHDOG IPS: ', ips)
    return ips


def main():
    ips = []
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
        "KEVASTO",
    ]:
        nReplicas = int(os.environ.get("N_" + processKey, 1))
        processIp = os.environ["IP_" + processKey]
        for i in range(nReplicas):
            ip = os.environ["IP_PREFIX"] + "_" + processIp + "_" + str(i + 1)
            ips.append(ip)
    iAmLeader = [False]
    LeaderServer(iAmLeader)
    time.sleep(10)  # Le doy tiempo a los procesos para levantar el flask
    leaderIp, leaderId = run_election()
    workersIps = [ip for ip in get_watchdogs_ips() if ip != leaderIp]
    while True:
        time.sleep(3)
        iAmLeader[0] = i_am_leader(leaderId)
        if iAmLeader[0]:
            logger.info("[LEADER] Checking processes health...")
            for ip in ips + workersIps:
                try:
                    requests.get(f"http://{ip}:80/health")
                except:
                    revive(ip)
        else:
            logger.info("[WORKER] Checking leader health...")
            try:
                requests.get(f"http://{leaderIp}:80/health")
                # logger.info('http://' + leaderIp + ':' + port + '/health')
            except:
                logger.info("Leader has died. Running leader election")
                leaderIp, leaderId = run_election()
                workersIps = [ip for ip in get_watchdogs_ips() if ip != leaderIp]


if __name__ == "__main__":
    main()
