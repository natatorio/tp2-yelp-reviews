import requests
import os
import time
from health_server import *

import logging

logging.basicConfig()
logger = logging.getLogger("WatchdogSideCar")
logger.setLevel(logging.INFO)


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
        "CONTROL",
    ]:
        nReplicas = int(os.environ.get("N_" + processKey, 1))
        processIp = os.environ["IP_" + processKey]
        for i in range(nReplicas):
            ip = os.environ["IP_PREFIX"] + "_" + processIp + "_" + str(i + 1)
            ips.append(ip)
    leaderServer = LeaderServer("watchdog")
    time.sleep(10)  # Le doy tiempo a los procesos para levantar el flask
    leaderServer.trigger_election()
    while True:
        time.sleep(20)
        leaderServer.wait_for_election_resolution()
        if leaderServer.i_am_leader():
            leaderServer.healthcheck_workers()
            # Start Leader stuff
            for ip in ips:
                try:
                    requests.get(f"http://{ip}:80/health")
                except:
                    revive(ip)
            # End Leader Stuff
        else:
            leaderServer.healthcheck_leader()


if __name__ == "__main__":
    main()
