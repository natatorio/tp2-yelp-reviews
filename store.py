import json
from threading import Lock
import requests
from datetime import datetime


class KeyValueLocal:
    def __init__(
        self,
        log_file_path,
        snapshot_file_path,
        max_snapshot_size,
    ) -> None:
        self.log_file_path = log_file_path
        self.snapshot_file_path = snapshot_file_path
        self.lock = Lock()
        self.data = {}
        self.timestamp = 0
        self.prev = -1
        self.max_snapshot_size = max_snapshot_size

        def recover(self):
            try:
                self.snapshot_file = open(self.snapshot_file_path, "a+")
                self.snapshot_file.seek(0)
                snapshot = self.snapshot_file.readline()
                if snapshot:
                    snapshot_data = json.loads(snapshot)
                    self.timestamp = snapshot_data["timestamp"]
                    self.data = snapshot_data["data"]

                self.log_file = open(self.log_file_path, "a+")
                self.log_file.seek(0)
                line = self.log_file.readline()
                while line:
                    self.prev = self.log_file.tell()
                    event = json.loads(line)
                    self.data[event["id"]] = event["val"]
                    self.timestamp += 1
                    line = self.log_file.readline()
            except Exception as e:
                if self.snapshot_file:
                    self.snapshot_file.close()
                if self.log_file:
                    self.log_file.close()
                raise e

        recover(self)

    def get(self, id):
        return self.data[id]

    def put(self, idVal):
        with self.lock:
            idVal["timestamp"] = self.timestamp
            idVal["prev"] = self.prev
            self.prev = self.log_file.tell()
            self.log_file.write(json.dumps(idVal) + "\n")
            self.data[idVal["id"]] = idVal["val"]

            if self.log_file.tell() > self.max_snapshot_size:
                self.snapshot_file.seek(0)
                self.prev = -1
                self.snapshot_file.truncate(0)
                self.snapshot_file.write(
                    json.dumps(
                        {
                            "data": self.data,
                            "timestamp": self.timestamp,
                        }
                    )
                    + "\n"
                )
                self.snapshot_file.flush()
                self.log_file.seek(0)
                self.log_file.truncate(0)

            self.timestamp += 1
            self.log_file.flush()
        return True

    def reset(self, snapshot):
        with self.lock:
            self.data = snapshot["data"]
            self.timestamp = snapshot["timestamp"]
            self.log_file.truncate(0)
            self.prev = -1
            self.snapshot_file.truncate(0)
            self.snapshot_file.write(json.dumps(snapshot) + "\n")
            self.snapshot_file.flush()

    def close(self):
        self.log_file.close()
        self.snapshot_file.close()


# fstore = FileStore("/tmp/log", "/tmp/snap", 1024)
# for i in range(500):
# fstore.put({"id": 1000 + i, "val": 1000 + i})
# fstore.put({"id": 201, "val": 201})
# fstore.reset({"snapshot":{1:1, 2:2}, "timestamp": 2})
# fstore.close()


class AppendLogLocal:
    def __init__(self, log_file_path) -> None:
        self.log_file_path = log_file_path
