import datetime
import hashlib
import os
from pipe import Pipe
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("Mapper")
logger.setLevel(logging.INFO)


class Mapper:
    def __init__(self, pipe_in: Pipe, pipe_out: Pipe):
        self.pipe_in = pipe_in
        self.pipe_out = pipe_out
        self.replicas = int(os.environ.get("N_REPLICAS", 1))

    def prepare(self):
        return

    def map(self, data):
        return None

    def run(self):
        logger.info("start mapping: %s -> %s", self.pipe_in, self.pipe_out)
        try:
            self.prepare()
            for payload, ack in self.pipe_in.recv():
                data = payload["data"]
                if data:
                    mapped_data = self.map(data)
                    self.pipe_out.send({**payload, "data": mapped_data})
                else:
                    count_down = payload.pop("count_down", self.replicas)
                    if count_down <= 1:
                        logger.info(
                            "count_down done: %s -> %s",
                            self.pipe_in,
                            self.pipe_out,
                        )
                        self.pipe_in.send({**payload, "data": None})
                    else:
                        logger.info(
                            "count_down %s: %s -> %s",
                            count_down,
                            self.pipe_in,
                            self.pipe_out,
                        )
                        self.pipe_out.send(
                            {
                                **payload,
                                "data": None,
                                "count_down": count_down - 1,
                            }
                        )
                        self.prepare()
                ack()
        finally:
            self.pipe_in.cancel()
        logger.info("done mapping: %s -> %s", self.pipe_in, self.pipe_out)


class Pop:
    def __init__(self, pipe_in) -> None:
        self.pipe_in = pipe_in
        self.replicas = int(os.environ.get("N_REPLICAS", 1))

    def pop(self):
        logger.info("start waiting item: %s", self.pipe_in)
        data_payload = None
        try:
            for payload, ack in self.pipe_in.recv():
                if payload["data"] is None:
                    print("empty")
                    ack()
                    continue
                else:
                    data_payload = payload
                    count_down = payload.pop("count_down", self.replicas)
                    if count_down > 1:
                        print("count_down:", count_down)
                        self.pipe_in.send(
                            {
                                **payload,
                                "count_down": count_down - 1,
                            }
                        )
                    ack()
                    break
        finally:
            self.pipe_in.cancel()
        print(len(data_payload["data"]))
        logger.info("end waiting item: %s", self.pipe_in)
        return data_payload["data"]


class FunnyMapper(Mapper):
    def __init__(self, pipe_in: Pipe, pipe_out: Pipe, business_cities: Pop):
        super().__init__(pipe_in, pipe_out)
        self.business_cities = business_cities

    def prepare(self):
        self.business_city = self.business_cities.pop()

    def map(self, reviews):
        return [
            {"city": self.business_city.get(r["business_id"], "Unknown")}
            for r in reviews
            if r["funny"] != 0
        ]


class CommentMapper(Mapper):
    def map(self, reviews):
        return [
            {
                "text": hashlib.sha1(r["text"].encode()).hexdigest(),
                "user_id": r["user_id"],
            }
            for r in reviews
        ]


class Stars5Mapper(Mapper):
    def map(self, reviews):
        return [
            {"stars": r["stars"], "user_id": r["user_id"]}
            for r in reviews
            if r["stars"] == 5.0
        ]


class HistogramMapper(Mapper):
    def map(self, dates):
        return [
            {
                "weekday": datetime.datetime.strptime(
                    d["date"], "%Y-%m-%d %H:%M:%S"
                ).strftime("%A")
            }
            for d in dates
        ]
