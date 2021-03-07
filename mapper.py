import datetime
import hashlib
import os
from typing import List
from pipe import Pipe, Send
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("Mapper")
logger.setLevel(logging.INFO)


class Mapper:
    def __init__(self, pipe_in: Pipe, pipes_out: List[Send]):
        self.pipe_in = pipe_in
        self.pipes_out = pipes_out
        self.replicas = int(os.environ.get("N_REPLICAS", 1))

    def run(self, map_fn, prepare=lambda: None):
        logger.info("start mapping: %s -> %s", self.pipe_in, self.pipes_out)
        try:
            prepare()
            for payload, ack in self.pipe_in.recv():
                data = payload["data"]
                if data:
                    mapped_data = map_fn(data)
                    for pipe_out in self.pipes_out:
                        pipe_out.send({**payload, "data": mapped_data})
                else:
                    count_down = payload.pop("count_down", self.replicas)
                    if count_down <= 1:
                        logger.info(
                            "count_down done: %s -> %s",
                            self.pipe_in,
                            self.pipes_out,
                        )
                        self.pipe_in.send({**payload, "data": None})
                    else:
                        logger.info(
                            "count_down %s: %s -> %s",
                            count_down,
                            self.pipe_in,
                            self.pipes_out,
                        )
                        for pipe_out in self.pipes_out:
                            pipe_out.send(
                                {
                                    **payload,
                                    "data": None,
                                    "count_down": count_down - 1,
                                }
                            )
                        prepare()
                ack()
        finally:
            self.pipe_in.cancel()
        logger.info("done mapping: %s -> %s", self.pipe_in, self.pipes_out)


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
                    ack()
                    continue
                else:
                    data_payload = payload
                    count_down = payload.pop("count_down", self.replicas)
                    if count_down > 1:
                        logger.info("count_down: %s", count_down)
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
        logger.info("end waiting item: %s", self.pipe_in)
        return data_payload["data"]


class FunnyMapper(Mapper):
    def __init__(self, pipe_in: Pipe, pipe_out: Pipe, business_cities: Pop):
        super().__init__(pipe_in, [pipe_out])
        self.business_cities = business_cities

    def run(self):
        super().run(self.map, self.prepare)

    def prepare(self):
        self.business_city = self.business_cities.pop()

    def map(self, reviews):
        return [
            {"city": self.business_city.get(r["business_id"], "Unknown")}
            for r in reviews
            if r["funny"] != 0
        ]


class CommentMapper(Mapper):
    def run(self):
        super().run(self.map)

    def map(self, reviews):
        return [
            {
                "text": hashlib.sha1(r["text"].encode()).hexdigest(),
                "user_id": r["user_id"],
            }
            for r in reviews
        ]


class Stars5Mapper(Mapper):
    def run(self):
        super().run(self.map, lambda: None)

    def map(self, reviews):
        return [
            {"stars": r["stars"], "user_id": r["user_id"]}
            for r in reviews
            if r["stars"] == 5.0
        ]


class HistogramMapper(Mapper):
    def run(self):
        super().run(self.map, lambda: None)

    def map(self, dates):
        return [
            {
                "weekday": datetime.datetime.strptime(
                    d["date"], "%Y-%m-%d %H:%M:%S"
                ).strftime("%A")
            }
            for d in dates
        ]
