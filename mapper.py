import datetime
import hashlib
from threading import Event, Thread
from typing import List
from pipe import Pipe, Send
from filters import Filter, MapperScatter, Notify
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("Mapper")
logger.setLevel(logging.INFO)


class Mapper:
    def __init__(self, pipe_in: Pipe, pipes_out: List[Send]):
        self.refiner = Filter(pipe_in)
        self.pipes_out = pipes_out

    def run(self, map_fn, prepare=lambda: None):
        self.refiner.run(
            cursor=MapperScatter(
                map_fn=map_fn,
                start_fn=prepare,
                pipes_out=self.pipes_out,
            )
        )


class FunnyMapper:
    def __init__(self, pipe_in: Pipe, pipe_out: Send, business_cities: Pipe):
        self.refiner = Filter(pipe_in)
        self.business_cities = Filter(business_cities)
        self.pipes_out = [pipe_out]
        self.business_done = Event()
        self.business_taken = Event()

    def on_business_done(self, data):
        self.business_city = data
        self.business_done.set()
        self.business_taken.wait()
        self.business_taken.clear()

    def consume_business(self):
        self.business_cities.run(Notify(observer=self.on_business_done))

    def run(self):
        thread = Thread(target=self.consume_business)
        thread.start()
        self.refiner.run(
            cursor=MapperScatter(
                map_fn=self.map,
                start_fn=self.prepare,
                pipes_out=self.pipes_out,
            )
        )
        thread.join()

    def prepare(self):
        self.business_done.wait()
        self.business_done.clear()
        self.business_taken.set()

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
