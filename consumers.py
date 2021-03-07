from filters import Filter, Join, MapperScatter, ReducerScatter
import os
from threading import RLock, Thread, Barrier
from typing import List

import logging
from pipe import Pipe, Send

logger = logging.getLogger("consumers")
logger.setLevel(logging.INFO)


class Consumer:
    def __init__(self, pipe_in: Pipe):
        self.pipe_in = pipe_in
        self.replicas = int(os.environ["N_REPLICAS"])

    def run(self, aggregate, done):
        logger.info("start consuming %s", self.pipe_in)
        acc = {}
        try:
            for payload, ack in self.pipe_in.recv():
                ack()
                data = payload["data"]
                if data:
                    acc = aggregate(acc, data)
                else:
                    count_down = payload.pop("count_down", self.replicas)
                    if count_down > 1:
                        logger.info("count_down", count_down)
                        self.pipe_in.send(
                            {
                                **payload,
                                "data": None,
                                "count_down": count_down - 1,
                            }
                        )
                    else:
                        done(payload, acc)
                        logger.info(
                            "batch done: %s %s", self.pipe_in, payload["session_id"]
                        )
                    acc = {}
        finally:
            self.pipe_in.cancel()
        logger.info("done consuming %s", self.pipe_in)

    def close(self):
        self.pipe_in.close()


class Scatter:
    def __init__(self, pipe_in: Pipe, pipes_out: List[Send]):
        self.consumer = Filter(pipe_in=pipe_in)
        self.pipes_out = pipes_out

    def close(self):
        self.consumer.close()
        for out in self.pipes_out:
            out.close()

    def run(self, aggregate):
        self.consumer.run(
            ReducerScatter(
                step_fn=aggregate,
                pipes_out=self.pipes_out,
            )
        )


class CounterBy(Scatter):
    def __init__(self, pipe_in, pipes_out, key_id):
        super().__init__(pipe_in, pipes_out)
        self.key_id = key_id

    def run(self):
        super().run(self.aggregate)

    def aggregate(self, acc, data):
        for elem in data:
            acc[elem[self.key_id]] = acc.get(elem[self.key_id], 0) + 1
        return acc


class Joiner:
    def __init__(self, left_in, right_in, join_out) -> None:
        self.left_consumer = Filter(left_in)
        self.right_consumer = Filter(right_in)
        self.join_out = join_out

    def run(self, left, right, join):
        j = Join(
            left_fn=left,
            right_fn=right,
            join_fn=join,
            pipes_out=[self.join_out],
        )
        thread = Thread(target=lambda: self.left_consumer.run(cursor=j.left()))
        thread.start()
        self.right_consumer.run(j.right())
        logger.info("waiting left to shutdown")
        thread.join()

    def close(self):
        self.join_out.close()
