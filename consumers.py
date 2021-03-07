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
        self.consumer = Consumer(pipe_in=pipe_in)
        self.pipes_out = pipes_out

    def done(self, end_mark, acc):
        for pipe_out in self.pipes_out:
            pipe_out.send(
                {
                    **end_mark,
                    "data": acc,
                }
            )
            pipe_out.send(
                {
                    **end_mark,
                    "data": None,
                }
            )

    def close(self):
        self.consumer.close()
        for out in self.pipes_out:
            out.close()

    def run(self, aggregate):
        self.consumer.run(done=self.done, aggregate=aggregate)


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
        self.left_consumer = Consumer(left_in)
        self.right_consumer = Consumer(right_in)
        self.join_out = join_out
        self.barrier = Barrier(2)
        self.lock = RLock()
        self.right = None
        self.left = None

    def left_consume(self, aggregate, join):
        def done(context, acc):
            logger.info("waiting right")
            self.left = acc
            self.barrier.wait()
            self.join_out.send({**context, "data": join(self.left, self.right)})
            logger.info("done")

        def aggregate_wrapper(acc, left):
            acc = aggregate(acc, left, self.right)
            self.left = acc
            return acc

        self.left_consumer.run(aggregate_wrapper, done)

    def right_consume(self, aggregate, join):
        def done(context, acc):
            logger.info("waiting left")
            self.right = acc
            self.barrier.wait()

        def aggregate_wrapper(acc, right):
            acc = aggregate(acc, self.left, right)
            self.right = acc
            return acc

        self.right_consumer.run(aggregate_wrapper, done)

    def run(self, left, right, join):
        thread = Thread(target=lambda: self.left_consume(left, join))
        thread.start()
        self.right_consume(right, join)
        logger.info("waiting left to shutdown")
        thread.join()

    def close(self):
        self.left_consume.close()
        self.right_consumer.close()
        self.join_out.close()

    def join_function(self, dictA):
        return {k: v for (k, v) in dictA.items() if dictA[k] == self.data.get(k, 0)}
