import os

import logging
from threading import Barrier
from typing import List
from pipe import Pipe, Send

logger = logging.getLogger("filter")
logger.setLevel(logging.INFO)


class Cursor:
    def start(self, caller) -> object:
        return None

    def restart(self) -> object:
        return None

    def step(self, acc, data, context) -> object:
        return None

    def end(self, acc, context):
        return

    def break_end(self, acc, context) -> bool:
        return False


class Filter:
    def __init__(self, pipe_in: Pipe):
        self.pipe_in = pipe_in

    def run(self, cursor: Cursor):
        logger.info("start consuming %s", self.pipe_in)
        acc = cursor.start(self)
        try:
            for payload, ack in self.pipe_in.recv():
                data = payload.pop("data", None)
                if data:
                    acc = cursor.step(acc, data, payload)
                else:
                    cursor.end(acc, payload)
                    if cursor.break_end(acc, payload):
                        break
                    acc = cursor.restart()
                ack()
        finally:
            self.pipe_in.cancel()
        logger.info("done consuming %s", self.pipe_in)

    def close(self):
        self.pipe_in.close()


def send_to_all(pipes_out: List[Send], data):
    for pipe_out in pipes_out:
        pipe_out.send(data)


class EndOnce(Cursor):
    replicas = int(os.environ.get("N_REPLICAS", 1))

    def start(self, caller) -> object:
        self.pipe_in = caller.pipe_in
        return self.start_once()

    def end(self, acc, context):
        count_down = context.pop("count_down", self.replicas)
        if count_down > 1:
            logger.info("count_down %s", count_down)
            self.pipe_in.send(
                {
                    **context,
                    "data": None,
                    "count_down": count_down - 1,
                }
            )
        else:
            logger.info("batch done: %s %s", self.pipe_in, context["session_id"])
            self.end_once(acc, context)

    def start_once(self):
        return None

    def end_once(self, acc, context):
        return


class MapperScatter(EndOnce):
    def __init__(self, map_fn, start_fn, pipes_out: List[Send]) -> None:
        self.pipes_out = pipes_out
        self.map_fn = map_fn
        self.start_fn = start_fn

    def start_once(self) -> object:
        return self.start_fn()

    def restart(self) -> object:
        return self.start_fn()

    def step(self, acc, data, context) -> object:
        send_to_all(self.pipes_out, {**context, "data": self.map_fn(data)})
        return None

    def end_once(self, acc, context) -> bool:
        send_to_all(self.pipes_out, {**context, "data": None})
        return False


class ReducerScatter(EndOnce):
    def __init__(self, step_fn, pipes_out: List[Send]) -> None:
        self.pipes_out = pipes_out
        self.step_fn = step_fn

    def start_once(self) -> object:
        return {}

    def restart(self) -> object:
        return {}

    def step(self, acc, data, context) -> object:
        return self.step_fn(acc, data)

    def end_once(self, acc, context) -> bool:
        send_to_all(self.pipes_out, {**context, "data": acc})
        send_to_all(self.pipes_out, {**context, "data": None})
        return False


class Join:
    def __init__(self, left_fn, right_fn, join_fn, pipes_out: List[Send]) -> None:
        self.barrier = Barrier(2)
        self.left_fn = left_fn
        self.right_fn = right_fn
        self.join_fn = join_fn
        self.pipes_out = pipes_out

    class Left(EndOnce):
        def __init__(self, parent) -> None:
            self.parent = parent

        def start_once(self) -> object:
            self.parent.left_data = {}
            return self.parent.left_data

        def restart(self) -> object:
            self.parent.left_data = {}
            return self.parent.left_data

        def step(self, acc, left_data, context) -> object:
            self.parent.left_data = self.parent.left_fn(
                acc,
                left_data,
                self.parent.right_data,
            )
            return self.parent.left_data

        def end_once(self, left_data, context):
            self.parent.barrier.wait()
            acc = self.parent.join_fn(self.parent.left_data, self.parent.right_data)
            send_to_all(self.parent.pipes_out, {**context, "data": acc})
            send_to_all(self.parent.pipes_out, {**context, "data": None})

    class Right(EndOnce):
        def __init__(self, parent) -> None:
            self.parent = parent

        def start_once(self) -> object:
            self.parent.right_data = {}
            return self.parent.right_data

        def restart(self) -> object:
            self.parent.right_data = {}
            return self.parent.right_data

        def step(self, acc, right_data, context) -> object:
            self.parent.right_data = self.parent.right_fn(
                acc,
                self.parent.right_data,
                right_data,
            )
            return self.parent.right_data

        def end_once(self, right_data, context):
            self.parent.barrier.wait()

    def left(self, step):
        return Join.Left(self)

    def right(self, step):
        return Join.Right(self)


class Notify(Cursor):
    def __init__(self, observer) -> None:
        self.observer = observer

    def start(self, caller) -> object:
        return None

    def restart(self) -> object:
        return None

    def step(self, acc, data, context) -> object:
        return data

    def end(self, acc, context) -> bool:
        self.observer(acc)
        return False
