import os

import logging
from threading import Barrier, Event
from typing import List
from pipe import Pipe, Send

logger = logging.getLogger("filter")
logger.setLevel(logging.INFO)


class Cursor:
    def setup(self, caller):
        return

    def start(self) -> object:
        return {}

    def step(self, acc, data, context) -> object:
        return None

    def end(self, acc, context):
        pass

    def quit(self, acc, context) -> bool:
        return False


class Filter:
    def __init__(self, pipe_in: Pipe):
        self.pipe_in = pipe_in

    def run(self, cursor: Cursor):
        logger.info("start consuming %s", self.pipe_in)
        cursor.setup(self)
        acc = cursor.start()
        try:
            for payload, ack in self.pipe_in.recv():
                data = payload.pop("data", None)
                if data:
                    acc = cursor.step(acc, data, payload)
                else:
                    cursor.end(acc, payload)
                    if cursor.quit(acc, payload):
                        break
                    acc = cursor.start()
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

    def setup(self, caller) -> object:
        self.pipe_in = caller.pipe_in

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

    def end_once(self, acc, context):
        return


class MapperScatter(EndOnce):
    def __init__(self, map_fn, start_fn, pipes_out: List[Send]) -> None:
        self.pipes_out = pipes_out
        self.map_fn = map_fn
        self.start_fn = start_fn

    def start(self) -> object:
        return self.start_fn()

    def step(self, acc, data, context) -> object:
        send_to_all(self.pipes_out, {**context, "data": self.map_fn(data)})
        return None

    def end_once(self, acc, context):
        send_to_all(self.pipes_out, {**context, "data": None})


class ReducerScatter(EndOnce):
    def __init__(self, step_fn, pipes_out: List[Send]) -> None:
        self.pipes_out = pipes_out
        self.step_fn = step_fn

    def step(self, acc, data, context) -> object:
        return self.step_fn(acc, data)

    def end_once(self, acc, context):
        send_to_all(self.pipes_out, {**context, "data": acc})
        send_to_all(self.pipes_out, {**context, "data": None})


class Join:
    def __init__(self, left_fn, right_fn, join_fn, pipes_out: List[Send]) -> None:
        self.barrier = Barrier(2)
        self.left_fn = left_fn
        self.right_fn = right_fn
        self.join_fn = join_fn
        self.pipes_out = pipes_out
        self.send_done = Event()

    class Left(EndOnce):
        def __init__(self, parent) -> None:
            self.parent = parent

        def start(self) -> object:
            self.parent.left_acc = {}
            return self.parent.left_acc

        def step(self, acc, left_data, context) -> object:
            self.parent.left_acc = self.parent.left_fn(
                acc,
                left_data,
                self.parent.right_acc,
            )
            return self.parent.left_acc

        def end_once(self, left_data, context):
            print("left", len(self.parent.left_acc), len(self.parent.right_acc))
            self.parent.barrier.wait()
            acc = self.parent.join_fn(self.parent.left_acc, self.parent.right_acc)
            self.parent.send_done.set()
            print(acc)
            send_to_all(self.parent.pipes_out, {**context, "data": acc})
            send_to_all(self.parent.pipes_out, {**context, "data": None})
            print("end left")

    class Right(EndOnce):
        def __init__(self, parent) -> None:
            self.parent = parent

        def start(self) -> object:
            self.parent.right_acc = {}
            return self.parent.right_acc

        def step(self, acc, right_data, context) -> object:
            self.parent.right_acc = self.parent.right_fn(
                acc,
                self.parent.left_acc,
                right_data,
            )
            return self.parent.right_acc

        def end_once(self, right_data, context):
            self.parent.barrier.wait()
            self.parent.send_done.wait()
            self.parent.send_done.clear()
            print("end right")

    def left(self):
        return Join.Left(self)

    def right(self):
        return Join.Right(self)


class Notify(Cursor):
    def __init__(self, observer) -> None:
        self.observer = observer

    def step(self, acc, data, context) -> object:
        return data

    def end(self, acc, context):
        self.observer(acc)


class Persistent:
    def __init__(self, cursor, db, name) -> None:
        self.cursor = cursor
        self.db = db
        self.name = name

    def start(self) -> object:
        state = self.db.state.get(self.name, {})
        acc = state["acc"]
        self.seq_num = state.get("start_seq")
        if self.seq_num:
            items = self.db.items.get(self.name, start=self.seq_num)
            for item in items:
                acc = self.cursor.step(state["acc"], item, state["context"])
                self.seq_num += 1
        else:
            self.seq_num = 0
        return acc

    def step(self, acc, data, context) -> object:
        self.db.items.append(self.seq_num, data)
        acc = self.cursor.step(acc, data, context)
        self.seq_num += 1
        if self.seq_num % 100 == 0:
            self.db.state.save(
                {"acc": acc, "context": context, "last_seq": self.seq_num}
            )
            self.db.items.drop(self.seq_num)
        return acc

    def end(self, acc, context):
        self.db.state.save({"acc": acc, "context": context, "last_seq": self.seq_num})
