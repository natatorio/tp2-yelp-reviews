import sys
import os
import logging
from threading import Barrier, Event
from typing import Dict, List, cast
from pipe import Pipe, Send

logger = logging.getLogger("filter")
logger.setLevel(logging.INFO)


class Cursor:
    def setup(self, caller):
        return

    def start(self) -> object:
        return {}

    def step(self, acc, payload) -> object:
        return None

    def end(self, acc, context):
        pass

    def quit(self, acc, context) -> bool:
        return False

    def exception(self, ex):
        raise ex


class Filter:
    def __init__(self, pipe_in: Pipe):
        self.pipe_in = pipe_in

    def run(self, cursor: Cursor):
        logger.info("start consuming %s", self.pipe_in)
        cursor.setup(self)
        acc = cursor.start()
        try:
            for payload, ack in self.pipe_in.recv():
                if payload.get("data", None) is not None:
                    acc = cursor.step(acc, payload)
                    ack()
                else:
                    cursor.end(acc, payload)
                    ack()
                    if cursor.quit(acc, payload):
                        break
                    acc = cursor.start()
        except Exception as e:
            cursor.exception(e)
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
        self.done(acc, context)
        count_down = context.pop("count_down", self.replicas)
        if count_down > 1:
            logger.info("count_down %s %s", count_down, self.pipe_in)
            self.pipe_in.send(
                {
                    **context,
                    "data": None,
                    "count_down": count_down - 1,
                }
            )
        else:
            self.end_once(acc, context)
            logger.info("batch done: %s %s", self.pipe_in, context["session_id"])

    def end_once(self, acc, context):
        return

    def done(self, acc, context):
        return


class MapperScatter(EndOnce):
    def __init__(self, map_fn, start_fn, pipes_out: List[Send]) -> None:
        self.pipes_out = pipes_out
        self.map_fn = map_fn
        self.start_fn = start_fn
        self.replicas = int(os.environ.get("N_REPLICAS", 1))

    def setup(self, caller) -> object:
        self.pipe_in = caller.pipe_in

    def start(self) -> object:
        return self.start_fn()

    def step(self, acc, payload) -> object:
        data = payload["data"]
        send_to_all(self.pipes_out, {**payload, "data": self.map_fn(data)})
        return None

    def end_once(self, acc, context):
        send_to_all(self.pipes_out, {**context, "data": None})

    def close(self):
        for pipe_out in self.pipes_out:
            pipe_out.close()


class ReducerScatter(EndOnce):
    def __init__(self, step_fn, pipes_out: List[Send]) -> None:
        self.pipes_out = pipes_out
        self.step_fn = step_fn

    def step(self, acc, payload) -> object:
        return self.step_fn(acc, payload["data"])

    def done(self, acc, context):
        send_to_all(self.pipes_out, {**context, "data": acc})

    def end_once(self, acc, context):
        send_to_all(self.pipes_out, {**context, "data": None})

    def close(self):
        for pipe_out in self.pipes_out:
            pipe_out.close()


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

        def step(self, acc, payload) -> object:
            left_data = payload["data"]
            self.parent.left_acc = self.parent.left_fn(
                acc,
                left_data,
                self.parent.right_acc,
            )
            return self.parent.left_acc

        def end_once(self, left_data, context):
            self.parent.barrier.wait()
            acc = self.parent.join_fn(self.parent.left_acc, self.parent.right_acc)
            self.parent.send_done.set()
            send_to_all(self.parent.pipes_out, {**context, "data": acc})
            send_to_all(self.parent.pipes_out, {**context, "data": None})

        def exception(self, ex):
            sys.exit(1)

    class Right(EndOnce):
        def __init__(self, parent) -> None:
            self.parent = parent

        def start(self) -> object:
            self.parent.right_acc = {}
            return self.parent.right_acc

        def step(self, acc, payload) -> object:
            right_data = payload["data"]
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

        def exception(self, ex):
            sys.exit(1)

    def left(self):
        return Join.Left(self)

    def right(self):
        return Join.Right(self)

    def close(self):
        for pipe_out in self.pipes_out:
            pipe_out.close()


class Notify(Cursor):
    def __init__(self, observer) -> None:
        self.observer = observer

    def step(self, acc, payload) -> object:
        return payload["data"]

    def end(self, acc, context):
        self.observer(acc)


from kevasto import Client


class Persistent(Cursor):
    processed: set

    def __init__(self, cursor, name) -> None:
        self.cursor = cursor
        self.db = Client()
        self.name = name
        self.batch_done = set()

    def setup(self, caller):
        return self.cursor.setup(caller)

    def quit(self, acc, context) -> bool:
        return self.cursor.quit(acc, context)

    def start_from_scratch(self):
        self.seq_num = 0
        self.db.log_drop(self.name, None)
        self.processed = set()
        acc = self.cursor.start()
        self.db.put(
            self.name,
            "state",
            {
                "acc": acc,
                "seq_num": self.seq_num,
            },
        )
        return acc

    def start_from_checkpoint(self, state):
        self.seq_num = state["seq_num"]
        acc = state["acc"]
        items = self.db.log_fetch(self.name, self.seq_num)
        processed = self.db.get(self.name, "processed")
        if processed is None:
            processed = []
        self.processed = set(processed)
        for item in items:
            self.processed.add(item["id"])
            if item.get("data") is None:
                self.end(acc, item)
                acc = self.start_from_scratch()
            else:
                acc = self.cursor.step(acc, item)
                self.seq_num += 1
        return acc

    def start(self) -> object:
        state = cast(Dict, self.db.get(self.name, "state"))
        if state is None:
            return self.start_from_scratch()
        else:
            return self.start_from_checkpoint(state)

    def step(self, acc, payload) -> object:
        if payload["session_id"] in self.batch_done:
            return acc
        if payload["id"] in self.processed:
            return acc
        self.db.log_append(self.name, self.seq_num, payload)
        self.processed.add(payload["id"])
        acc = self.cursor.step(acc, payload)
        self.seq_num += 1
        if self.seq_num % 100 == 0:
            payload.pop("data", None)
            self.db.put(
                self.name,
                "state",
                {
                    "acc": acc,
                    "seq_num": self.seq_num,
                },
            )
            self.db.log_drop(self.name, self.seq_num)
        return acc

    def end(self, acc, payload):
        # if payload["session_id"] in self.batch_done:
        self.db.log_append(self.name, self.seq_num, payload)
        self.cursor.end(acc, payload)
        self.db.delete(
            self.name,
            "state",
        )
        self.batch_done.add(payload["session_id"])

    def exception(self, ex):
        return self.cursor.exception(ex)
