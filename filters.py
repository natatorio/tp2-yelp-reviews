import os
import logging
from threading import Barrier, Event
from typing import Dict, cast
from pipe import Pipe, Send

logger = logging.getLogger("filter")
logger.setLevel(logging.INFO)


def count_key(key):
    def key_counter(acc, data):
        for elem in data:
            acc[elem[key]] = acc.get(elem[key], 0) + 1
        return acc

    return key_counter


def use_value(acc, right):
    return right


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
        quit = False
        while not quit:
            acc = cursor.start()
            try:
                for payload, ack in self.pipe_in.recv():
                    if payload.get("data"):
                        acc = cursor.step(acc, payload)
                        ack()
                    else:
                        cursor.end(acc, payload)
                        ack()
                        if cursor.quit(acc, payload):
                            quit = True
                            break
                        acc = cursor.start()
            except Exception as e:
                cursor.exception(e)
            finally:
                self.pipe_in.cancel()
        logger.info("done consuming %s", self.pipe_in)

    def close(self):
        self.pipe_in.close()


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


class Mapper(EndOnce):
    def __init__(self, map_fn, start_fn, pipe_out: Send) -> None:
        self.pipe_out = pipe_out
        self.map_fn = map_fn
        self.start_fn = start_fn
        self.replicas = int(os.environ.get("N_REPLICAS", 1))

    def setup(self, caller) -> object:
        self.pipe_in = caller.pipe_in

    def start(self) -> object:
        return self.start_fn()

    def step(self, acc, payload) -> object:
        data = payload["data"]
        self.pipe_out.send({**payload, "data": self.map_fn(data)})
        return None

    def end_once(self, acc, context):
        self.pipe_out.send({**context, "data": None})

    def close(self):
        self.pipe_out.close()


class Reducer(EndOnce):
    def __init__(self, step_fn, pipe_out: Send) -> None:
        self.pipe_out = pipe_out
        self.step_fn = step_fn

    def step(self, acc, payload) -> object:
        return self.step_fn(acc, payload["data"])

    def done(self, acc, context):
        self.pipe_out.send({**context, "data": acc})

    def end_once(self, acc, context):
        self.pipe_out.send({**context, "data": None})

    def close(self):
        self.pipe_out.close()


class Join:
    def __init__(self, join_fn, pipe_out: Send) -> None:
        self.barrier = Barrier(2)
        self.join_fn = join_fn
        self.pipe_out = pipe_out
        self.send_done = Event()

    class Left(EndOnce):
        def __init__(self, parent, step_fn) -> None:
            self.parent = parent
            self.step_fn = step_fn

        def start(self) -> object:
            self.parent.left_acc = {}
            return self.parent.left_acc

        def step(self, acc, payload) -> object:
            left_data = payload["data"]
            self.parent.left_acc = self.step_fn(
                acc,
                left_data,
            )
            return self.parent.left_acc

        def end_once(self, left_data, context):
            self.parent.barrier.wait()
            acc = self.parent.join_fn(self.parent.left_acc, self.parent.right_acc)
            self.parent.send_done.set()
            self.parent.pipe_out.send({**context, "data": acc})
            self.parent.pipe_out.send({**context, "data": None})

        def close(self):
            pass

    class Right(EndOnce):
        def __init__(self, parent, step_fn) -> None:
            self.parent = parent
            self.step_fn = step_fn

        def start(self) -> object:
            self.parent.right_acc = {}
            return self.parent.right_acc

        def step(self, acc, payload) -> object:
            right_data = payload["data"]
            self.parent.right_acc = self.step_fn(
                acc,
                right_data,
            )
            return self.parent.right_acc

        def end_once(self, right_data, context):
            self.parent.barrier.wait()
            self.parent.send_done.wait()
            self.parent.send_done.clear()

        def close(self):
            pass

    def left(self, step_fn):
        return Join.Left(self, step_fn)

    def right(self, step_fn):
        return Join.Right(self, step_fn)

    def close(self):
        self.pipe_out.close()


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

    def close(self):
        self.cursor.close()
