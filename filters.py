from kevasto import Client
import os
import logging
from threading import Barrier, Event
from typing import Dict, cast
from pipe import Pipe, Send

logger = logging.getLogger("filter")
logger.setLevel(logging.INFO)


class Cursor:
    is_done = False

    def setup(self, caller):
        return

    def start(self) -> object:
        return {}

    def step(self, acc, payload) -> object:
        return None

    def end(self, acc, context):
        pass

    def close(self):
        pass


class Filter:
    def __init__(self, pipe_in: Pipe):
        self.pipe_in = pipe_in

    def run(self, cursor: Cursor):
        cursor.setup(self)
        acc = cursor.start()
        if not cursor.is_done:
            logger.info("start consuming %s", self.pipe_in)
            for payload, ack in self.pipe_in.recv(auto_ack=False):
                if payload.get("data"):
                    acc = cursor.step(acc, payload)
                    ack()
                else:
                    cursor.end(acc, payload)
                    ack()
                    break
            logger.info("done consuming %s", self.pipe_in)
        cursor.close()

    def close(self):
        self.pipe_in.close()

    def __enter__(self):
        return self

    def __exit__(self, ex_type, ex, trace):
        self.close()
        if ex:
            logger.exception(str(ex))
        return False


class EndOnce(Cursor):
    replicas = int(os.environ.get("N_REPLICAS", 1))

    def setup(self, caller) -> object:
        self.pipe_in = caller.pipe_in

    def end(self, acc, payload):
        self.done(acc, payload)
        count_down = payload.pop("count_down", self.replicas)
        if count_down > 1:
            logger.info("count_down %s %s", count_down, self.pipe_in)
            self.pipe_in.send(
                {
                    **payload,
                    "data": None,
                    "count_down": count_down - 1,
                }
            )
        else:
            self.end_once(acc, payload)
            logger.info("batch done: %s %s", self.pipe_in, payload["session_id"])
        self.is_done = True

    def end_once(self, acc, payload):
        return

    def done(self, acc, payload):
        return


class Mapper(EndOnce):
    def __init__(self, map_fn, pipe_out: Send, start_fn=lambda: None) -> None:
        self.pipe_out = pipe_out
        self.map_fn = map_fn
        self.start_fn = start_fn
        self.replicas = int(os.environ.get("N_REPLICAS", 1))

    def start(self) -> object:
        return self.start_fn()

    def step(self, acc, payload) -> object:
        data = payload["data"]
        self.pipe_out.send({**payload, "data": self.map_fn(data)})
        return acc

    def end_once(self, acc, payload):
        self.pipe_out.send({**payload, "data": None})

    def close(self):
        self.pipe_out.close()


class Reducer(EndOnce):
    def __init__(self, step_fn, pipe_out: Send) -> None:
        self.pipe_out = pipe_out
        self.step_fn = step_fn

    def step(self, acc, payload) -> object:
        return self.step_fn(acc, payload["data"])

    def done(self, acc, payload):
        self.pipe_out.send({**payload, "data": acc})

    def end_once(self, acc, payload):
        self.pipe_out.send({**payload, "data": None})

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
            return {}

        def step(self, acc, payload) -> object:
            return self.step_fn(
                acc,
                payload["data"],
            )

        def end_once(self, left_acc, payload):
            self.parent.barrier.wait()
            acc = self.parent.join_fn(left_acc, self.parent.right_acc)
            self.parent.send_done.set()
            self.parent.pipe_out.send({**payload, "data": acc})
            self.parent.pipe_out.send({**payload, "data": None})

        def close(self):
            pass

    class Right(EndOnce):
        def __init__(self, parent, step_fn) -> None:
            self.parent = parent
            self.step_fn = step_fn

        def start(self) -> object:
            return {}

        def step(self, acc, payload) -> object:
            return self.step_fn(
                acc,
                payload["data"],
            )

        def end_once(self, right_acc, payload):
            self.parent.right_acc = right_acc
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

    def end(self, acc, payload):
        self.observer(acc)


CHECKPOINT = int(os.environ.get("CHECKPOINT", 100))


class Persistent(Cursor):
    def __init__(self, name: str, cursor: Cursor, client: Client) -> None:
        self.cursor = cursor
        self.db = client
        self.name = name

    def setup(self, caller):
        return self.cursor.setup(caller)

    def start_from_scratch(self):
        self.seq_num = 0
        self.db.log_drop(self.name, None)
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
        for item in items:
            if item.get("data") is None:
                self.end(acc, item)
                self.is_done = self.cursor.is_done
            else:
                acc = self.cursor.step(acc, item)
                self.seq_num += 1
        return acc

    def start(self) -> object:
        state = cast(Dict, self.db.get(self.name, "state"))
        logger.debug("state %s", state)
        if state is None:
            return self.start_from_scratch()
        else:
            return self.start_from_checkpoint(state)

    def step(self, acc, payload) -> object:
        self.db.log_append(self.name, payload)
        acc = self.cursor.step(acc, payload)
        if self.seq_num % CHECKPOINT == 0:
            payload.pop("data", None)
            self.db.put(
                self.name,
                "state",
                {
                    "acc": acc,
                    "seq_num": self.seq_num,
                },
            )
            if self.seq_num > 0:
                self.db.log_drop(self.name, self.seq_num)
        self.seq_num += 1
        return acc

    def end(self, acc, payload):
        self.db.log_append(self.name, payload)
        self.cursor.end(acc, payload)
        self.db.delete(
            self.name,
            "state",
        )

    def close(self):
        self.cursor.close()


class Dedup:
    def __init__(self, name: str, cursor: Cursor, client: Client) -> None:
        self.cursor = cursor
        self.processed = set()
        self.db = client
        self.name = name + "_processed"

    def setup(self, caller):
        return self.cursor.setup(caller)

    def start(self) -> object:
        processed = self.db.log_fetch(self.name, 0)
        if processed is None:
            processed = []
        self.processed = set(processed)
        return self.cursor.start()

    def step(self, acc, payload) -> object:
        if payload["id"] in self.processed:
            return acc
        acc = self.cursor.step(acc, payload)
        self.processed.add(payload["id"])
        self.db.log_append(self.name, payload["id"])
        return acc

    def end(self, acc, payload):
        self.cursor.end(acc, payload)
        self.db.log_drop(self.name, None)
        self.is_done = self.cursor.is_done

    def close(self):
        self.cursor.close()


class Keep(Cursor):
    is_done = False

    def __init__(self, cursor, batch_id) -> None:
        super().__init__()
        self.cursor = cursor
        self.batch_id = batch_id

    def setup(self, caller):
        return self.cursor.setup(caller)

    def start(self) -> object:
        return self.cursor.start()

    def step(self, acc, payload) -> object:
        if self.batch_id == payload["session_id"]:
            return self.cursor.step(acc, payload)
        return acc

    def end(self, acc, context):
        self.cursor.end(acc, context)
        self.is_done = self.cursor.is_done

    def close(self):
        self.cursor.close()
