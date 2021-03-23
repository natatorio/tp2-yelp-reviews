import os
from threading import Thread

from kevasto import Client
from filters import Filter, Join, Keep, Mapper, Notify, Persistent, Reducer
import logging
import docker

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


def node_name():
    client = docker.from_env()
    container = client.containers.get(os.environ["HOSTNAME"])
    host_name = container.name
    client.close()
    return host_name


def count_key(key):
    def key_counter(acc, data):
        for elem in data:
            acc[elem[key]] = acc.get(elem[key], 0) + 1
        return acc

    return key_counter


def use_value(acc, right):
    return right


def tolerant(cursor, batch_id, dedup, name):
    client = Client()
    return Keep(
        Persistent(
            name,
            cursor,
            client,
        ),
        batch_id,
        dedup,
    )


def mapper(
    pipe_in,
    pipe_out,
    map_fn,
    batch_id,
    dedup,
    start_fn=lambda: None,
):
    with Filter(pipe_in) as consumer:
        consumer.run(
            Keep(
                Mapper(
                    start_fn=start_fn,
                    map_fn=map_fn,
                    pipe_out=pipe_out,
                ),
                batch_id,
                dedup,
            )
        )


def sink(
    pipe_in,
    observer,
    batch_id,
    dedup,
):
    name = node_name() + "_sink"
    with Filter(pipe_in) as consumer:
        consumer.run(
            tolerant(
                Notify(observer=observer, dedup=dedup, batch_id=batch_id),
                batch_id,
                dedup,
                name,
            )
        )


def reducer(pipe_in, pipe_out, step_fn, batch_id, dedup, suffix=""):
    name = node_name() + suffix
    with Filter(pipe_in) as consumer:
        consumer.run(
            tolerant(
                Reducer(
                    step_fn=step_fn,
                    pipe_out=pipe_out,
                ),
                batch_id,
                dedup,
                name,
            )
        )


def joiner(
    pipe_left,
    left_fn,
    pipe_right,
    right_fn,
    pipe_out,
    join_fn,
    dedup_left,
    dedup_right,
    batch_id,
):
    joint = Join(join_fn, pipe_out)

    name = node_name()
    left_name = name + "_left"
    right_name = name + "_right"

    def consume_left():
        try:
            with Filter(pipe_left) as consumer:
                consumer.run(
                    tolerant(
                        joint.left(left_fn),
                        batch_id,
                        dedup_left,
                        left_name,
                    )
                )
            return
        # except ChannelClosed as e:
        # logger.exception(str(e))
        except Exception as e:
            logger.exception(str(e))
            os._exit(1)

    def consume_right():
        try:
            with Filter(pipe_right) as consumer:
                consumer.run(
                    tolerant(
                        joint.right(right_fn),
                        batch_id,
                        dedup_right,
                        right_name,
                    )
                )
            return
        # except ChannelClosed as e:
        # logger.exception(str(e))
        except Exception as e:
            logger.exception(str(e))
            os._exit(1)

    thread = Thread(target=consume_left, daemon=True)
    thread.start()
    consume_right()
    thread.join()


import debug
