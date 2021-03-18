import os
from threading import Thread
from kevasto import Client
from filters import Filter, Join, Mapper, Notify, Persistent, Reducer
import logging
import docker

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


def count_key(key):
    def key_counter(acc, data):
        for elem in data:
            acc[elem[key]] = acc.get(elem[key], 0) + 1
        return acc

    return key_counter


def use_value(acc, right):
    return right


def mapper(
    pipe_in,
    pipe_out,
    map_fn,
    start_fn=lambda: None,
    logger=logger,
):
    logger.setLevel(logging.INFO)
    consumer = Filter(pipe_in)
    mapper = Mapper(
        start_fn=start_fn,
        map_fn=map_fn,
        pipe_out=pipe_out,
    )
    try:
        logger.info("waiting")
        consumer.run(mapper)
    except Exception as e:
        logger.exception("")
        raise e
    finally:
        consumer.close()


def sink(pipe_in, observer, logger=logger):
    client = docker.from_env()
    container = client.containers.get(os.environ["HOSTNAME"])
    host_name = container.name
    client.close()
    consumer = Filter(pipe_in)
    reducer = Persistent(
        cursor=Notify(observer=observer),
        client=Client(),
        name=host_name,
    )
    try:
        consumer.run(reducer)
    except Exception as e:
        logger.exception("")
        raise e
    finally:
        consumer.close()


def reducer(pipe_in, pipe_out, step_fn, logger=logger):
    client = docker.from_env()
    container = client.containers.get(os.environ["HOSTNAME"])
    host_name = container.name
    client.close()
    consumer = Filter(pipe_in)
    reducer = Persistent(
        cursor=Reducer(
            step_fn=step_fn,
            pipe_out=pipe_out,
        ),
        client=Client(),
        name=host_name,
    )
    try:
        consumer.run(reducer)
    except Exception as e:
        logger.exception("")
        raise e
    finally:
        consumer.close()


def joiner(pipe_left, left_fn, pipe_right, right_fn, pipe_out, join_fn):
    left_consumer = Filter(pipe_left)
    right_consumer = Filter(pipe_right)
    joint = Join(join_fn, pipe_out)

    def consume_left():
        left_mapper = joint.left(left_fn)
        try:
            left_consumer.run(cursor=left_mapper)
        finally:
            left_consumer.close()

    right_mapper = joint.right(right_fn)
    try:
        thread = Thread(target=consume_left)
        thread.start()

        right_consumer.run(right_mapper)

        thread.join()
    except Exception as e:
        logger.exception("")
        raise e
    finally:
        right_consumer.close()
