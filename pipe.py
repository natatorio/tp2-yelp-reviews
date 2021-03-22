import json
import logging
import threading
import atexit
from typing import List

import pika
import os
from pika.exceptions import AMQPConnectionError, ChannelClosed

from contextlib import contextmanager

logging.basicConfig(level=logging.ERROR)
logger = logging.getLogger("pipe")
logger.setLevel(logging.INFO)
RETRIES = 3


def open_connection():
    return pika.BlockingConnection(
        parameters=pika.URLParameters(url=os.environ["AMQP_URL"])
    )


class Connection:
    def __init__(self) -> None:
        self.local = threading.local()
        self.local.connection = open_connection()
        atexit.register(self.local.connection.close)

    def close(self):
        if hasattr(self.local, "connection"):
            self.local.connection.close()

    def channel(self):
        if not hasattr(self.local, "connection"):
            self.local.connection = open_connection()
            atexit.register(self.local.connection.close)
        i = 0
        while i < RETRIES:
            try:
                if self.local.connection.is_closed:
                    self.local.connection = open_connection()
                chan = self.local.connection.channel()
                if os.environ.get("PREFETCH_COUNT"):
                    chan.basic_qos(prefetch_count=int(os.environ["PREFETCH_COUNT"]))
                return chan
            except Exception as e:
                logger.exception(f"while trying to get channel {str(e)}")
            i += 1
        raise Exception("Couldn't instantiate channel")


connection = Connection()


@contextmanager
def lease_channel():
    channel = connection.channel()
    try:
        yield channel
    finally:
        channel.close()


class Close:
    def close(self):
        pass


class Send(Close):
    def send(self, data):
        pass


class Exchange(Send):
    def __init__(self, exchange, routing_key) -> None:
        self.channel = None
        self.routing_key = routing_key
        self.exchange = exchange

    def send(self, data):
        self.send_to(self.exchange, self.routing_key, data)

    def send_to(self, exchange, routing_key, data):
        try:
            if self.channel is None:
                self.channel = connection.channel()
            return self.channel.basic_publish(
                exchange=exchange,
                routing_key=routing_key,
                body=json.dumps(data),
            )
        except (AMQPConnectionError, ChannelClosed) as e:
            logger.exception(str(e))
            self.channel = None
            raise

    def close(self):
        if self.channel is not None:
            self.channel.close()
            self.channel = None


class Formatted(Send):
    def __init__(self, sender, formatter) -> None:
        self.sender = sender
        self.formatter = formatter

    def send(self, payload):
        data = payload["data"]
        if data is not None:
            data = self.formatter(data)
        self.sender.send({**payload, "data": data})

    def close(self):
        self.sender.close()


class Scatter(Send):
    def __init__(self, outputs: List[Send]) -> None:
        self.outputs = outputs

    def send(self, data):
        for pipe_out in self.outputs:
            pipe_out.send(data)

    def close(self):
        for output in self.outputs:
            try:
                output.close()
            except:
                logger.exception("on close")


class Recv(Close):
    def recv(self, auto_ack=False):
        return


class Pipe(Recv, Exchange):
    def __init__(self, exchange, routing_key, queue):
        logger.info("pipe %s %s %s", exchange, routing_key, queue)
        self.channel = None
        with lease_channel() as channel:
            self.exchange = exchange
            if exchange:
                channel.exchange_declare(exchange=self.exchange, exchange_type="direct")

            self.remove_queue = not queue
            queue_response = channel.queue_declare(queue=queue, durable=True)
            self.queue = queue_response.method.queue

            self.routing_key = self.queue
            if routing_key:
                self.routing_key = routing_key

            if self.exchange and self.queue:
                channel.queue_bind(
                    exchange=self.exchange,
                    queue=self.queue,
                    routing_key=self.routing_key,
                )

    def __str__(self) -> str:
        return f"Pipe[{self.exchange},{self.routing_key},{self.queue}]"

    def recv(self, auto_ack=False):
        try:
            if self.channel is None or self.channel.is_closed:
                self.channel = connection.channel()
            for method, _, body in self.channel.consume(self.queue, auto_ack=False):
                ack = lambda: self.channel.basic_ack(method.delivery_tag)
                yield (
                    json.loads(body.decode("utf-8")),
                    ack,
                )
                if auto_ack:
                    ack()
        except (AMQPConnectionError, ChannelClosed) as e:
            logger.exception(str(e))
            self.channel = None
            raise
        finally:
            if self.channel and self.channel.is_open:
                self.channel.cancel()

    def close(self):
        if self.channel and self.channel.is_open:
            try:
                self.channel.close()
                self.channel = None
            except:
                logger.error("failed to close channel")
        self.unbind()

    def unbind(self):
        if self.remove_queue:
            with lease_channel() as channel:
                channel.queue_unbind(
                    queue=self.queue,
                    exchange=self.exchange,
                    routing_key=self.routing_key,
                )
                channel.queue_delete(queue=self.queue)

    def __enter__(self):
        return self

    def __exit__(self, ex_type, ex, trace):
        self.close()
        return False


# routed by reviews


def comment_summary():
    return Pipe(
        exchange="reviews",
        routing_key="comment.summary",
        queue="comment.summary",
    )


def user_count_5():
    return Pipe(
        exchange="reviews",
        routing_key="user5.comment",
        queue="user5.comment",
    )


def funny_summary():
    return Pipe(
        exchange="reviews",
        routing_key="funny.summary",
        queue="funny.summary",
    )


def histogram_summary():
    return Pipe(
        exchange="reviews",
        routing_key="histogram.summary",
        queue="histogram.summary",
    )


def star5_summary():
    return Pipe(
        exchange="reviews",
        routing_key="star5.summary",
        queue="star5.summary",
    )


def user_count_50():
    return Pipe(
        exchange="reviews",
        routing_key="user50.star5",
        queue="user50.star5",
    )


def user_summary():
    return Pipe(
        exchange="reviews",
        routing_key="users.summary",
        queue="users.summary",
    )


# routed by map


def map_funny():
    return Pipe(
        exchange="map",
        routing_key="funny",
        queue="funny",
    )


def map_comment():
    return Pipe(
        exchange="map",
        routing_key="comment",
        queue="comment",
    )


def map_histogram():
    return Pipe(
        exchange="map",
        routing_key="histogram",
        queue="histogram",
    )


def map_stars5():
    return Pipe(
        exchange="map",
        routing_key="stars5",
        queue="stars5",
    )


# routed by data
def pub_funny_business_cities():
    return Exchange(
        exchange="map",
        routing_key="funny.business_cities",
    )


def sub_funny_business_cities():
    return Pipe(
        exchange="map",
        routing_key="funny.business_cities",
        queue="",
    )


def business_cities_summary():
    return Pipe(
        exchange="map",
        routing_key="business.summary",
        queue="business.summary",
    )


def data_business():
    return Pipe(
        exchange="data",
        routing_key="business",
        queue="business",
    )


def data_review():
    return Pipe(
        exchange="data",
        routing_key="review",
        queue="review",
    )


def reports():
    return Pipe(
        exchange="",
        routing_key="reports",
        queue="reports",
    )


import docker


def pub_sub_control():
    try:
        client = docker.from_env()
        container = client.containers.get(os.environ["HOSTNAME"])
        name = container.name
        client.close()
    except:
        name = ""
    return Pipe(
        exchange="control",
        routing_key="control",
        queue=name,
    )
