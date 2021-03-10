import json
import logging

import pika
import os


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
        self.connection = open_connection()

    def close(self):
        self.connection.close()

    def channel(self):
        i = 0
        while i < RETRIES:
            try:
                if self.connection.is_closed:
                    self.connection = open_connection()
                return self.connection.channel()
            except Exception as e:
                logger.exception(f"while trying to get channel {str(e)}")
            i += 1
        raise Exception("Couldn't instantiate channel")


@contextmanager
def lease_channel():
    connection = Connection()
    channel = connection.channel()
    try:
        yield channel
    finally:
        channel.close()
        connection.close()


class Send:
    def __init__(self, exchange, routing_key) -> None:
        self.connection = None
        self.channel = None
        self.routing_key = routing_key
        self.exchange = exchange

    def send(self, data):
        self.send_to(self.exchange, self.routing_key, data)

    def send_to(self, exchange, routing_key, data):
        if self.connection is None:
            self.connection = Connection()
        if self.channel is None:
            self.channel = self.connection.channel()
        i = 0
        while i < RETRIES:
            try:
                return self.channel.basic_publish(
                    exchange=exchange,
                    routing_key=routing_key,
                    body=json.dumps(data),
                )
            except Exception as e:
                logger.exception(f"retry connection {str(e)}")
                self.channel = self.connection.channel()
            i += 1
        raise Exception("Couldn't instantiate channel")

    def close(self):
        if self.channel is not None:
            self.channel.close()
        if self.connection is not None:
            self.connection.close()


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


class Recv:
    def recv(self, auto_ack=False):
        return

    def close(self):
        return


class Pipe(Send, Recv):
    def __init__(self, exchange, routing_key, queue):
        logger.info("pipe %s %s %s", exchange, routing_key, queue)
        self.connection = None
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
        if self.connection is None:
            self.connection = Connection()
        if self.channel is None:
            self.channel = self.connection.channel()
        i = 0
        while i < RETRIES:
            try:
                for method, _, body in self.channel.consume(
                    self.queue, auto_ack=auto_ack
                ):
                    yield (
                        json.loads(body.decode("utf-8")),
                        lambda: self.channel.basic_ack(method.delivery_tag),
                    )
                return
            except Exception as e:
                logger.exception(f"retry connection {str(e)}")
                self.channel = self.connection.channel()
            i += 1
        raise Exception("Couldn't instantiate channel")

    def cancel(self):
        self.channel.cancel()

    def close(self):
        if self.channel is not None:
            self.channel.close()
        if self.connection is not None:
            self.connection.close()
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

    def __exit__(self):
        self.close()


# routed by reviews
def consume_business():
    return Pipe(exchange="reviews", routing_key="business", queue="business.CONSUME")


def consume_comment():
    return Pipe(exchange="reviews", routing_key="comment", queue="comment.CONSUME")


def consume_comment_data():
    return Pipe(exchange="reviews", routing_key="comment.DATA", queue="comment.DATA")


def consume_funny():
    return Pipe(exchange="reviews", routing_key="funny", queue="funny.CONSUME")


def consume_histogram():
    return Pipe(exchange="reviews", routing_key="histogram", queue="histogram.CONSUME")


def consume_star5():
    return Pipe(exchange="reviews", routing_key="star5", queue="star5.CONSUME")


def consume_star5_data():
    return Pipe(exchange="reviews", routing_key="star5.DATA", queue="star5.DATA")


def consume_users():
    return Pipe(exchange="reviews", routing_key="users", queue="users.CONSUME")


# routed by map
def pub_funny_data():
    return Send(exchange="map", routing_key="funny.DATA")


def sub_map_funny_data():
    return Pipe(exchange="map", routing_key="funny.DATA", queue="")


def map_funny():
    return Pipe(exchange="map", routing_key="funny", queue="funny.MAP_QUEUE")


def map_comment():
    return Pipe(exchange="map", routing_key="comment", queue="comment.MAP_QUEUE")


def map_histogram():
    return Pipe(exchange="map", routing_key="histogram", queue="histogram.MAP_QUEUE")


def map_stars5():
    return Pipe(exchange="map", routing_key="stars5", queue="stars5.MAP_QUEUE")


# routed by data
def data_business():
    return Pipe(exchange="data", routing_key="business", queue="business.QUEUE")


def data_review():
    return Pipe(exchange="data", routing_key="review", queue="review.QUEUE")


def annon():
    return Pipe(exchange="", routing_key="reports", queue="reports")
