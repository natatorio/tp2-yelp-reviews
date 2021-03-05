import json
import logging

import pika
import os


LOG = logging.getLogger("pipe")
RETRIES = 3


def open_connection():
    return pika.BlockingConnection(
        parameters=pika.URLParameters(url=os.environ["AMQP_URL"])
    )


class Connection:
    def __init__(self) -> None:
        self.connection = open_connection()

    def channel(self):
        i = 0
        while i < RETRIES:
            try:
                if self.connection.is_closed():
                    self.connection = open_connection()
                return self.connection.channel()
            except Exception as e:
                LOG.error(f"while trying to get channel {str(e)}")
            i += 1
        raise Exception("Couldn't instantiate channel")


CONNECTION = Connection()


class Pipe:
    def __init__(self, exchange, routing_key, queue):
        self.connection = CONNECTION

        self.exchange = exchange
        self.queue = queue
        self.routing_key = routing_key

        self.channel = self.connection.channel()
        self.channel.exchange_declare(exchange=self.exchange, exchange_type="direct")
        self.channel.queue_declare(queue=self.queue, durable=True)
        self.channel.queue_bind(
            exchange=self.exchange,
            queue=self.queue,
            routing_key=self.routing_key,
        )

    def recv(self):
        i = 0
        while i < RETRIES:
            try:
                for method, _, body in self.channel.consume(self.queue, auto_ack=False):
                    yield (
                        lambda: self.channel.basic_ack(method.delivery_tag),
                        json.loads(body.decode("utf-8")),
                    )
                return
            except Exception as e:
                LOG.exception(f"retry connection {str(e)}")
                self.channel = self.connection.channel()
            i += 1
        raise Exception("Couldn't instantiate channel")

    def send(self, data):
        i = 0
        while i < RETRIES:
            try:
                return self.channel.basic_publish(
                    exchange=self.exchange,
                    routing_key=self.routing_key,
                    body=json.dumps(data),
                )
            except Exception as e:
                LOG.exception(f"retry connection {str(e)}")
                self.channel = self.connection.channel()
            i += 1
        raise Exception("Couldn't instantiate channel")


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
def map_funny_data():
    return Pipe(exchange="map", routing_key="funny.DATA", queue="funny.DATA")


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
