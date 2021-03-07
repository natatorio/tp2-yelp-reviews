import json
import logging

import pika
import os

logging.basicConfig(level=logging.ERROR)
LOG = logging.getLogger("pipe")
LOG.setLevel(logging.INFO)
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
                LOG.exception(f"while trying to get channel {str(e)}")
            i += 1
        raise Exception("Couldn't instantiate channel")


CONNECTION = Connection()


class Send:
    def send(self, data):
        return

    def close(self):
        return


class Formatted:
    def __init__(self, sender, formatter) -> None:
        self.sender = sender
        self.formatter = formatter

    def send(self, data):
        self.sender.send(self.formatter(data))

    def close(self):
        self.sender.close()


class Recv:
    def recv(self, auto_ack=False):
        return

    def close(self):
        return


class Pipe(Send, Recv):
    def __init__(self, exchange, routing_key, queue):
        self.connection = CONNECTION

        self.channel = self.connection.channel()
        self.exchange = exchange
        if exchange:
            self.channel.exchange_declare(
                exchange=self.exchange, exchange_type="direct"
            )

        self.queue = queue
        queue_response = self.channel.queue_declare(queue=queue, durable=True)
        if not queue:
            self.queue = queue_response.method.queue

        self.routing_key = self.queue
        if routing_key:
            self.routing_key = routing_key

        if self.exchange and self.queue:
            self.channel.queue_bind(
                exchange=self.exchange,
                queue=self.queue,
                routing_key=self.routing_key,
            )

    def __str__(self) -> str:
        return f"Pipe[{self.exchange},{self.routing_key},{self.queue}]"

    def recv(self, auto_ack=False):
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
                LOG.exception(f"retry connection {str(e)}")
                self.channel = self.connection.channel()
            i += 1
        raise Exception("Couldn't instantiate channel")

    def cancel(self):
        self.channel.cancel()

    def close(self):
        self.channel.close()
        self.connection.close()

    def send(self, data):
        routing_key_override = self.routing_key
        if data.get("reply"):
            routing_key_override = data["reply"]

        i = 0
        while i < RETRIES:
            try:
                return self.channel.basic_publish(
                    exchange=self.exchange,
                    routing_key=routing_key_override,
                    body=json.dumps(data),
                )
            except Exception as e:
                LOG.exception(f"retry connection {str(e)}")
                self.channel = self.connection.channel()
            i += 1
        raise Exception("Couldn't instantiate channel")

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


def annon():
    return Pipe(exchange="", routing_key="", queue="")


# data_business
#     consume_business
#         map_funny_data

# data_reviews
#    consume_users
#    map_comment
#    map_funny
#     consume_funny
#    map_histogram
#    map_stars5 x consume_star5_data
#     consume_star5
