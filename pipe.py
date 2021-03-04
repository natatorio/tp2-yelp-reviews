import json

from pika.adapters.blocking_connection import BlockingChannel


class Pipe:
    def __init__(self, exchange, routing_key, queue):
        self.exchange = exchange
        self.queue = queue
        self.routing_key = routing_key

    def bind(self, channel):
        channel.exchange_declare(exchange=self.exchange, exchange_type="direct")
        channel.queue_declare(queue=self.queue, durable=True)
        channel.queue_bind(
            exchange=self.exchange,
            queue=self.queue,
            routing_key=self.routing_key,
        )

    def recv(self, channel: BlockingChannel):
        for method, _, body in channel.consume(self.queue, auto_ack=False):
            yield (
                lambda: channel.basic_ack(method.delivery_tag),
                json.loads(body.decode("utf-8")),
            )

    def send(self, channel, data):
        channel.basic_publish(
            exchange=self.exchange,
            routing_key=self.routing_key,
            body=json.dumps(data),
        )
