import datetime
import hashlib
import json
import os
import pika


class Mapper:
    def __init__(self, exchange, outExchange, routing_key):
        self.routing_key = routing_key
        self.outExchange = outExchange
        self.inExchange = exchange
        self.connection = pika.BlockingConnection(
            parameters=pika.URLParameters(url=os.environ["AMQP_URL"])
        )
        self.channel = self.connection.channel()

        self.channel.exchange_declare(exchange=exchange, exchange_type="direct")
        self.consumer_queue = self.channel.queue_declare(
            queue=routing_key + ".MAP_QUEUE", durable=True
        ).method.queue

        self.channel.queue_bind(
            exchange=exchange,
            queue=self.consumer_queue,
            routing_key=self.routing_key,
        )

        self.replicas = int(os.environ.get("N_REPLICAS", "1"))

    def prepare(self):
        return

    def map(self, data):
        return None

    def run(self):
        print(
            "Start mapping", self.inExchange, self.routing_key, "->", self.outExchange
        )
        try:
            self.prepare()
            for method, props, body in self.channel.consume(
                self.consumer_queue, auto_ack=False
            ):
                payload = json.loads(body.decode("utf-8"))
                data = payload["data"]
                if data:
                    mapped_data = self.map(data)
                    self.channel.basic_publish(
                        exchange=self.outExchange,
                        routing_key=self.routing_key,
                        properties=props,
                        body=json.dumps({**payload, "data": mapped_data}),
                    )
                    self.channel.basic_ack(method.delivery_tag)
                else:
                    count_down = payload.pop("count_down", self.replicas)
                    if count_down <= 1:
                        print("count_down done, forward eof")
                        self.channel.basic_publish(
                            exchange=self.outExchange,
                            routing_key=self.routing_key,
                            properties=props,
                            body=json.dumps({**payload, "data": None}),
                        )
                    else:
                        print("count_down", count_down)
                        self.channel.basic_publish(
                            exchange=self.inExchange,
                            routing_key=self.routing_key,
                            properties=props,
                            body=json.dumps(
                                {
                                    **payload,
                                    "data": None,
                                    "count_down": count_down - 1,
                                }
                            ),
                        )
                    self.channel.basic_ack(method.delivery_tag)
                    break
        finally:
            self.channel.cancel()
            self.connection.close()
        print("Done mapping", self.inExchange, self.routing_key, "->", self.outExchange)


class FunnyMapper(Mapper):
    def prepare(self):
        self.data = None
        self.business_queue = self.channel.queue_declare(
            queue=self.routing_key + ".DATA",
            durable=True,
        ).method.queue
        self.business_routing_key = self.routing_key + ".DATA"
        self.channel.queue_bind(
            exchange=self.inExchange,
            queue=self.business_queue,
            routing_key=self.business_routing_key,
        )
        try:
            for method, props, body in self.channel.consume(
                self.business_queue, auto_ack=False
            ):
                payload = json.loads(body.decode("utf-8"))
                self.data = payload["data"]
                count_down = payload.pop("count_down", self.replicas)
                if count_down > 1:
                    print("count_down:", count_down)
                    self.channel.basic_publish(
                        exchange=self.inExchange,
                        routing_key=self.business_routing_key,
                        properties=props,
                        body=json.dumps(
                            {
                                **payload,
                                "data": self.data,
                                "count_down": count_down - 1,
                            }
                        ),
                    )
                else:
                    print("count_down done")
                self.channel.basic_ack(method.delivery_tag)
                break
        finally:
            self.channel.cancel()
        print("prepare ready")
        return None

    def map(self, reviews):
        return [
            {"city": self.data.get(r["business_id"], "Unknown")}
            for r in reviews
            if r["funny"] != 0
        ]


class CommentMapper(Mapper):
    def map(self, reviews):
        return [
            {
                "text": hashlib.sha1(r["text"].encode()).hexdigest(),
                "user_id": r["user_id"],
            }
            for r in reviews
        ]


class Stars5Mapper(Mapper):
    def map(self, reviews):
        return [
            {"stars": r["stars"], "user_id": r["user_id"]}
            for r in reviews
            if r["stars"] == 5.0
        ]


class HistogramMapper(Mapper):
    def map(self, dates):
        return [
            {
                "weekday": datetime.datetime.strptime(
                    d["date"], "%Y-%m-%d %H:%M:%S"
                ).strftime("%A")
            }
            for d in dates
        ]
