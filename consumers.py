import json
import os
from typing import Dict, List, Tuple

from pika.adapters.blocking_connection import BlockingChannel
from kevasto import Client
import pika
import time


class Consumer:
    def __init__(self, exchange, routing_key):
        self.exchange = exchange
        self.routing_key = routing_key
        self.connection = pika.BlockingConnection(
            parameters=pika.URLParameters(url=os.environ["AMQP_URL"])
        )
        self.channel = self.connection.channel()
        self.channel.exchange_declare(exchange=exchange, exchange_type="direct")
        self.consumer_queue = self.channel.queue_declare(
            queue=self.routing_key + ".CONSUME", durable=True
        ).method.queue
        self.channel.queue_bind(
            exchange=exchange,
            queue=self.consumer_queue,
            routing_key=self.routing_key,
        )

        self.replicas = int(os.environ["N_REPLICAS"])
        time.sleep(3)
        self.state_store = Client("tp3_kevasto_1")
        self.i = 0
        self.start = 0
        self.reply_to = None

    def run(self):
        print("Start Consuming", self.exchange, self.routing_key)
        try:
            for method, props, body in self.channel.consume(
                self.consumer_queue, auto_ack=False
            ):
                payload = json.loads(body.decode("utf-8"))
                self.state_store.put(self.routing_key, payload["id"], payload)
                self.channel.basic_ack(method.delivery_tag)
                data = payload["data"]
                if data:
                    self.aggregate(data)
                    # self.state_store.put(self.routing_key, "state", self.get_state())
                else:
                    self.reply_to = props.reply_to
                    print(payload.get("reply"))
                    count_down = payload.pop("count_down", self.replicas)
                    if count_down > 1:
                        print("count_down", count_down)
                        self.channel.basic_publish(
                            exchange=self.exchange,
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
                    else:
                        print("count_down done")
                    break
        finally:
            self.channel.cancel()
        print("Done Consuming", self.exchange, self.routing_key)

    def close(self):
        self.connection.process_data_events()
        self.connection.close()

    def get_state(self):
        return self.state_store.get(self.routing_key, "state")

    def put_state(self, state):
        self.state_store.put(self.routing_key, "state", state)

    def is_state_done(self):
        # TODO Caso borde: Si se cae entre que termino de aggregar y se resetea estado quedaría bloqueado intentando
        # procesar un proximo mensaje que nunca va a llegar
        return False

    def aggregate(self, data):
        return

    def reply(self, response):
        print("Reply", self.reply_to, response)
        self.channel.basic_publish(
            exchange="",
            routing_key=self.reply_to,
            body=json.dumps(response),
        )

    def forward(self, exchange, send_to, response):
        self.channel.basic_publish(
            exchange=exchange,
            routing_key=send_to + ".DATA",
            body=json.dumps({"data": response}),
        )


class BusinessConsumer(Consumer):
    def __init__(self, exchange, routing_key):
        super().__init__(exchange, routing_key)
        self.businessCities = self.get_state()

    def get_business_cities(self):
        if not self.is_state_done():
            self.run()
        return self.businessCities

    def aggregate(self, data):
        newBusinessCities = {}
        for elem in data:
            newBusinessCities[elem["business_id"]] = elem["city"]
        self.businessCities = {**self.get_state(), **newBusinessCities}
        self.put_state(self.businessCities)


class CounterBy(Consumer):
    def __init__(self, keyId, exchange, routing_key):
        self.keyId = keyId
        super().__init__(exchange, routing_key)
        self.keyCount = self.get_state()

    def count(self):
        if not self.is_state_done():
            self.run()
        return self.keyCount

    def aggregate(self, data):
        self.keyCount = self.get_state()
        for elem in data:
            self.keyCount[elem[self.keyId]] = self.keyCount.get(elem[self.keyId], 0) + 1
        self.put_state(self.keyCount)


class JoinerCounterBy(CounterBy):
    def join(self, aggregated):
        self.data = None
        self.data_queue_name = self.channel.queue_declare(
            queue=self.routing_key + ".JOIN_DATA", durable=True
        ).method.queue
        self.data_routing_key = self.routing_key + ".DATA"
        self.channel.queue_bind(
            exchange=self.exchange,
            queue=self.data_queue_name,
            routing_key=self.data_routing_key,
        )
        try:
            for method, props, body in self.channel.consume(
                self.data_queue_name, auto_ack=False
            ):
                payload = json.loads(body.decode("utf-8"))
                self.data = payload["data"]
                count_down = payload.pop("count_down", self.replicas)
                if count_down > 1:
                    print("count_down", count_down)
                    self.channel.basic_publish(
                        exchange=self.exchange,
                        routing_key=self.data_routing_key,
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
                ret = self.join_function(aggregated)
                self.reply((self.routing_key, ret))
                self.channel.basic_ack(method.delivery_tag)
                return ret
        finally:
            self.channel.cancel()

    def join_function(self, dictA):
        return {k: v for (k, v) in dictA.items() if dictA[k] == self.data.get(k, 0)}


class CommentQuerier(JoinerCounterBy):
    def aggregate(self, data):
        self.keyCount = self.get_state()
        for elem in data:
            commentCount = self.keyCount.get(elem[self.keyId])
            if commentCount and commentCount[0] == elem["text"]:
                self.keyCount[elem[self.keyId]] = (commentCount[0], commentCount[1] + 1)
            else:
                self.keyCount[elem[self.keyId]] = (elem["text"], 1)
        self.put_state(self.keyCount)

    def join_function(self, dictA):
        return {
            k: v[1] for (k, v) in dictA.items() if dictA[k][1] == self.data.get(k, 0)
        }


from pipe import Pipe


class Fold:
    pipeIn: Pipe
    pipesOut: List[Pipe]
    replicas: int
    N: int

    def recover(self):
        return (0, {})

    def save_item(self, item):
        pass

    def truncate_items(self, start, n):
        pass

    def save_state(self, n, state):
        pass

    def consume(self, channel: BlockingChannel, fold):
        try:
            start, res = self.recover()
            n = start
            for ack, payload in self.pipeIn.recv(channel):
                self.save_item(payload)
                ack()
                data = payload["data"]
                if data:
                    res = fold(res, data)
                    if n % self.N == 0:
                        self.save_state(n, res)
                        self.truncate_items(start, n)
                        start = n
                else:
                    count_down = payload.get("count_down", self.replicas)
                    if count_down <= 1:
                        print("count_down done, forward eof")
                        for pipeOut in self.pipesOut:
                            pipeOut.send(channel, data)
                        res = {}
                    else:
                        print("count_down", count_down)
                        self.pipeIn.send(
                            channel,
                            {
                                **payload,
                                "count_down": count_down - 1,
                            },
                        )
                    break
        finally:
            channel.cancel()


class Join:
    def consume_left(self):
        pass

    def consume_right(self):
        pass
