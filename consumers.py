import json
import os
from typing import Dict

import pika


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

        self.stateChannel = self.connection.channel()
        self.stateQueue = self.stateChannel.queue_declare(
            queue=routing_key+'.STATE', durable=True
        ).method.queue
        self.stateChannel.queue_bind(
            exchange=exchange,
            queue=self.stateQueue,
            routing_key=routing_key + '.STATE'
        )
        m, p, b = self.stateChannel.basic_get(self.stateQueue, auto_ack=True)
        initialState = json.dumps({}) if (None, None, None) == (m, p, b) else b
        self.stateChannel.basic_publish(
            exchange=self.exchange,
            routing_key=self.routing_key + '.STATE',
            body = initialState
        )
        print("Initial State", initialState)

    def prepare(self):
        return

    def run(self):
        print("Start Consuming", self.exchange, self.routing_key)
        self.prepare()
        try:
            for method, props, body in self.channel.consume(
                self.consumer_queue, auto_ack=True
            ):
                payload = json.loads(body.decode("utf-8"))
                data = payload["data"]
                if data:
                    self.aggregate(data)
                else:
                    self.reply_to = props.reply_to
                    count_down = payload.get("count_down", self.replicas)
                    if count_down > 1:
                        print("count_down", count_down)
                        self.channel.basic_publish(
                            exchange=self.exchange,
                            routing_key=self.routing_key,
                            properties=props,
                            body=json.dumps(
                                {
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
        for m, p, b in self.stateChannel.consume(self.stateQueue, auto_ack=False):
            if (None,None,None) == (m,p,b):
                return
            state = json.loads(b.decode("utf-8"))
            self.stateChannel.basic_ack(m.delivery_tag)
            self.stateChannel.cancel()
        return state

    def put_state(self, state):
        self.stateChannel.basic_publish(
            exchange=self.exchange,
            routing_key=self.routing_key + '.STATE',
            body = json.dumps(state)
        )

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

    def get_business_cities(self):
        self.run()
        return self.get_state()

    def aggregate(self, data):
        newBusinessCities = {}
        for elem in data:
            newBusinessCities[elem['business_id']] = elem['city']
        businessCities = {**self.get_state(), **newBusinessCities}
        self.put_state(businessCities)


class CounterBy(Consumer):
    def __init__(self, keyId, exchange, routing_key):
        self.keyId = keyId
        super().__init__(exchange, routing_key)

    def count(self):
        self.run()
        return self.get_state()

    def aggregate(self, data):
        keyCount = self.get_state()
        for elem in data:
            keyCount[elem[self.keyId]] = keyCount.get(elem[self.keyId], 0) + 1
        self.put_state(keyCount)


class JoinerCounterBy(CounterBy):
    def prepare(self):
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
                self.data_queue_name, auto_ack=True
            ):
                payload = json.loads(body.decode("utf-8"))
                self.data = payload["data"]
                count_down = payload.get("count_down", self.replicas)
                if count_down > 1:
                    print("count_down", count_down)
                    self.channel.basic_publish(
                        exchange=self.exchange,
                        routing_key=self.data_routing_key,
                        properties=props,
                        body=json.dumps(
                            {
                                "data": self.data,
                                "count_down": count_down - 1,
                            }
                        ),
                    )
                else:
                    print("count_down done")
                break
        finally:
            self.channel.cancel()

    def join(self, dictA):
        return {k: v for (k, v) in dictA.items() if dictA[k] == self.data.get(k, 0)}


class CommentQuerier(JoinerCounterBy):
    def aggregate(self, data):
        keyCount = self.get_state()
        for elem in data:
            commentCount = keyCount.get(elem[self.keyId])
            if commentCount and commentCount[0] == elem["text"]:
                keyCount[elem[self.keyId]] = (commentCount[0], commentCount[1] + 1)
            else:
                keyCount[elem[self.keyId]] = (elem["text"], 1)
        self.put_state(keyCount)

    def join(self, dictA):
        return {
            k: v[1] for (k, v) in dictA.items() if dictA[k][1] == self.data.get(k, 0)
        }
