import os
import pika
import json
import datetime
import hashlib

class Consumer():

    def __init__(self, exchange, routingKey):
        amqp_url = os.environ['AMQP_URL']
        parameters = pika.URLParameters(amqp_url)
        self.connection = pika.BlockingConnection(parameters)
        self.channel = self.connection.channel()
        self.channel.exchange_declare(exchange=exchange, exchange_type='direct')
        self.consumerQueue = self.channel.queue_declare(queue='', durable=True).method.queue
        self.channel.queue_bind(exchange=exchange, queue=self.consumerQueue, routing_key=routingKey)

        self.endQueue = self.channel.queue_declare(queue='', durable=True).method.queue
        self.channel.queue_bind(exchange=exchange, queue=self.endQueue, routing_key=routingKey+'.END')

        self.activeProducers = int(os.environ['N_MAPPERS'])

    def bind_consume(self):
        self.consumerTag = self.channel.basic_consume(queue=self.consumerQueue, on_message_callback=self.aggregate, auto_ack=True)
        self.channel.basic_consume(queue=self.endQueue, on_message_callback=self.end, auto_ack=True)

    def start_consuming(self, bind_first=True):
        if bind_first: self.bind_consume()
        try:
            self.channel.start_consuming()
        except KeyboardInterrupt:
            self.channel.stop_consuming()
            self.close()

    def stop_consuming(self):
        self.channel.stop_consuming()

    def close(self):
        self.channel.close()
        self.connection.close()

    def aggregate(self, ch, method, properties, body):
        return

    def end(self, ch, method, properties, body):
        self.activeProducers -= 1
        if not self.activeProducers:
            map(self.aggregate, self.channel.basic_cancel(self.consumerTag))
            self.reply_to = properties.reply_to
            self.stop_consuming()

    def reply(self, response):
        self.channel.basic_publish(exchange='', routing_key=self.reply_to, body=json.dumps(response))

    def forward(self, exchange, send_to, response):
        props = pika.BasicProperties(reply_to=self.reply_to,)
        self.channel.basic_publish(exchange=exchange, routing_key=send_to + '.DATA',
            properties = props, body=json.dumps(response))

class BusinessConsumer(Consumer):

    def __init__(self, exchange, routingKey):
        super().__init__(exchange, routingKey)
        self.businessCities = {}

    def get_business_cities(self):
        self.start_consuming()
        return self.businessCities

    def aggregate(self, ch, method, properties, body):
        for elem in json.loads(body):
            self.businessCities[elem['business_id']] = elem['city']

class CounterBy(Consumer):

    def __init__(self, keyId, exchange, routingKey):
        self.keyId = keyId
        self.keyCount = {}
        super().__init__(exchange, routingKey)

    def count(self):
        self.start_consuming()
        return self.keyCount

    def aggregate(self, ch, method, properties, body):
        for elem in json.loads(body):
            self.keyCount[elem[self.keyId]] = self.keyCount.get(elem[self.keyId], 0) + 1

class JoinerCounterBy(CounterBy):

    def __init__(self, keyId, exchange, routingKey):
        super().__init__(keyId, exchange, routingKey)
        self.data = None
        queue_name = self.channel.queue_declare(queue='', durable=True).method.queue
        self.channel.queue_bind(exchange=exchange, queue=queue_name, routing_key=routingKey+'.DATA')
        self.channel.basic_consume(queue=queue_name, on_message_callback=self.receive_data, auto_ack=True)

    def receive_data(self, ch, method, properties, body):
        self.data = json.loads(body)
        if not self.activeProducers:
            self.channel.stop_consuming()

    def stop_consuming(self):
        if self.data:
            self.channel.stop_consuming()

    def join(self, dictA):
        return dict([(k,v) for (k,v) in dictA.items() if dictA[k] == self.data.get(k, 0)])

class CommentQuerier(JoinerCounterBy):

    def aggregate(self, ch, method, properties, body):
        for elem in json.loads(body):
            commentCount = self.keyCount.get(elem[self.keyId])
            if commentCount and commentCount[0] == elem['text']:
                self.keyCount[elem[self.keyId]] = (commentCount[0], commentCount[1] + 1)
            else:
                self.keyCount[elem[self.keyId]] = (elem['text'], 1)

    def join(self, dictA):
        return dict([(k,v[1]) for (k,v) in dictA.items() if dictA[k][1] == self.data.get(k, 0)])
