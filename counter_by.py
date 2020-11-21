import os
import pika
import json

class CounterBy():

    def __init__(self, keyIds, exchange, routingKey):
        amqp_url = os.environ['AMQP_URL']
        parameters = pika.URLParameters(amqp_url)
        self.connection = pika.BlockingConnection(parameters)
        self.channel = self.connection.channel()
        self.channel.exchange_declare(exchange=exchange, exchange_type='direct')
        queue_name = self.channel.queue_declare(queue='', durable=True).method.queue
        self.channel.queue_bind(exchange=exchange, queue=queue_name, routing_key=routingKey)
        self.consumerTag = self.channel.basic_consume(queue=queue_name, on_message_callback=self.aggregate, auto_ack=True)

        queue_name = self.channel.queue_declare(queue='', durable=True).method.queue
        self.channel.queue_bind(exchange=exchange, queue=queue_name, routing_key=routingKey+'.END')
        self.channel.basic_consume(queue=queue_name, on_message_callback=self.end, auto_ack=True)

        self.keyIds = keyIds
        self.keyCount = {}
        self.activeProducers = int(os.environ['N_ROUTERS'])

    def count(self):
        try:
            self.channel.start_consuming()
        except KeyboardInterrupt:
            self.channel.stop_consuming()
            self.close()
        return self.keyCount

    def close(self):
        self.channel.close()
        self.connection.close()


    def aggregate(self, ch, method, properties, body):
        for elem in json.loads(body):
            key = '-'.join([elem[k] for k in self.keyIds])
            self.keyCount[key] = self.keyCount.get(key, 0) + 1

    def end(self, ch, method, properties, body):
        self.activeProducers -= 1
        if not self.activeProducers:
            map(self.aggregate, self.channel.basic_cancel(self.consumerTag))
            self.reply_to = properties.reply_to
            self.stop_consuming()

    def stop_consuming(self):
        self.channel.stop_consuming()

    def reply(self, response):
        self.channel.basic_publish(exchange='', routing_key=self.reply_to, body=json.dumps(response))

class UserCounterBy(CounterBy):

    def foward(self, exchange, send_to, response):
        props = pika.BasicProperties(reply_to=self.reply_to,)
        self.channel.basic_publish(exchange=exchange, routing_key=send_to + '.DATA',
            properties = props, body=json.dumps(response))

class JoinerCounterBy(CounterBy):

    def __init__(self, keyIds, exchange, routingKey):
        super().__init__(keyIds, exchange, routingKey)
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


class Stars5JoinerCounterBy(JoinerCounterBy):

    def aggregate(self, ch, method, properties, body):
        for elem in json.loads(body):
            if elem['stars'] != 5.0: continue
            key = '-'.join([elem[k] for k in self.keyIds])
            self.keyCount[key] = self.keyCount.get(key, 0) + 1
