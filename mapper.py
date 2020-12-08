import os
import pika
import json
import datetime
import hashlib


class Mapper:

    def __init__(self, exchange, outExchange, routingKey):
        amqp_url = os.environ['AMQP_URL']
        parameters = pika.URLParameters(amqp_url)
        self.routingKey = routingKey
        self.outExchange = outExchange
        self.connection = pika.BlockingConnection(parameters)
        self.channel = self.connection.channel()
        self.channel.exchange_declare(exchange=exchange, exchange_type='direct')
        self.consumerQueue = self.channel.queue_declare(queue='', durable=True).method.queue
        self.channel.queue_bind(exchange=exchange, queue=self.consumerQueue, routing_key=self.routingKey)

        self.endQueue = self.channel.queue_declare(queue='', durable=True).method.queue
        self.channel.queue_bind(exchange=exchange, queue=self.endQueue, routing_key=self.routingKey+'.END')

        self.activeProducers = int(os.environ['N_ROUTERS'])

    def bind_consume(self):
        self.consumerTag = self.channel.basic_consume(queue=self.consumerQueue, on_message_callback=self.consume, auto_ack=True)
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

    def consume(self, ch, method, properties, body):
        data = json.loads(body)
        mappedData = self.map(data)
        ch.basic_publish(exchange=self.outExchange, routing_key=self.routingKey, body=json.dumps(mappedData))

    def map(self, data):
        return

    def end(self, ch, method, properties, body):
        self.activeProducers -= 1
        if not self.activeProducers:
            map(self.consume, self.channel.basic_cancel(self.consumerTag))
            self.stop_consuming()
            ch.basic_publish(exchange=self.outExchange, routing_key=self.routingKey+".END", properties=properties, body='')

class FunnyMapper(Mapper):

    def __init__(self, exchange, outExchange, routingKey):
        super().__init__(exchange, outExchange, routingKey)
        self.data = None
        queue_name = self.channel.queue_declare(queue='', durable=True).method.queue
        self.channel.queue_bind(exchange=exchange, queue=queue_name, routing_key=routingKey+'.DATA')
        self.channel.basic_consume(queue=queue_name, on_message_callback=self.receive_data, auto_ack=True)

    def receive_data(self, ch, method, properties, body):
        self.data = json.loads(body)
        self.bind_consume()

    def map(self, reviews):
        return [{'city': self.data.get(r['business_id'], 'Unknown')} for r in reviews if r['funny'] != 0]

class CommentMapper(Mapper):

    def map(self, reviews):
        return [{'text':hashlib.sha1(r['text'].encode()).hexdigest(), 'user_id':r['user_id']} for r in reviews]

class Stars5Mapper(Mapper):

    def map(self, reviews):
        return [{'stars':r['stars'], 'user_id':r['user_id']} for r in reviews if r['stars'] == 5.0]

class HistogramMapper(Mapper):

    def map(self, dates):
        return [{'weekday': datetime.datetime.strptime(d['date'], '%Y-%m-%d %H:%M:%S').strftime('%A')} for d in dates]
