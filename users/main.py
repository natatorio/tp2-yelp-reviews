import os
import pika
import json

class DistinctCounter():

    def __init__(self, keyIds, exchange, routingKey):
        amqp_url = os.environ['AMQP_URL']
        parameters = pika.URLParameters(amqp_url)
        self.connection = pika.BlockingConnection(parameters)
        self.channel = self.connection.channel()
        self.channel.exchange_declare(exchange=exchange, exchange_type='direct')
        result = self.channel.queue_declare(queue='', durable=True)
        queue_name = result.method.queue
        self.channel.queue_bind(exchange=exchange, queue=queue_name, routing_key=routingKey)
        self.channel.basic_consume(queue=queue_name, on_message_callback=self.callback, auto_ack=True)

        self.keyIds = keyIds
        self.keyCount = {}

    def count(self):
        try:
            self.channel.start_consuming()
        except KeyboardInterrupt:
            self.channel.stop_consuming()
        self.channel.close()
        self.connection.close()
        return self.keyCount

    def callback(self, ch, method, properties, body):
        for elem in json.loads(body):
            key = '-'.join([elem[k] for k in self.keyIds])
            self.keyCount[key] = self.keyCount.get(key, 0) + 1
            print([x for x in self.keyCount.items() if x[1] > 1], flush = True)

def main():
    counter = DistinctCounter(keyIds = ['user_id'], exchange = 'reviews', routingKey = 'users')
    user_count = counter.count()

if __name__ == '__main__':
    main()
