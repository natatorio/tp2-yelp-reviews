import os
import pika
import json

class Router:

    def __init__(self):
        amqp_url = os.environ['AMQP_URL']
        parameters = pika.URLParameters(amqp_url)
        self.connection = pika.BlockingConnection(parameters)
        self.channel = self.connection.channel()
        self.channel.exchange_declare(exchange='data', exchange_type='direct')
        queue_name = self.channel.queue_declare(queue='', durable=True).method.queue
        self.channel.queue_bind(exchange='data', queue=queue_name, routing_key='review')
        self.reviewsTag = self.channel.basic_consume(queue=queue_name, on_message_callback=self.route, auto_ack=True)

        queue_name = self.channel.queue_declare(queue='', durable=True).method.queue
        self.channel.queue_bind(exchange='data', queue=queue_name, routing_key='END')
        self.channel.basic_consume(queue=queue_name, on_message_callback=self.stop, auto_ack=True)

    def run(self):
        try:
            self.channel.start_consuming()
        except KeyboardInterrupt:
            self.channel.stop_consuming()
        finally:
            self.channel.close()
            self.connection.close()

    def route(self, ch, method, properties, body):
        reviews = json.loads(body)
        ch.exchange_declare(exchange='reviews', exchange_type='direct')
        funny = [{'funny':r['funny'], 'business_id':r['business_id']} for r in reviews]
        comment = [{'text':r['text'], 'user_id':r['user_id']} for r in reviews]
        users = [{'user_id':r['user_id']} for r in reviews]
        stars5 = [{'stars':r['stars'], 'user_id':r['user_id']} for r in reviews]
        histogram = [{'date':r['date']} for r in reviews]
        ch.basic_publish(exchange='reviews', routing_key="funny", body=json.dumps(funny))
        ch.basic_publish(exchange='reviews', routing_key="comment", body=json.dumps(comment))
        ch.basic_publish(exchange='reviews', routing_key="users", body=json.dumps(users))
        ch.basic_publish(exchange='reviews', routing_key="stars5", body=json.dumps(stars5))
        ch.basic_publish(exchange='reviews', routing_key="histogram", body=json.dumps(histogram))

    def stop(self, ch, method, props, body):
        map(self.route, self.channel.basic_cancel(self.reviewsTag))
        self.channel.stop_consuming()
        self.channel.basic_publish(exchange='reviews', routing_key="comment.END", properties=props, body='')
        self.channel.basic_publish(exchange='reviews', routing_key="users.END", properties=props, body='')
        self.channel.basic_publish(exchange='reviews', routing_key="funny.END", properties=props, body='')
        self.channel.basic_publish(exchange='reviews', routing_key="stars5.END", properties=props, body='')
        self.channel.basic_publish(exchange='reviews', routing_key="histogram.END", properties=props, body='')

def main():
    Router().run()

if __name__ == '__main__':
    main()
