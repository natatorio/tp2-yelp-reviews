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
        self.channel.exchange_declare(exchange='reviews', exchange_type='direct')
        self.channel.exchange_declare(exchange='map', exchange_type='direct')

        queue_name = self.channel.queue_declare(queue='', durable=True).method.queue
        self.channel.queue_bind(exchange='data', queue=queue_name, routing_key='business')
        self.businessTag = self.channel.basic_consume(queue=queue_name, on_message_callback=self.route_business, auto_ack=True)

        self.reviewsQueue = self.channel.queue_declare(queue='', durable=True).method.queue
        self.channel.queue_bind(exchange='data', queue=self.reviewsQueue, routing_key='review')

        queue_name = self.channel.queue_declare(queue='', durable=True).method.queue
        self.channel.queue_bind(exchange='data', queue=queue_name, routing_key='review.END')
        self.channel.basic_consume(queue=queue_name, on_message_callback=self.stop, auto_ack=True)

        queue_name = self.channel.queue_declare(queue='', durable=True).method.queue
        self.channel.queue_bind(exchange='data', queue=queue_name, routing_key='business.END')
        self.channel.basic_consume(queue=queue_name, on_message_callback=self.listen_reviews, auto_ack=True)

    def run(self):
        try:
            self.channel.start_consuming()
        except KeyboardInterrupt:
            self.channel.stop_consuming()
        finally:
            self.channel.close()
            self.connection.close()

    def route_business(self, ch, method, properties, body):
        business = json.loads(body)
        businessCities = [{'city':b['city'], 'business_id':b['business_id']} for b in business]
        ch.basic_publish(exchange='reviews', routing_key="business", body=json.dumps(businessCities))

    def listen_reviews(self, ch, method, props, body):
        map(self.route_business, self.channel.basic_cancel(self.businessTag))
        ch.basic_publish(exchange='reviews', routing_key="business.END", properties=props, body='')
        self.reviewsTag = self.channel.basic_consume(queue=self.reviewsQueue, on_message_callback=self.route_review, auto_ack=True)

    def route_review(self, ch, method, properties, body):
        reviews = json.loads(body)
        funny = [{'funny':r['funny'], 'business_id':r['business_id']} for r in reviews]
        comment = [{'text':r['text'], 'user_id':r['user_id']} for r in reviews]
        users = [{'user_id':r['user_id']} for r in reviews]
        stars5 = [{'stars':r['stars'], 'user_id':r['user_id']} for r in reviews]
        histogram = [{'date':r['date']} for r in reviews]
        ch.basic_publish(exchange='reviews', routing_key="users", body=json.dumps(users))
        ch.basic_publish(exchange='map', routing_key="comment", body=json.dumps(comment))
        ch.basic_publish(exchange='map', routing_key="funny", body=json.dumps(funny))
        ch.basic_publish(exchange='map', routing_key="stars5", body=json.dumps(stars5))
        ch.basic_publish(exchange='map', routing_key="histogram", body=json.dumps(histogram))

    def stop(self, ch, method, props, body):
        map(self.route_review, self.channel.basic_cancel(self.reviewsTag))
        self.channel.stop_consuming()
        self.channel.basic_publish(exchange='reviews', routing_key="users.END", properties=props, body='')
        self.channel.basic_publish(exchange='map', routing_key="comment.END", properties=props, body='')
        self.channel.basic_publish(exchange='map', routing_key="funny.END", properties=props, body='')
        self.channel.basic_publish(exchange='map', routing_key="stars5.END", properties=props, body='')
        self.channel.basic_publish(exchange='map', routing_key="histogram.END", properties=props, body='')

def main():
    Router().run()

if __name__ == '__main__':
    main()
