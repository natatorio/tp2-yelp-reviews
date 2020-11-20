import os
import pika
import json

def callback(ch, method, properties, body):
    ch.exchange_declare(exchange='reviews', exchange_type='direct')
    reviews = json.loads(body)
    for review in reviews:
        funny = json.dumps(review)
        print(funny, flush = True)
        # channel.basic_publish(exchange='reviews', routing_key="funny", body=)

def main():
    amqp_url = os.environ['AMQP_URL']
    parameters = pika.URLParameters(amqp_url)
    connection = pika.BlockingConnection(parameters)
    channel = connection.channel()
    channel.exchange_declare(exchange='data', exchange_type='direct')
    result = channel.queue_declare(queue='', durable=True)
    queue_name = result.method.queue
    channel.queue_bind(exchange='data', queue=queue_name, routing_key='review')
    channel.basic_consume(queue=queue_name, on_message_callback=callback, auto_ack=True)
    try:
        channel.start_consuming()
    except KeyboardInterrupt:
        connection.close()


if __name__ == '__main__':
    main()
