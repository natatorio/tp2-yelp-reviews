import os
import pika



def main():
    amqp_url = os.environ['AMQP_URL']
    parameters = pika.URLParameters(amqp_url)
    connection = pika.BlockingConnection(parameters)
    channel = connection.channel()
    channel.exchange_declare(exchange='reviews', exchange_type='direct')
    for _ in range(5):
        channel.basic_publish(exchange='reviews', routing_key="line", body="message")
    connection.close()


if __name__ == '__main__':
    main()
