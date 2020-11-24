import os
import time
import pika
import json

REVIEWS_DATASET_FILEPATH = "data/yelp_academic_dataset_review.json"
BUSINESS_DATASET_FILEPATH = "data/yelp_academic_dataset_business.json"
CHUNK_SIZE = 1 * 1024 * 1024
MAX_REVIEWS = 9000000000        #  8021122 Hasta 1 chunk de más
MAX_BUSINESS = 50000000           # 209393
QUERIES = 5

responses = []

def on_response(ch, method, properties, body):
    response = json.loads(body)
    print(response)
    responses.append(response)

def main():
    amqp_url = os.environ['AMQP_URL']
    parameters = pika.URLParameters(amqp_url)
    connection = pika.BlockingConnection(parameters)
    channel = connection.channel()
    channel.exchange_declare(exchange='data', exchange_type='direct')
    result = channel.queue_declare(queue='', exclusive=True)
    callback_queue = result.method.queue
    channel.basic_consume(queue=callback_queue, on_message_callback=on_response, auto_ack=True)
    time.sleep(10)

    with open(BUSINESS_DATASET_FILEPATH, 'r') as f:
        business_count = 0
        lines = f.readlines(CHUNK_SIZE)
        while lines and business_count < MAX_BUSINESS:
            message = ""
            business = [json.loads(line) for line in lines]
            business_count += len(business)
            message = json.dumps(business)
            channel.basic_publish(exchange='data', routing_key="business", body=message)
            lines = f.readlines(CHUNK_SIZE)
    channel.basic_publish(exchange='data', routing_key="business.END", body='')
    print(business_count, " Business Read")

    with open(REVIEWS_DATASET_FILEPATH, 'r') as f:
        review_count = 0
        lines = f.readlines(CHUNK_SIZE)
        while lines and review_count < MAX_REVIEWS:
            message = ""
            reviews = [json.loads(line) for line in lines]
            review_count += len(reviews)
            message = json.dumps(reviews)
            channel.basic_publish(exchange='data', routing_key="review", body=message)
            lines = f.readlines(CHUNK_SIZE)
    print(review_count, " Reviews Read")
    properties=pika.BasicProperties(reply_to=callback_queue,)
    channel.basic_publish(exchange='data', routing_key="review.END",properties=properties, body='')
    while len(responses) < QUERIES:
        # do other stuff...
        connection.process_data_events()
    channel.close()
    connection.close()


if __name__ == '__main__':
    main()
