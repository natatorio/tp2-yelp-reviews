import os
import time
import pika
import json

REVIEWS_DATASET_FILEPATH = "data/yelp_academic_dataset_review.json"
BUSINESS_DATASET_FILEPATH = "data/yelp_academic_dataset_business.json"
CHUNK_SIZE = 4 * 1024
MAX_REVIEWS = 6

def main():
    amqp_url = os.environ['AMQP_URL']
    parameters = pika.URLParameters(amqp_url)
    connection = pika.BlockingConnection(parameters)
    channel = connection.channel()
    channel.exchange_declare(exchange='data', exchange_type='direct')
    with open(REVIEWS_DATASET_FILEPATH, 'r') as f:
        review_count = 0
        lines = f.readlines(CHUNK_SIZE)
        while lines and review_count < MAX_REVIEWS:
            message = ""
            lines = f.readlines(CHUNK_SIZE)
            reviews = [json.loads(line) for line in lines]
            review_count += len(reviews)
            message = json.dumps(reviews)
            channel.basic_publish(exchange='data', routing_key="review", body=message)
    time.sleep(5)
    connection.close()


if __name__ == '__main__':
    main()
