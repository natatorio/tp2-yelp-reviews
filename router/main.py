import json
import os

import pika


# https://stackoverflow.com/questions/24510310/consume-multiple-queues-in-python-pika
class Router:
    def __init__(self):
        self.connection = pika.BlockingConnection(
            parameters=pika.URLParameters(url=os.environ["AMQP_URL"])
        )
        self.channel = self.connection.channel()
        self.channel.exchange_declare(exchange="data", exchange_type="direct")

        self.channel.exchange_declare(exchange="reviews", exchange_type="direct")
        self.business_queue = self.channel.queue_declare(
            queue="business.QUEUE", durable=True
        ).method.queue
        self.channel.queue_bind(
            exchange="data",
            queue=self.business_queue,
            routing_key="business",
        )
        self.reviews_queue = self.channel.queue_declare(
            queue="review.QUEUE", durable=True
        ).method.queue
        self.channel.queue_bind(
            exchange="data",
            queue=self.reviews_queue,
            routing_key="review",
        )

        self.channel.exchange_declare(exchange="map", exchange_type="direct")

        self.replicas = int(os.environ.get("N_REPLICAS", "1"))

    def consume_business(self):
        try:
            count_down = 1
            for method, props, body in self.channel.consume(
                self.business_queue, auto_ack=True
            ):
                payload = json.loads(body.decode("utf-8"))
                business = payload["data"]
                if business:
                    business_cities = [
                        {"city": b["city"], "business_id": b["business_id"]}
                        for b in business
                    ]
                    self.channel.basic_publish(
                        exchange="reviews",
                        routing_key="business",
                        properties=props,
                        body=json.dumps({"data": business_cities}),
                    )
                else:
                    count_down = payload.get("count_down", self.replicas)
                    break
        finally:
            self.channel.cancel()
        if count_down > 1:
            self.channel.basic_publish(
                exchange="data",
                routing_key="business",
                properties=props,
                body=json.dumps(
                    {
                        "data": None,
                        "count_down": count_down - 1,
                    }
                ),
            )
        else:
            self.channel.basic_publish(
                exchange="reviews",
                routing_key="business",
                properties=props,
                body=json.dumps({"data": None}),
            )

    def consume_reviews(self):
        for method, props, body in self.channel.consume(
            self.reviews_queue, auto_ack=True
        ):
            payload = json.loads(body.decode("utf-8"))
            reviews = payload["data"]
            if reviews:
                self.route_review(reviews, props)
            else:
                count_down = payload.get("count_down", self.replicas)
                if count_down > 1:
                    self.channel.basic_publish(
                        exchange="reviews",
                        routing_key="business",
                        properties=props,
                        body=json.dumps(
                            {
                                "data": None,
                                "count_down": count_down - 1,
                            }
                        ),
                    )
                else:
                    self.route_review(None, props)
                break
        self.channel.cancel()

    def route_review(self, reviews, props):
        funny = None
        comment = None
        users = None
        stars5 = None
        histogram = None
        if reviews:
            funny = [
                {"funny": r["funny"], "business_id": r["business_id"]} for r in reviews
            ]
            comment = [{"text": r["text"], "user_id": r["user_id"]} for r in reviews]
            users = [{"user_id": r["user_id"]} for r in reviews]
            stars5 = [{"stars": r["stars"], "user_id": r["user_id"]} for r in reviews]
            histogram = [{"date": r["date"]} for r in reviews]
        self.channel.basic_publish(
            exchange="reviews",
            routing_key="users",
            properties=props,
            body=json.dumps({"data": users}),
        )
        self.channel.basic_publish(
            exchange="map",
            routing_key="comment",
            properties=props,
            body=json.dumps({"data": comment}),
        )
        self.channel.basic_publish(
            exchange="map",
            routing_key="funny",
            properties=props,
            body=json.dumps({"data": funny}),
        )
        self.channel.basic_publish(
            exchange="map",
            routing_key="stars5",
            properties=props,
            body=json.dumps({"data": stars5}),
        )
        self.channel.basic_publish(
            exchange="map",
            routing_key="histogram",
            properties=props,
            body=json.dumps({"data": histogram}),
        )

    def run(self):
        try:
            print("consume_business")
            self.consume_business()
            print("consume_reviews")
            self.consume_reviews()
            print("done")
        finally:
            self.connection.close()


def main():
    Router().run()


if __name__ == "__main__":
    main()
