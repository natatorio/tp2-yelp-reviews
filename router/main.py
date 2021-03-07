from threading import Thread
from health_server import HealthServer
import pipe
from mapper import Mapper

# https://stackoverflow.com/questions/24510310/consume-multiple-queues-in-python-pika
class Router:
    def __init__(self):
        self.business_mapper = Mapper(pipe.data_business(), [pipe.consume_business()])

        def funny(reviews):
            return [
                {
                    "funny": r["funny"],
                    "business_id": r["business_id"],
                }
                for r in reviews
            ]

        def comment(reviews):
            return [{"text": r["text"], "user_id": r["user_id"]} for r in reviews]

        def users(reviews):
            return [{"user_id": r["user_id"]} for r in reviews]

        def stars5(reviews):
            return [{"stars": r["stars"], "user_id": r["user_id"]} for r in reviews]

        def histogram(reviews):
            return [{"date": r["date"]} for r in reviews]

        self.reviews_scatter = Mapper(
            pipe.data_review(),
            [
                pipe.Formatted(pipe.consume_users(), users),
                pipe.Formatted(pipe.map_comment(), comment),
                pipe.Formatted(pipe.map_funny(), funny),
                pipe.Formatted(pipe.map_histogram(), histogram),
                pipe.Formatted(pipe.map_stars5(), stars5),
            ],
        )

    def consume_business(self):
        def route_business(business):
            return [
                {"city": b["city"], "business_id": b["business_id"]} for b in business
            ]

        self.business_mapper.run(map_fn=route_business)

    def consume_reviews(self):
        self.reviews_scatter.run(lambda x: x)

    def run(self):
        print("consume_business")
        thread = Thread(target=self.consume_business)
        thread.start()
        print("consume_reviews")
        self.consume_reviews()
        thread.join()
        print("done")


def main():
    healthServer = HealthServer()
    Router().run()
    healthServer.stop()


if __name__ == "__main__":
    main()
