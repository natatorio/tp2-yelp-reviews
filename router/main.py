import sys
from filters import Filter, Mapper, Mapper
from threading import Thread
from health_server import HealthServer
import pipe
from pipe import Scatter
import logging

logger = logging.getLogger(__name__)


def consume_reviews():
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

    consumer = Filter(pipe_in=pipe.data_review())
    mapper = Mapper(
        start_fn=lambda: None,
        map_fn=lambda x: x,
        pipe_out=Scatter(
            [
                pipe.Formatted(pipe.consume_users(), users),
                pipe.Formatted(pipe.map_comment(), comment),
                pipe.Formatted(pipe.map_funny(), funny),
                pipe.Formatted(pipe.map_histogram(), histogram),
                pipe.Formatted(pipe.map_stars5(), stars5),
            ]
        ),
    )
    try:
        consumer.run(mapper)
    except Exception as e:
        logger.exception("")
        raise e
    finally:
        mapper.close()
        consumer.close()


def consume_business():
    consumer = Filter(pipe.data_business())

    def route_business(business):
        return [{"city": b["city"], "business_id": b["business_id"]} for b in business]

    mapper = Mapper(
        start_fn=lambda: None,
        map_fn=route_business,
        pipe_out=pipe.consume_business(),
    )
    try:
        consumer.run(mapper)
    except Exception as e:
        logger.exception("")
        raise e
    finally:
        mapper.close()
        consumer.close()


def main():
    healthServer = HealthServer()
    thread = Thread(target=consume_business)
    thread.start()
    consume_reviews()
    thread.join()
    healthServer.stop()


if __name__ == "__main__":
    main()
