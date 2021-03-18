from health_server import HealthServer
import pipe
from pipe import Scatter
import logging
from factory import mapper

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


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

    mapper(
        pipe_in=pipe.data_review(),
        map_fn=lambda x: x,
        pipe_out=Scatter(
            [
                pipe.Formatted(pipe.user_summary(), users),
                pipe.Formatted(pipe.map_comment(), comment),
                pipe.Formatted(pipe.map_funny(), funny),
                pipe.Formatted(pipe.map_histogram(), histogram),
                pipe.Formatted(pipe.map_stars5(), stars5),
            ]
        ),
    )


def consume_business():
    def route_business(business):
        return [{"city": b["city"], "business_id": b["business_id"]} for b in business]

    mapper(
        pipe_in=pipe.data_business(),
        map_fn=route_business,
        pipe_out=pipe.business_cities_summary(),
    )


def main():
    with HealthServer():
        control = pipe.pub_sub_control()
        for payload, _ in control.recv(auto_ack=True):
            logger.info("batch %s", payload)
            consume_business()
            consume_reviews()


if __name__ == "__main__":
    main()
