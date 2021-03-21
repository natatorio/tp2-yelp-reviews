from health_server import HealthServer, get_my_ip
import pipe
from pipe import Scatter
import logging
from factory import mapper
from dedup import Dedup
from control_server import ControlClient

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def consume_reviews(batch_id, dedup):
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
        batch_id=batch_id,
        dedup=dedup,
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




def consume_business(batch_id, dedup):
    def route_business(business):
        return [{"city": b["city"], "business_id": b["business_id"]} for b in business]

    mapper(
        pipe_in=pipe.data_business(),
        map_fn=route_business,
        pipe_out=pipe.business_cities_summary(),
        batch_id=batch_id,
        dedup=dedup,
    )


def main():
    with HealthServer():
        dedup = Dedup("router_bussiness")
        dedupBussiness = Dedup("router")
        controlClient = ControlClient()
        control = pipe.pub_sub_control()
        for payload, ack in control.recv():
            if not dedupBussiness.is_batch_processed(payload["session_id"]):
                consume_business(payload["session_id"], dedupBussiness)
                logger.info("batch %s", payload)
            if not dedup.is_batch_processed(payload["session_id"]):
                consume_reviews(payload["session_id"], dedup)
            controlClient.batch_done(session_id, get_my_ip())
            ack()


if __name__ == "__main__":
    main()
