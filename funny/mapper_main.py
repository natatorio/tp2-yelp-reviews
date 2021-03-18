from health_server import HealthServer
import pipe
import logging
from factory import mapper, sink

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def main():
    def funny(business_city, batch_id):
        logger.info("start mapping funny business")

        def map_business(reviews):
            return [
                {"city": business_city.get(r["business_id"], "Unknown")}
                for r in reviews
                if r["funny"] != 0
            ]

        mapper(
            pipe_in=pipe.map_funny(),
            map_fn=map_business,
            pipe_out=pipe.funny_summary(),
            batch_id=batch_id,
        )

    with HealthServer():
        control = pipe.pub_sub_control()
        for payload, ack in control.recv():
            logger.info("batch %s", payload)
            sink(
                pipe_in=pipe.sub_funny_business_cities(),
                observer=lambda business: funny(
                    business_city=business, batch_id=payload["session_id"]
                ),
                batch_id=payload["session_id"],
            )
            ack()


if __name__ == "__main__":
    main()
