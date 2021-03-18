from health_server import HealthServer
import pipe
import logging
from factory import mapper, sink

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def main():
    def funny(business_city):
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
        )

    with HealthServer():
        control = pipe.pub_sub_control()
        for payload, _ in control.recv(auto_ack=True):
            logger.info("batch %s", payload)
            sink(
                pipe_in=pipe.sub_funny_business_cities(),
                observer=funny,
            )


if __name__ == "__main__":
    main()
