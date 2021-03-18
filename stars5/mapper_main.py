from health_server import HealthServer
import pipe
import logging
from factory import mapper

logger = logging.getLogger(__name__)


def main():
    def map_stars(reviews):
        return [
            {"stars": r["stars"], "user_id": r["user_id"]}
            for r in reviews
            if r["stars"] == 5.0
        ]

    with HealthServer():
        control = pipe.pub_sub_control()
        for payload, _ in control.recv(auto_ack=True):
            logger.info("batch %s", payload)
            mapper(
                pipe_in=pipe.map_stars5(),
                map_fn=map_stars,
                pipe_out=pipe.star5_summary(),
                logger=logger,
            )


if __name__ == "__main__":
    main()
