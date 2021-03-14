from filters import Filter, Mapper
from health_server import HealthServer
import pipe
import logging

logger = logging.getLogger(__name__)


def main():
    def map_stars(reviews):
        return [
            {"stars": r["stars"], "user_id": r["user_id"]}
            for r in reviews
            if r["stars"] == 5.0
        ]

    healthServer = HealthServer()
    consumer = Filter(pipe.map_stars5())
    mapper = Mapper(
        start_fn=lambda: None,
        map_fn=map_stars,
        pipe_out=pipe.star5_summary(),
    )
    try:
        consumer.run(mapper)
    except Exception as e:
        logger.exception("")
        raise e
    finally:
        consumer.close()
        mapper.close()
        healthServer.stop()


if __name__ == "__main__":
    main()
