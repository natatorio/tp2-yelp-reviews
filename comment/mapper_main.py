import hashlib
from health_server import HealthServer
import pipe
from filters import Filter, Mapper
import logging

logger = logging.getLogger(__name__)


def main():
    def map_user_text(reviews):
        return [
            {
                "text": hashlib.sha1(r["text"].encode()).hexdigest(),
                "user_id": r["user_id"],
            }
            for r in reviews
        ]

    consumer = Filter(pipe.map_comment())
    healthServer = HealthServer()
    mapper = Mapper(
        start_fn=lambda: None,
        map_fn=map_user_text,
        pipe_out=pipe.consume_comment(),
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
