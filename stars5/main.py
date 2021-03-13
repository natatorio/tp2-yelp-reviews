from threading import Thread
from filters import Filter, Join, count_key, use_value
from health_server import HealthServer
import pipe
import logging

logger = logging.getLogger(__name__)


def main():
    def join(user_count, review_count):
        return (
            "stars5",
            {k: v for (k, v) in user_count.items() if v == review_count.get(k, 0)},
        )

    healthServer = HealthServer()
    left_consumer = Filter(pipe.consume_star5())
    right_consumer = Filter(pipe.consume_star5_data())
    joint = Join(join_fn=join, pipe_out=pipe.annon())
    left_mapper = joint.left(count_key("user_id"))

    def consume_right():
        right_mapper = joint.right(use_value)
        try:
            right_consumer.run(right_mapper)
        finally:
            right_mapper.close()
            right_consumer.close()

    try:
        thread = Thread(target=consume_right)
        thread.start()
        left_consumer.run(left_mapper)
        thread.join()
    except Exception as e:
        logger.exception("")
        raise e
    finally:
        left_mapper.close()
        left_consumer.close()
        healthServer.stop()


if __name__ == "__main__":
    main()
