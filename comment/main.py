from threading import Thread
from filters import Filter, Join, use_value
from health_server import HealthServer
import pipe
import logging

logger = logging.getLogger(__name__)


def main():
    def user_comment_counter(key_count, data):
        for elem in data:
            commentCount = key_count.get(elem["user_id"])
            if commentCount and commentCount[0] == elem["text"]:
                key_count[elem["user_id"]] = (commentCount[0], commentCount[1] + 1)
            else:
                key_count[elem["user_id"]] = (elem["text"], 1)
        return key_count

    def join(user_comment_count, review_count):
        return (
            "comment",
            {
                k: v[1]
                for (k, v) in user_comment_count.items()
                if user_comment_count[k][1] == review_count.get(k, 0)
            },
        )

    healthServer = HealthServer()
    left_consumer = Filter(pipe.consume_comment())
    right_consumer = Filter(pipe.consume_comment_data())
    joint = Join(join_fn=join, pipe_out=pipe.annon())

    def consume_left():
        left_mapper = joint.left(user_comment_counter)
        try:
            left_consumer.run(cursor=left_mapper)
        finally:
            left_mapper.close()
            left_consumer.close()

    right_mapper = joint.right(use_value)
    try:
        thread = Thread(target=consume_left)
        thread.start()
        right_consumer.run(right_mapper)
        thread.join()
    except Exception as e:
        logger.exception("")
        raise e
    finally:
        right_mapper.close()
        right_consumer.close()
        healthServer.stop()


if __name__ == "__main__":
    main()
