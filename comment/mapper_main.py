import hashlib
from health_server import HealthServer
import pipe
import logging
from factory import mapper

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

    with HealthServer():
        control = pipe.pub_sub_control()
        for payload, ack in control.recv():
            logger.info("batch %s", payload)
            mapper(
                pipe_in=pipe.map_comment(),
                pipe_out=pipe.comment_summary(),
                map_fn=map_user_text,
                batch_id=payload["session_id"],
            )
            ack()


if __name__ == "__main__":
    main()
