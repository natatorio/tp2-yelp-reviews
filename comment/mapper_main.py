import hashlib
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

    control = pipe.pub_sub_control()
    for payload, _ in control.recv(auto_ack=True):
        logger.info("batch %s", payload)
        mapper(
            pipe_in=pipe.map_comment(),
            pipe_out=pipe.comment_summary(),
            map_fn=map_user_text,
            logger=logger,
        )


if __name__ == "__main__":
    main()
