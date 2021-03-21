import hashlib
from health_server import HealthServer, get_my_ip
import pipe
import logging
from factory import mapper
from dedup import Dedup
from control_server import ControlClient

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
        dedup = Dedup("comment_mapper")
        controlClient = ControlClient()
        control = pipe.pub_sub_control()
        for payload, ack in control.recv():
            if not dedup.is_batch_processed(payload["session_id"]):
                logger.info("batch %s", payload)
                mapper(
                    pipe_in=pipe.map_comment(),
                    pipe_out=pipe.comment_summary(),
                    map_fn=map_user_text,
                    batch_id=payload["session_id"],
                    dedup=dedup,
                )
            controlClient.batch_done(payload["session_id"], get_my_ip())
            ack()


if __name__ == "__main__":
    main()
