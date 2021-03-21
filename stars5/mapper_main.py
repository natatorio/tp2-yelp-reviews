from health_server import HealthServer, get_my_ip
import pipe
import logging
from factory import mapper
from dedup import Dedup
from control_server import ControlClient

logger = logging.getLogger(__name__)


def main():
    def map_stars(reviews):
        return [
            {"stars": r["stars"], "user_id": r["user_id"]}
            for r in reviews
            if r["stars"] == 5.0
        ]

    with HealthServer():
        dedup = Dedup("stars5_mapper")
        controlClient = ControlClient()
        control = pipe.pub_sub_control()
        for payload, ack in control.recv():
            if not dedup.is_batch_processed(payload["session_id"]):
                logger.info("batch %s", payload)
                mapper(
                    pipe_in=pipe.map_stars5(),
                    map_fn=map_stars,
                    pipe_out=pipe.star5_summary(),
                    batch_id=payload["session_id"],
                    dedup=dedup,
                )
            controlClient.batch_done(payload["session_id"], get_my_ip())
            ack()


if __name__ == "__main__":
    main()
