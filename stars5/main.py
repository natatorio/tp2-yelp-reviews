from health_server import HealthServer, get_my_ip
import pipe
import logging
from factory import joiner, use_value, count_key
from dedup import AggregatorDedup
from control_server import ControlClient

logger = logging.getLogger(__name__)


def main():
    def join(user_count, review_count):
        return (
            "stars5",
            {k: v for (k, v) in user_count.items() if v == review_count.get(k, 0)},
        )

    with HealthServer():
        dedup_left = AggregatorDedup("stars5_left")
        dedup_right = AggregatorDedup("stars5_right")
        controlClient = ControlClient()
        control = pipe.pub_sub_control()
        for payload, ack in control.recv():
            if not (
                dedup_left.is_batch_processed(payload["session_id"])
                and dedup_right.is_batch_processed(payload["session_id"])
            ):
                logger.info("batch %s", payload)
                joiner(
                    pipe_left=pipe.star5_summary(),
                    left_fn=count_key("user_id"),
                    pipe_right=pipe.user_count_50(),
                    right_fn=use_value,
                    join_fn=join,
                    pipe_out=pipe.reports(),
                    batch_id=payload["session_id"],
                    dedup_left=dedup_left,
                    dedup_right=dedup_right,
                )
            bucket_name = get_my_ip()
            left_name = bucket_name + "_left"
            right_name = bucket_name + "_right"
            if left_name or right_name:
                dedup_left.db.log_drop(left_name + "_processed", None)
                dedup_left.db.log_drop(left_name, None)
                dedup_left.db.delete(
                    left_name,
                    "state",
                )
                dedup_right.db.log_drop(right_name + "_processed", None)
                dedup_right.db.log_drop(right_name, None)
                dedup_right.db.delete(
                    right_name,
                    "state",
                )
            controlClient.batch_done(payload["session_id"], bucket_name)
            ack()


if __name__ == "__main__":
    main()
