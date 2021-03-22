from health_server import HealthServer, get_my_ip
import pipe
import logging
from factory import joiner, use_value
from dedup import AggregatorDedup
from control_server import ControlClient

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

    with HealthServer():
        dedup_left = AggregatorDedup("comment_left")
        dedup_right = AggregatorDedup("comment_right")
        controlClient = ControlClient()
        control = pipe.pub_sub_control()
        for payload, ack in control.recv():
            left_name = None
            right_name = None
            if not (
                dedup_left.is_batch_processed(payload["session_id"])
                and dedup_right.is_batch_processed(payload["session_id"])
            ):
                logger.info("batch %s", payload)
                [left_name, right_name] = joiner(
                    pipe_left=pipe.comment_summary(),
                    left_fn=user_comment_counter,
                    pipe_right=pipe.user_count_5(),
                    right_fn=use_value,
                    join_fn=join,
                    pipe_out=pipe.reports(),
                    batch_id=payload["session_id"],
                    dedup_right=dedup_right,
                    dedup_left=dedup_left,
                )
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
            controlClient.batch_done(payload["session_id"], get_my_ip())
            ack()


if __name__ == "__main__":
    main()
