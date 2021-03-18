from health_server import HealthServer
import pipe
import logging
from factory import joiner, use_value

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
        control = pipe.pub_sub_control()
        for payload, ack in control.recv():
            logger.info("batch %s", payload)
            joiner(
                pipe_left=pipe.comment_summary(),
                left_fn=user_comment_counter,
                pipe_right=pipe.user_count_5(),
                right_fn=use_value,
                join_fn=join,
                pipe_out=pipe.reports(),
                batch_id=payload["session_id"],
            )
            ack()


if __name__ == "__main__":
    main()
