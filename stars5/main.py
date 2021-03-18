from health_server import HealthServer
import pipe
import logging
from factory import joiner, use_value, count_key

logger = logging.getLogger(__name__)


def main():
    def join(user_count, review_count):
        return (
            "stars5",
            {k: v for (k, v) in user_count.items() if v == review_count.get(k, 0)},
        )

    with HealthServer():
        control = pipe.pub_sub_control()
        for payload, _ in control.recv(auto_ack=True):
            logger.info("batch %s", payload)
            joiner(
                pipe_left=pipe.star5_summary(),
                left_fn=count_key("user_id"),
                pipe_right=pipe.user_count_50(),
                right_fn=use_value,
                join_fn=join,
                pipe_out=pipe.reports(),
            )


if __name__ == "__main__":
    main()
