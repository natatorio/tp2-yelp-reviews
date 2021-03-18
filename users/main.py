import pipe
from pipe import Send
import logging
from factory import reducer, count_key

logger = logging.getLogger(__name__)


class UserSend(Send):
    def __init__(self) -> None:
        self.comment = pipe.user_count_5()
        self.stars5 = pipe.user_count_50()
        self.report = pipe.reports()

    def send(self, payload):
        user_count = payload.pop("data", None)
        user_count_5 = None
        user_count_50 = None
        user_count_150 = None
        if user_count is not None:
            user_count_5 = dict([u for u in user_count.items() if u[1] >= 3])
            user_count_50 = dict([u for u in user_count_5.items() if u[1] >= 15])
            user_count_150 = (
                "users_150",
                dict([u for u in user_count_50.items() if u[1] >= 100]),
            )
        self.comment.send({**payload, "data": user_count_5})
        self.stars5.send({**payload, "data": user_count_50})
        self.report.send({**payload, "data": user_count_150})

    def close(self):
        self.comment.close()
        self.stars5.close()
        self.report.close()


def main():
    control = pipe.pub_sub_control()
    for payload, _ in control.recv(auto_ack=True):
        logger.info("batch %s", payload)
        reducer(
            pipe_in=pipe.user_summary(),
            step_fn=count_key("user_id"),
            pipe_out=UserSend(),
            logger=logger,
        ),


if __name__ == "__main__":
    main()
