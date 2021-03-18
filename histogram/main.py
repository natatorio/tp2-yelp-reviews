import pipe
from pipe import Formatted
import logging
from factory import reducer, count_key

logger = logging.getLogger(__name__)


def main():
    control = pipe.pub_sub_control()
    for payload, _ in control.recv(auto_ack=True):
        logger.info("batch %s", payload)
        reducer(
            pipe_in=pipe.histogram_summary(),
            step_fn=count_key("weekday"),
            pipe_out=Formatted(
                pipe.reports(),
                lambda histogram: ("histogram", histogram),
            ),
            logger=logger,
        )


if __name__ == "__main__":
    main()
