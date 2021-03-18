from health_server import HealthServer
import pipe
from pipe import Formatted
import logging
from factory import reducer, count_key

logger = logging.getLogger(__name__)


def main():
    with HealthServer():
        control = pipe.pub_sub_control()
        for payload, ack in control.recv():
            logger.info("batch %s", payload)
            reducer(
                pipe_in=pipe.histogram_summary(),
                step_fn=count_key("weekday"),
                pipe_out=Formatted(
                    pipe.reports(),
                    lambda histogram: ("histogram", histogram),
                ),
                batch_id=payload["session_id"],
            )
            ack()


if __name__ == "__main__":
    main()
