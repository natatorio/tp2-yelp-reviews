from datetime import datetime
from health_server import HealthServer
import pipe
import logging
from factory import mapper

logger = logging.getLogger(__name__)


def main():
    def map_histogram(dates):
        return [
            {
                "weekday": datetime.strptime(d["date"], "%Y-%m-%d %H:%M:%S").strftime(
                    "%A"
                )
            }
            for d in dates
        ]

    with HealthServer():
        control = pipe.pub_sub_control()
        for payload, ack in control.recv():
            logger.info("batch %s", payload)
            mapper(
                pipe_in=pipe.map_histogram(),
                map_fn=map_histogram,
                pipe_out=pipe.histogram_summary(),
                batch_id=payload["session_id"],
            )
            ack()


if __name__ == "__main__":
    main()
