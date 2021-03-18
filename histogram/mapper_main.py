from datetime import datetime
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

    control = pipe.pub_sub_control()
    for payload, _ in control.recv(auto_ack=True):
        logger.info("batch %s", payload)
        mapper(
            pipe_in=pipe.map_histogram(),
            map_fn=map_histogram,
            pipe_out=pipe.histogram_summary(),
            logger=logger,
        )


if __name__ == "__main__":
    main()
