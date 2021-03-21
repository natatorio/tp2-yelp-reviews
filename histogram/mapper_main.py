from datetime import datetime
from health_server import HealthServer, get_my_ip
import pipe
import logging
from factory import mapper
from dedup import Dedup
from control_server import ControlClient

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
        dedup = Dedup("histogram_mapper")
        controlClient = ControlClient()
        control = pipe.pub_sub_control()
        for payload, ack in control.recv():
            if not dedup.is_batch_processed(payload["session_id"]):
                logger.info("batch %s", payload)
                mapper(
                    pipe_in=pipe.map_histogram(),
                    map_fn=map_histogram,
                    pipe_out=pipe.histogram_summary(),
                    batch_id=payload["session_id"],
                    dedup=dedup,
                )
            controlClient.batch_done(session_id, get_my_ip())
            ack()


if __name__ == "__main__":
    main()
