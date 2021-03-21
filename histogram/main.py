from health_server import HealthServer, get_my_ip
import pipe
from pipe import Formatted
import logging
from factory import reducer, count_key
from dedup import AggregatorDedup
from control_server import ControlClient

logger = logging.getLogger(__name__)


def main():
    with HealthServer():
        dedup = AggregatorDedup("histogram")
        controlClient = ControlClient()
        control = pipe.pub_sub_control()
        for payload, ack in control.recv():
            if not dedup.is_batch_processed(payload["session_id"]):
                logger.info("batch %s", payload)
                reducer(
                    pipe_in=pipe.histogram_summary(),
                    step_fn=count_key("weekday"),
                    pipe_out=Formatted(
                        pipe.reports(),
                        lambda histogram: ("histogram", histogram),
                    ),
                    batch_id=payload["session_id"],
                    dedup=dedup,
                )
            controlClient.batch_done(payload["session_id"], get_my_ip())
            ack()


if __name__ == "__main__":
    main()
