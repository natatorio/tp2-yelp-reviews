from health_server import HealthServer, get_my_ip
import pipe
from pipe import Formatted
import logging
from factory import reducer, count_key
from dedup import AggregatorDedup
from control_server import ControlClient

logger = logging.getLogger(__name__)


def main():
    def topTenFunnyPerCity(funnyPerCity):
        return (
            "funny",
            {
                fun: city
                for (city, fun) in sorted(
                    funnyPerCity.items(),
                    key=lambda item: item[1],
                    reverse=True,
                )[:10]
            },
        )

    with HealthServer():
        dedup = AggregatorDedup("funny")
        controlClient = ControlClient()
        control = pipe.pub_sub_control()
        for payload, ack in control.recv():
            bucket_name = None
            if not dedup.is_batch_processed(payload["session_id"]):
                logger.info("batch %s", payload)
                bucket_name = reducer(
                    pipe_in=pipe.funny_summary(),
                    step_fn=count_key("city"),
                    pipe_out=Formatted(pipe.reports(), topTenFunnyPerCity),
                    batch_id=payload["session_id"],
                    dedup=dedup,
                )
            if bucket_name:
                dedup.db.log_drop(bucket_name, None)
                dedup.db.delete(
                    bucket_name,
                    "state",
                )
            controlClient.batch_done(payload["session_id"], get_my_ip())
            ack()


if __name__ == "__main__":
    main()
