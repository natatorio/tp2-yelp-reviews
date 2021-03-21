from health_server import HealthServer, get_my_ip
import pipe
import logging
from factory import reducer
from dedup import AggregatorDedup
from control_server import ControlClient

logger = logging.getLogger(__name__)


def main():
    def build_business_city_dict(acc, data):
        for elem in data:
            acc[elem["business_id"]] = elem["city"]
        return acc

    with HealthServer():
        dedup = AggregatorDedup("business")
        controlClient = ControlClient()
        control = pipe.pub_sub_control()
        for payload, ack in control.recv():
            if not dedup.is_batch_processed(payload["session_id"]):
                logger.info("batch %s", payload)
                reducer(
                    pipe_in=pipe.business_cities_summary(),
                    pipe_out=pipe.pub_funny_business_cities(),
                    step_fn=build_business_city_dict,
                    batch_id=payload["session_id"],
                    dedup=dedup,
                )
            controlClient.batch_done(payload["session_id"], get_my_ip())
            ack()


if __name__ == "__main__":
    main()
