from health_server import HealthServer
import pipe
import logging
from factory import reducer

logger = logging.getLogger(__name__)


def main():
    def build_business_city_dict(acc, data):
        for elem in data:
            acc[elem["business_id"]] = elem["city"]
        return acc

    with HealthServer():
        control = pipe.pub_sub_control()
        for payload, ack in control.recv():
            logger.info("batch %s", payload)
            reducer(
                pipe_in=pipe.business_cities_summary(),
                pipe_out=pipe.pub_funny_business_cities(),
                step_fn=build_business_city_dict,
                batch_id=payload["session_id"],
            )
            ack()


if __name__ == "__main__":
    main()
