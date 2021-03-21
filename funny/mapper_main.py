from health_server import HealthServer, get_my_ip
import pipe
import logging
from factory import mapper, sink
from dedup import Dedup
from control_server import ControlClient

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def main():
    def funny(business_city, batch_id):
        logger.info("start mapping funny business")

        def map_business(reviews):
            return [
                {"city": business_city.get(r["business_id"], "Unknown")}
                for r in reviews
                if r["funny"] != 0
            ]

        mapper(
            pipe_in=pipe.map_funny(),
            map_fn=map_business,
            pipe_out=pipe.funny_summary(),
            batch_id=batch_id,
            dedup=dedup,
        )

    with HealthServer():
        dedup = Dedup("funny_mapper")
        controlClient = ControlClient()
        dedupBussiness = Dedup("funny_mapper_bussiness")
        control = pipe.pub_sub_control()
        for payload, ack in control.recv():
            if not dedup.is_batch_processed(payload["session_id"]):
                logger.info("batch %s", payload)
                sink(
                    pipe_in=pipe.sub_funny_business_cities(),
                    observer=lambda business: funny(
                        business_city=business, batch_id=payload["session_id"]
                    ),
                    batch_id=payload["session_id"],
                    dedup=dedupBussiness,
                )
            controlClient.batch_done(session_id, get_my_ip())
            ack()


if __name__ == "__main__":
    main()
