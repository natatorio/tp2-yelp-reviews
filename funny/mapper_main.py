from health_server import HealthServer, get_my_ip
import pipe
import logging
from factory import mapper, sink
from dedup import Dedup
from control_server import ControlClient

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def main():
    def funny(business_city, batch_id, dedup):
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
        dedup = Dedup(get_my_ip())
        controlClient = ControlClient()
        dedupBusiness = Dedup(get_my_ip() + "_business")
        control = pipe.pub_sub_control()
        for payload, ack in control.recv():
            if not dedup.is_batch_processed(payload["session_id"]):
                logger.info("batch %s", payload)
                sink(
                    pipe_in=pipe.sub_funny_business_cities(),
                    observer=lambda business: funny(
                        business_city=business,
                        batch_id=payload["session_id"],
                        dedup=dedup,
                    ),
                    batch_id=payload["session_id"],
                    dedup=dedupBusiness,
                )
            # elif not dedup.is_batch_processed(payload["session_id"]):
            #     funny(
            #         business_city=business,
            #         batch_id=payload["session_id"],
            #         dedup=dedup,
            #     )
            bucket_name = get_my_ip()
            dedup.db.log_drop(bucket_name + "_sink", None)
            dedup.db.log_drop(bucket_name + "_processed", None)
            dedup.db.delete(
                bucket_name + "_sink",
                "state",
            )
            controlClient.batch_done(payload["session_id"], bucket_name)
            ack()


if __name__ == "__main__":
    main()
