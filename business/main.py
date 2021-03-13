from filters import Filter, Reducer
from health_server import HealthServer
import pipe
import logging

logger = logging.getLogger(__name__)


def main():
    def build_business_city_dict(acc, data):
        for elem in data:
            acc[elem["business_id"]] = elem["city"]
        return acc

    healthServer = HealthServer()
    consumer = Filter(pipe.consume_business())
    mapper = Reducer(
        step_fn=build_business_city_dict,
        pipe_out=pipe.pub_funny_data(),
    )
    try:
        consumer.run(mapper)
    except Exception as e:
        logger.exception("")
        raise e
    finally:
        mapper.close()
        consumer.close()
        healthServer.stop()


if __name__ == "__main__":
    main()
