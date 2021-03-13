from threading import Event, Thread
from filters import Filter, Mapper, Notify
from health_server import HealthServer
import pipe
import logging

logger = logging.getLogger(__name__)


class BusinessEvent:
    def __init__(self):
        self.business_done = Event()
        self.business_taken = Event()

    def prepare(self):
        print("waiting for business")
        self.business_done.wait()
        self.business_done.clear()
        self.business_taken.set()
        print("waiting for business")

    def on_business_done(self, data):
        print("business done")
        self.business_city = data
        self.business_done.set()
        self.business_taken.wait()
        self.business_taken.clear()
        print("busines done restart")

    def map_business(self, reviews):
        return [
            {"city": self.business_city.get(r["business_id"], "Unknown")}
            for r in reviews
            if r["funny"] != 0
        ]


def consume_business(coordination):
    business_cities = Filter(pipe.sub_map_funny_data())
    try:
        business_cities.run(Notify(observer=coordination.on_business_done))
    finally:
        business_cities.close()


def main():
    healthServer = HealthServer()
    consumer = Filter(pipe.map_funny())
    coordination = BusinessEvent()
    mapper = Mapper(
        map_fn=coordination.map_business,
        start_fn=coordination.prepare,
        pipe_out=pipe.consume_funny(),
    )
    try:
        thread = Thread(target=consume_business, args=[coordination])
        thread.start()
        consumer.run(mapper)
        thread.join()
    except Exception as e:
        logger.exception("")
        raise e
    finally:
        mapper.close()
        consumer.close()
        healthServer.stop()


if __name__ == "__main__":
    main()
