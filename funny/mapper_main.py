import sys
from threading import Event, Thread
from filters import Filter, MapperScatter, Notify
from health_server import HealthServer
import pipe


class Coordination:
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
    try:
        business_cities = Filter(pipe.sub_map_funny_data())
        business_cities.run(Notify(observer=coordination.on_business_done))
        business_cities.close()
    except:
        print("ERROR")
        sys.exit(1)


def main():
    healthServer = HealthServer()
    refiner = Filter(pipe.map_funny())
    coordination = Coordination()
    thread = Thread(target=consume_business, args=[coordination])
    thread.start()
    try:
        refiner.run(
            cursor=MapperScatter(
                map_fn=coordination.map_business,
                start_fn=coordination.prepare,
                pipes_out=[pipe.consume_funny()],
            )
        )
    except:
        sys.exit(1)
    thread.join()
    healthServer.stop()


if __name__ == "__main__":
    main()
