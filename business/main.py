from consumers import Scatter
from health_server import HealthServer
import pipe


def main():
    def aggregate(acc, data):
        for elem in data:
            acc[elem["business_id"]] = elem["city"]
        return acc

    healthServer = HealthServer()
    business_consumer = Scatter(
        pipe_in=pipe.consume_business(), pipes_out=[pipe.pub_funny_data()]
    )
    business_consumer.run(aggregate=aggregate)
    business_consumer.close()
    healthServer.stop()


if __name__ == "__main__":
    main()
