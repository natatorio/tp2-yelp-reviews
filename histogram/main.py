from consumers import CounterBy
from health_server import HealthServer

import pipe


def main():
    healthServer = HealthServer()
    counter = CounterBy(pipe.consume_histogram, [pipe.annon()], key_id="weekday")
    counter.run(lambda histogram: ("histogram", histogram))
    counter.close()
    healthServer.stop()


if __name__ == "__main__":
    main()
