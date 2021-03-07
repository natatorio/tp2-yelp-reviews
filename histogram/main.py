from consumers import CounterBy
from health_server import HealthServer

import pipe
from pipe import Formatted


def main():
    healthServer = HealthServer()
    counter = CounterBy(
        pipe.consume_histogram(),
        [Formatted(pipe.annon(), lambda histogram: ("histogram", histogram))],
        key_id="weekday",
    )
    counter.run()
    counter.close()
    healthServer.stop()


if __name__ == "__main__":
    main()
