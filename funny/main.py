from consumers import CounterBy
from health_server import HealthServer

import pipe
from pipe import Formatted


def main():
    def topTenFunnyPerCity(funnyPerCity):
        return (
            "funny",
            {
                fun: city
                for (city, fun) in sorted(
                    funnyPerCity.items(),
                    key=lambda item: item[1],
                    reverse=True,
                )[:10]
            },
        )

    healthServer = HealthServer()
    querier = CounterBy(
        pipe.consume_funny(),
        [Formatted(pipe.annon(), topTenFunnyPerCity)],
        key_id="city",
    )
    querier.run()
    healthServer.stop()


if __name__ == "__main__":
    main()
