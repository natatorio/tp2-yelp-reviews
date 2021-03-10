from filters import Filter, Persistent, ReducerScatter
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

    def aggregate(acc, data):
        for elem in data:
            acc[elem["city"]] = acc.get(elem["city"], 0) + 1
        return acc

    healthServer = HealthServer()
    consumer = Filter(pipe_in=pipe.consume_funny())
    consumer.run(
        Persistent(
            name="funny",
            cursor=ReducerScatter(
                step_fn=aggregate,
                pipes_out=[Formatted(pipe.annon(), topTenFunnyPerCity)],
            ),
        )
    )
    consumer.close()
    healthServer.stop()


if __name__ == "__main__":
    main()
