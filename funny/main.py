from kevasto import Client
from filters import Filter, Persistent, Reducer, count_key
from health_server import HealthServer

import pipe
from pipe import Formatted
import logging

logger = logging.getLogger(__name__)


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
    consumer = Filter(pipe_in=pipe.funny_summary())
    reducer = Persistent(
        cursor=Reducer(
            step_fn=count_key("city"),
            pipe_out=Formatted(pipe.reports(), topTenFunnyPerCity),
        ),
        name="funny",
        client=Client(),
    )
    try:
        consumer.run(reducer)
    except Exception as e:
        logger.exception("")
        raise e
    finally:
        reducer.close()
        consumer.close()
        healthServer.stop()


if __name__ == "__main__":
    main()
