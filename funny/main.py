from health_server import HealthServer
import pipe
from pipe import Formatted
import logging
from factory import reducer, count_key

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

    with HealthServer():
        control = pipe.pub_sub_control()
        for payload, _ in control.recv(auto_ack=True):
            logger.info("batch %s", payload)
            reducer(
                pipe_in=pipe.funny_summary(),
                step_fn=count_key("city"),
                pipe_out=Formatted(pipe.reports(), topTenFunnyPerCity),
                logger=logger,
            )


if __name__ == "__main__":
    main()
