from filters import Filter, Reducer, count_key
from health_server import HealthServer

import pipe
from pipe import Formatted
import logging

logger = logging.getLogger(__name__)


def main():
    healthServer = HealthServer()
    counter = Filter(pipe.consume_histogram())
    reducer = Reducer(
        step_fn=count_key("weekday"),
        pipe_out=Formatted(
            pipe.annon(),
            lambda histogram: ("histogram", histogram),
        ),
    )
    try:
        counter.run(reducer)
    except Exception as e:
        logger.exception("")
        raise e
    finally:
        reducer.close()
        counter.close()
        healthServer.stop()


if __name__ == "__main__":
    main()
