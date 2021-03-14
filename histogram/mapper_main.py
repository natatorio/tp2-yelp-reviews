from datetime import datetime
from filters import Filter, Mapper
from health_server import HealthServer
import pipe
import logging

logger = logging.getLogger(__name__)


def main():
    def map_histogram(dates):
        return [
            {
                "weekday": datetime.strptime(d["date"], "%Y-%m-%d %H:%M:%S").strftime(
                    "%A"
                )
            }
            for d in dates
        ]

    healthServer = HealthServer()
    consumer = Filter(pipe.map_histogram())
    mapper = Mapper(
        start_fn=lambda: None,
        map_fn=map_histogram,
        pipe_out=pipe.histogram_summary(),
    )
    try:
        consumer.run(mapper)
    except Exception as e:
        logger.exception("")
        raise e
    finally:
        consumer.close()
        mapper.close()
        healthServer.stop()


if __name__ == "__main__":
    main()
