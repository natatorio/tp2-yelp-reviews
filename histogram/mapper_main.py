from datetime import datetime
from filters import Filter, MapperScatter
from health_server import HealthServer
import pipe


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
    mapper = Filter(pipe.map_histogram())
    cursor = MapperScatter(
        start_fn=lambda: None,
        map_fn=map_histogram,
        pipes_out=[pipe.consume_histogram()],
    )
    mapper.run(cursor=cursor)
    healthServer.stop()


if __name__ == "__main__":
    main()
