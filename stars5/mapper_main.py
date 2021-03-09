from filters import Filter, MapperScatter
from health_server import HealthServer
import pipe


def main():
    def map_stars(reviews):
        return [
            {"stars": r["stars"], "user_id": r["user_id"]}
            for r in reviews
            if r["stars"] == 5.0
        ]

    healthServer = HealthServer()
    mapper = Filter(pipe.map_stars5())
    cursor = MapperScatter(
        start_fn=lambda: None,
        map_fn=map_stars,
        pipes_out=[pipe.consume_star5()],
    )
    mapper.run(cursor=cursor)
    healthServer.stop()


if __name__ == "__main__":
    main()
