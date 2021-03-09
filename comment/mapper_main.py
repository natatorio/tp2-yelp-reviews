import hashlib
from health_server import HealthServer
import pipe
from filters import Filter, MapperScatter


def main():
    def map_user_text(reviews):
        return [
            {
                "text": hashlib.sha1(r["text"].encode()).hexdigest(),
                "user_id": r["user_id"],
            }
            for r in reviews
        ]

    healthServer = HealthServer()
    cursor = MapperScatter(
        start_fn=lambda: None,
        map_fn=map_user_text,
        pipes_out=[pipe.consume_comment()],
    )
    mapper = Filter(pipe.map_comment())
    mapper.run(cursor=cursor)
    healthServer.stop()


if __name__ == "__main__":
    main()
