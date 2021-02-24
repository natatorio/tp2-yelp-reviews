from mapper import CommentMapper
from health_server import HealthServer


def main():
    healthServer = HealthServer()
    while True:
        mapper = CommentMapper("map", "reviews", "comment")
        mapper.run()
    healthServer.stop()


if __name__ == "__main__":
    main()
