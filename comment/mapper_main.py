from mapper import CommentMapper
from health_server import HealthServer
import pipe


def main():
    healthServer = HealthServer()
    mapper = CommentMapper(pipe.map_comment(), pipe.consume_comment())
    while True:
        mapper.run()
    healthServer.stop()


if __name__ == "__main__":
    main()
