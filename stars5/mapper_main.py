from mapper import Stars5Mapper
from health_server import HealthServer
import pipe


def main():
    healthServer = HealthServer()
    mapper = Stars5Mapper(pipe.map_stars5(), pipe.consume_star5())
    while True:
        mapper.run()
    healthServer.stop()


if __name__ == "__main__":
    main()
