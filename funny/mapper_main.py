from mapper import FunnyMapper, Pop
from health_server import HealthServer
import pipe


def main():
    healthServer = HealthServer()
    mapper = FunnyMapper(
        pipe.map_funny(), pipe.consume_funny(), Pop(pipe.map_funny_data())
    )
    while True:
        mapper.run()
    healthServer.stop()


if __name__ == "__main__":
    main()
