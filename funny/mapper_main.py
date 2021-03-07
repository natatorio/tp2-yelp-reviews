from mapper import FunnyMapper, Pop
from health_server import HealthServer
import pipe
from consumers import Joiner


def main():
    healthServer = HealthServer()
    mapper = FunnyMapper(
        pipe.map_funny(), pipe.consume_funny(), Pop(pipe.map_funny_data())
    )
    mapper.run()
    healthServer.stop()


if __name__ == "__main__":
    main()
