from mapper import FunnyMapper
from health_server import HealthServer
import pipe


def main():
    healthServer = HealthServer()
    mapper = FunnyMapper(
        pipe.map_funny(), pipe.consume_funny(), pipe.sub_map_funny_data()
    )
    mapper.run()
    healthServer.stop()


if __name__ == "__main__":
    main()
