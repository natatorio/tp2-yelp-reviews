from mapper import HistogramMapper
from health_server import HealthServer
import pipe


def main():
    healthServer = HealthServer()
    mapper = HistogramMapper(pipe.map_histogram(), pipe.consume_histogram())
    while True:
        mapper.run()
    healthServer.stop()


if __name__ == "__main__":
    main()
