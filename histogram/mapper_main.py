from mapper import HistogramMapper
from health_server import HealthServer


def main():
    healthServer = HealthServer()
    while True:
        mapper = HistogramMapper("map", "reviews", "histogram")
        mapper.run()
    healthServer.stop()


if __name__ == "__main__":
    main()
