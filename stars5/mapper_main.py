from mapper import Stars5Mapper
from health_server import HealthServer

def main():
    healthServer = HealthServer()
    while True:
        mapper = Stars5Mapper("map", "reviews", "stars5")
        mapper.run()
    healthServer.stop()


if __name__ == "__main__":
    main()
