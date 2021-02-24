from mapper import FunnyMapper
from health_server import HealthServer

def main():
    healthServer = HealthServer()
    while True:
        mapper = FunnyMapper("map", "reviews", "funny")
        mapper.run()
    healthServer.stop()


if __name__ == "__main__":
    main()
