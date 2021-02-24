from consumers import BusinessConsumer
from health_server import HealthServer


def main():
    healthServer = HealthServer()
    while True:
        bc = BusinessConsumer(exchange="reviews", routing_key="business")
        businessCities = bc.get_business_cities()
        print("forward")
        bc.forward("map", "funny", businessCities)
        print(len(businessCities), " Business Processed")
        bc.close()
    healthServer.stop()


if __name__ == "__main__":
    main()
