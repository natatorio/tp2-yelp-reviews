from consumers import BusinessConsumer


def main():
    bc = BusinessConsumer(exchange="reviews", routing_key="business")
    businessCities = bc.get_business_cities()
    print("forward")
    bc.forward("map", "funny", businessCities)
    print(len(businessCities), " Business Processed")
    bc.close()


if __name__ == "__main__":
    main()
