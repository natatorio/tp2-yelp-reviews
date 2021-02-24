from consumers import CounterBy
from health_server import HealthServer


def main():
    healthServer = HealthServer()
    while True:
        counter = CounterBy(keyId="weekday", exchange="reviews", routing_key="histogram")
        histogram = counter.count()
        print("histogram", histogram, counter.reply_to)
        counter.reply(("histogram", histogram))
        counter.close()
    healthServer.stop()


if __name__ == "__main__":
    main()
