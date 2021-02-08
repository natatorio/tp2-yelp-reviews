from consumers import CounterBy


def main():
    counter = CounterBy(keyId="weekday", exchange="reviews", routing_key="histogram")
    histogram = counter.count()
    print("histogram", histogram, counter.reply_to)
    counter.reply(("histogram", histogram))
    counter.close()


if __name__ == "__main__":
    main()
