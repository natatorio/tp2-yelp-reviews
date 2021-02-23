from consumers import CounterBy


def main():
    while True:
        querier = CounterBy(keyId="city", exchange="reviews", routing_key="funny")
        funnyPerCity = querier.count()
        topTenFunnyPerCity = {
            fun: city
            for (city, fun) in sorted(
                funnyPerCity.items(),
                key=lambda item: item[1],
                reverse=True,
            )[:10]
        }

        print(len(funnyPerCity), " Funny Cities")
        querier.reply(("funny", topTenFunnyPerCity))
        querier.close()


if __name__ == "__main__":
    main()
