from consumers import CounterBy


def main():
    counter = CounterBy(keyId="user_id", exchange="reviews", routing_key="users")
    user_count = counter.count()
    user_count_5 = dict([u for u in user_count.items() if u[1] >= 3])
    user_count_50 = dict([u for u in user_count_5.items() if u[1] >= 15])
    user_count_150 = dict([u for u in user_count_50.items() if u[1] >= 100])
    counter.forward("reviews", "stars5", user_count_50)
    counter.forward("reviews", "comment", user_count_5)

    print(len(user_count_50), " Users with more than 50 reviews")
    counter.reply(("user_150", user_count_150))
    # counter.reply(("user_50", user_count_50))
    counter.close()


if __name__ == "__main__":
    main()
