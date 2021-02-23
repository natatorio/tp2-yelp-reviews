from mapper import Stars5Mapper


def main():
    while True:
        mapper = Stars5Mapper("map", "reviews", "stars5")
        mapper.run()


if __name__ == "__main__":
    main()
