from mapper import FunnyMapper


def main():
    while True:
        mapper = FunnyMapper("map", "reviews", "funny")
        mapper.run()


if __name__ == "__main__":
    main()
