from mapper import HistogramMapper


def main():
    while True:
        mapper = HistogramMapper("map", "reviews", "histogram")
        mapper.run()


if __name__ == "__main__":
    main()
