from mapper import CommentMapper


def main():
    while True:
        mapper = CommentMapper("map", "reviews", "comment")
        mapper.run()


if __name__ == "__main__":
    main()
