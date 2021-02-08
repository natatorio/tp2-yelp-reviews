from mapper import CommentMapper


def main():
    mapper = CommentMapper("map", "reviews", "comment")
    mapper.run()


if __name__ == "__main__":
    main()
