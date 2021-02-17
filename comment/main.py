from consumers import CommentQuerier


def main():
    querier = CommentQuerier(keyId="user_id", exchange="reviews", routing_key="comment")
    LastCommentCountPerUser = querier.count()
    allSameCommentReviewsPerRelevantUser = querier.join(LastCommentCountPerUser)

    print(len(LastCommentCountPerUser), " Users")
    print(
        len(allSameCommentReviewsPerRelevantUser),
        " Relevant users always commenting the same",
    )
    querier.reply(("sameComment", allSameCommentReviewsPerRelevantUser))
    querier.close()


if __name__ == "__main__":
    main()
