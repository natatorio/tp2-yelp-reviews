from consumers import CommentQuerier
from health_server import HealthServer


def main():
    healthServer = HealthServer()
    while True:
        querier = CommentQuerier(keyId="user_id", exchange="reviews", routing_key="comment")
        LastCommentCountPerUser = querier.count()
        allSameCommentReviewsPerRelevantUser = querier.join(LastCommentCountPerUser)

        print(len(LastCommentCountPerUser), " Users")
        print(
            len(allSameCommentReviewsPerRelevantUser),
            " Relevant users always commenting the same",
        )
        querier.close()
    healthServer.stop()


if __name__ == "__main__":
    main()
