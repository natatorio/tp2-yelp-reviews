from consumers import JoinerCounterBy
from health_server import HealthServer


def main():
    healthServer = HealthServer()
    while True:
        querier = JoinerCounterBy(
            keyId="user_id",
            exchange="reviews",
            routing_key="stars5",
        )
        stars5ReviewsPerUser = querier.count()
        allStars5ReviewsPerRelevantUser = querier.join(stars5ReviewsPerUser)

        print(len(stars5ReviewsPerUser), " Users reviewing with 5 star")
        print(
            len(allStars5ReviewsPerRelevantUser),
            " Relevant Users reviewing all with 5 star",
        )
        querier.close()
    healthServer.stop()


if __name__ == "__main__":
    main()
