from consumers import Joiner
from health_server import HealthServer
import pipe


def main():
    healthServer = HealthServer()
    joiner = Joiner(
        left_in=pipe.consume_comment(),
        right_in=pipe.consume_comment_data(),
        join_out=pipe.annon(),
    )

    def count(acc, data):
        for elem in data:
            acc[elem["user_id"]] = acc.get(elem["user_id"], 0) + 1
        return acc

    def aggregate(self, data):
        self.keyCount = self.get_state()
        for elem in data:
            commentCount = self.keyCount.get(elem[self.keyId])
            if commentCount and commentCount[0] == elem["text"]:
                self.keyCount[elem[self.keyId]] = (commentCount[0], commentCount[1] + 1)
            else:
                self.keyCount[elem[self.keyId]] = (elem["text"], 1)
        self.put_state(self.keyCount)

    def join(left, rigth):
        return {k: v[1] for (k, v) in rigth.items() if rigth[k][1] == left.get(k, 0)}

    joiner.run(,join)
    LastCommentCountPerUser = joiner.count()
    print("count ready")
    allSameCommentReviewsPerRelevantUser = joiner.join(LastCommentCountPerUser)
    print(len(LastCommentCountPerUser), " Users")
    print(
        len(allSameCommentReviewsPerRelevantUser),
        " Relevant users always commenting the same",
    )
    joiner.close()
    healthServer.stop()


if __name__ == "__main__":
    main()
