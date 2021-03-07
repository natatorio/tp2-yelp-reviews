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

    def aggregate(key_count, data):
        for elem in data:
            commentCount = key_count.get(elem["user_id"])
            if commentCount and commentCount[0] == elem["text"]:
                key_count[elem["user_id"]] = (commentCount[0], commentCount[1] + 1)
            else:
                key_count[elem["user_id"]] = (elem["text"], 1)
        return key_count

    def nothing(acc, data):
        return data

    def join(left, right):
        return {k: v[1] for (k, v) in left.items() if left[k][1] == right.get(k, 0)}

    joiner.run(aggregate, nothing, join)
    joiner.close()
    healthServer.stop()


if __name__ == "__main__":
    main()
