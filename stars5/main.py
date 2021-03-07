from consumers import Joiner
from health_server import HealthServer
import pipe


def main():
    healthServer = HealthServer()
    joiner = Joiner(
        left_in=pipe.consume_star5(),
        right_in=pipe.consume_star5_data(),
        join_out=pipe.annon(),
    )

    def join(right, left):
        return (
            "stars5",
            {k: v for (k, v) in right.items() if right[k] == left.get(k, 0)},
        )

    def count(acc, data, _):
        for elem in data:
            acc[elem["user_id"]] = acc.get(elem["user_id"], 0) + 1
        return acc

    def nothing(acc, data, _):
        return data

    joiner.run(count, nothing, join)
    joiner.close()
    healthServer.stop()


if __name__ == "__main__":
    main()
