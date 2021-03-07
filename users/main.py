from consumers import CounterBy
from health_server import HealthServer
import pipe
from pipe import Formatted


def main():
    def user_count_5(user_count):
        return dict([u for u in user_count.items() if u[1] >= 3])

    def user_count_50(user_count):
        return dict([u for u in user_count.items() if u[1] >= 15])

    def user_count_150(user_count):
        return (
            "users_150",
            dict([u for u in user_count.items() if u[1] >= 100]),
        )

    healthServer = HealthServer()
    counter = CounterBy(
        pipe.consume_users(),
        [
            Formatted(pipe.consume_star5_data(), user_count_50),
            Formatted(pipe.consume_comment_data(), user_count_5),
            Formatted(pipe.annon(), user_count_150),
        ],
        key_id="user_id",
    )
    counter.run()
    counter.close()
    healthServer.stop()


if __name__ == "__main__":
    main()
