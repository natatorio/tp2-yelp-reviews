from threading import Thread, Event, RLock
from datetime import datetime


class Scheduler:
    def __init__(self) -> None:
        self.thread = Thread(target=self.run)
        self.event = Event()
        self.lock = RLock()
        self.is_shutdown = False
        self.tasks = []
        self.thread.start()

    def schedule(self, delta, func):
        with self.lock:
            self.tasks.append([datetime.now() + delta, func])
            self.tasks.sort(key=lambda it: it[0])
        self.event.set()

    def shutdown(self):
        self.is_shutdown = True
        self.event.set()

    def run(self):
        # print("stated")
        # basado en ThreadSchedulerExecutor de java
        timeout = None
        while not self.is_shutdown:
            self.event.wait(timeout)
            self.event.clear()
            with self.lock:
                # need a lock because task list is re-assigned
                remaining = []
                for task in self.tasks:
                    [deadline, func] = task
                    if deadline <= datetime.now():
                        func()
                    else:
                        remaining.append(task)

                self.tasks = remaining
                self.tasks.sort(key=lambda it: it[0])
            timeout = None
            if len(self.tasks) > 0:
                timeout = max(
                    0,
                    (self.tasks[0][0] - datetime.now()).total_seconds(),
                )
            # print("wait", timeout)
        # print("shutdown")


# s = Scheduler()
# def re_schedule():
#     print("hola")
#     s.schedule(
#         timedelta(milliseconds=1000),
#         re_schedule,
#     )
