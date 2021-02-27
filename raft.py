class Raft:
    def __init__(self) -> None:
        self.current_term = 0
        self.voted_for = None
        self.entries = []

        self.commit_index = -1
        self.last_applied = -1

        #  {
        #     "term": 0,
        #     "leader_id": "",
        #     "prev_log_index": 0,
        #     "prev_log_term": 0,
        #     "entries": [],
        #     "leader_commit": 0,
        # }

    def append_entries(self, req):
        if req["term"] < self.current_term:
            return {
                "term": self.current_term,
                "success": False,
            }
        if len(self.entries) > req["prev_log_index"]:
            prev_log_entry = self.entries[req["prev_log_index"]]
            if prev_log_entry is None or prev_log_entry["term"] != req["prev_log_term"]:
                return {
                    "term": self.current_term,
                    "success": False,
                }
        # if conflict else append entries
        index = req["prev_log_index"] + 1
        self.entries = self.entries[0:index] + req["entries"]

        if req["leader_commit"] > self.commit_index:
            self.commit_index = min(
                req["leader_commit"],
                len(self.entries - 1),
            )

        return {
            "term": self.current_term,
            "success": True,
        }

    # {
    #  "term":0,
    #  "canditate_id":"",
    #  "last_log_index": 0,
    #  "last_log_term":0,
    #  }
    def request_vote(self, req):
        if req["term"] >= self.current_term:
            if self.voted_for is None or self.voted_for == req["candidate_id"]:
                if req["last_log_index"] >= len(self.entries):
                    last_log_entry = self.entries[req["last_log_index"]]
                    if last_log_entry["term"] == req["last_log_term"]:
                        self.voted_for = req["candidate_id"]
                        self.term = req["term"]
                        return {
                            "term": self.current_term,
                            "vote_granted": True,
                        }
        return {
            "term": self.current_term,
            "vote_granted": False,
        }


class Follower(Raft):
    def append_entries(self, req):
        if self.voted_for == req["leader_id"]:
            return super().append_entries(req)
        return {}


class Candidate:
    def __init__(self) -> None:
        pass


class Leader:
    def __init__(self):
        self.next_index = []
        self.match_index = []

    def append_entries(self, req):
        return {"term": self.current}
