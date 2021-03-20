from kevasto import Client
from typing import cast, Dict
import os

def serialize_set(set):
    return ','.join([str(i) for i in list(set)])

def deserialize_set(str):
    return set([i for i in str.split(',')])

class Dedup:

    def __init__(self, stageName):
        self.processedBatches = set()
        self.db = Client()
        self.name = stageName
        self.retrieve_initial_state()

    def retrieve_initial_state(self):
        self.state = cast(Dict, self.db.get(self.name, "processed"))
        if self.state:
            self.processedBatches = deserialize_set(self.state.get("processed_batches"))
            self.retrieve_extension()
        else:
            self.persist_state()

    def persist_state(self):
        processed = {
            "processed_batches" : serialize_set(self.processedBatches),
        }
        processed = self.persist_extension(processed)
        self.db.put(
            self.name,
            "processed",
            processed,
        )

    def retrieve_extension(self):
        return

    def persist_extension(self, processed):
        return processed

    def set_processed_batch(self, batchId):
        self.processedBatches.add(batchId)

    def is_batch_processed(self, batchId):
        return batchId in self.processedBatches


class BussinessDedup(Dedup):
    def __init__(self, stageName):
        self.processedBussinessBatches = set()
        super().__init__(stageName)

    def retrieve_extension(self):
        self.processedBussinessBatches = deserialize_set(self.state.get("processed_bussiness_batches"))

    def persist_extension(self, processed):
        processed["processed_bussiness_batches"] = serialize_set(self.processedBussinessBatches)
        return processed

    def is_bussiness_batch_processed(self, bussinessBatchId):
        return bussinessBatchId in self.processedBussinessBatches

    def set_processed_bussiness_batch(self, batchId):
        self.processedBussinessBatches.add(batchId)


class AggregatorDedup(Dedup):
    def __init__(self, stageName):
        self.processedMessages = set()
        super().__init__(stageName)

    def retrieve_extension(self):
        self.processedMessages = deserialize_set(self.state.get("processed_messages"))

    def persist_extension(self, processed):
        processed["processed_messages"] = serialize_set(self.processedMessages)
        return processed

    def set_processed_message(self, messageId):
        self.processedMessages.add(messageId)

    def is_message_processed(self, messageId):
        return messageId in self.processedMessages

    def clear_processed_messages(self):
        self.processedMessages = set()

class ControlDedup(Dedup):
    def __init__(self, stageName):
        self.donePids = self.__get_all_pids()
        self.attendedRequests = set()
        super().__init__(stageName)

    def retrieve_extension(self):
        self.donePids = deserialize_set(self.state.get("done_pids"))
        self.attendedRequests = deserialize_set(self.state.get("attended_requests"))

    def persist_extension(self, processed):
        processed["done_pids"] = serialize_set(self.donePids)
        processed["attended_requests"] = serialize_set(self.attendedRequests)
        return processed

    def set_pid_done(self, pid):
        self.donePids.add(pid)

    def clear_pids_done(self):
        self.donePids = set()

    def are_all_pids_done(self):
        print(f"pids done {self.donePids}   all pids {self.__get_all_pids()}")
        return self.donePids == self.__get_all_pids()

    def set_request_attended(self, requestId):
        self.attendedRequests.add(requestId)

    def is_request_attended(self, requestId):
        return requestId in self.attendedRequests

    def __get_all_pids(self):
        pids = set()
        for processKey in [
            "ROUTER",
            "STARS5",
            "COMMENT",
            "BUSSINESS",
            "USERS",
            "HISTOGRAM",
            "FUNNY",
            "STARS5_MAPPER",
            "COMMENT_MAPPER",
            "HISTOGRAM_MAPPER",
            "FUNNY_MAPPER",
        ]:
            nReplicas = int(os.environ.get("N_" + processKey, 1))
            processIp = os.environ["IP_" + processKey]
            for i in range(nReplicas):
                ip = os.environ["IP_PREFIX"] + "_" + processIp + "_" + str(i + 1)
                pids.add(ip)
        return pids
