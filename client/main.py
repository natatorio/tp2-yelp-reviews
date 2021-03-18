import json
import sys
import zipfile
import pprint
import logging
import pipe

logging.basicConfig(level=logging.ERROR)
logger = logging.getLogger("client")
logger.setLevel(logging.INFO)

REVIEWS_DATASET_FILEPATH = "data/yelp_academic_dataset_review.json.zip"
BUSINESS_DATASET_FILEPATH = "data/yelp_academic_dataset_business.json.zip"
CHUNK_SIZE = 1 * 1024 * 1024
MAX_REVIEWS = 500000
# 9000000000  #  8021122 Hasta 1 chunk de más
MAX_BUSINESS = 20000  # 209393
QUERIES = 5


def publish_file(
    file_path, chunk_size, max_size, session_id, pipe_out: pipe.Pipe, pause=None
):
    item_count = 0
    with zipfile.ZipFile(file_path) as z:
        for zname in z.namelist():
            with z.open(zname) as f:
                lines = f.readlines(chunk_size)
                while lines and item_count < max_size:
                    chunk = [json.loads(line) for line in lines]
                    item_count += len(chunk)
                    pipe_out.send(
                        {
                            "data": chunk,
                            "session_id": session_id,
                            "id": item_count,
                        }
                    )
                    if pause is not None and item_count > pause:
                        pause += pause
                        logger.info("%s press enter to continue", item_count)
                        input()
                    lines = f.readlines(chunk_size)
    logger.info(
        "%s items read from %s",
        item_count,
        file_path,
    )
    return item_count


def main():
    control = pipe.pub_sub_control()
    reports = pipe.reports()
    business = pipe.data_business()
    reviews = pipe.data_review()
    session_id = 1
    control.send({"session_id": session_id})
    if len(sys.argv) > 1:
        session_id = int(sys.argv[1])

    logger.info("start session: %s", session_id)
    logger.info("loading business")
    items = publish_file(
        file_path=BUSINESS_DATASET_FILEPATH,
        chunk_size=CHUNK_SIZE,
        max_size=MAX_BUSINESS,
        session_id=session_id,
        pipe_out=business,
    )
    business.send(
        {
            "id": items + 1,
            "data": None,
            "session_id": session_id,
        }
    )

    logger.info("loading reviews")
    items = publish_file(
        file_path=REVIEWS_DATASET_FILEPATH,
        chunk_size=CHUNK_SIZE,
        max_size=MAX_REVIEWS,
        session_id=session_id,
        pipe_out=reviews,
        pause=MAX_REVIEWS / 2,
    )
    reviews.send(
        {
            "id": items + 1,
            "data": None,
            "reply": "reports",
            "session_id": session_id,
        }
    )

    logger.info("waiting report")
    report = {}
    for payload, _ in reports.recv(auto_ack=True):
        if payload["data"]:
            key, val = payload["data"]
            report[key] = val
            logger.info("%s = %s", key, pprint.pformat(val))
        if len(report) >= 5:
            break

    logger.info("end session %s", session_id)
    reports.close()
    business.close()
    reviews.close()
    control.close()


if __name__ == "__main__":
    main()
