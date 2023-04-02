import json
import time
import random
import logging

from datetime import datetime
from google.cloud import pubsub_v1

# TODO(developer)
project_id = "ms-data-projects"
topic_id = "demo-topic"


def build_data_packet() -> str:
    return json.dumps(
        {
            "id": random.randint(0, 9),
            "timestamp": datetime.utcnow().isoformat(),
        }
    )


def main() -> None:
    publisher = pubsub_v1.PublisherClient()
    topic_path = publisher.topic_path(project_id, topic_id)

    while True:
        data = build_data_packet().encode("utf-8")

        future = publisher.publish(topic_path, data, origin="python-sample")
        logging.info(f"Published message %s: %s", future.result(), data)
        time.sleep(0.1)


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    main()
