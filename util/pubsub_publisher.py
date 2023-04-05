import json
import logging
import random
import time
from datetime import datetime

import numpy as np
from google.cloud import pubsub_v1

import numpy as np

project_id = "ms-data-projects"
topic_id = "demo-topic"


def build_data_packet() -> str:
    random_value =  random.randint(0, 9)
    return json.dumps(
        {
            "id": random_value,
            "timestamp": datetime.utcnow().isoformat(),
            "value": np.random.normal(random_value, 0.5)
        }
    )


def main() -> None:
    publisher = pubsub_v1.PublisherClient()
    topic_path = publisher.topic_path(project_id, topic_id)

    while True:
        data = build_data_packet().encode("utf-8")

        future = publisher.publish(topic_path, data, origin="python-sample")
        logging.info(f"%s: %s", future.result(), data)
        time.sleep(0.1)


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    main()
