import argparse
import json
import logging
import os
import random
import time
from datetime import datetime

import numpy as np

# For Pub/Sub client
try:
    from google.cloud import pubsub_v1
except ImportError:
    logging.error(
        "Google Cloud Pub/Sub library not installed. Run 'pip install google-cloud-pubsub'"
    )
    raise


def build_data_packet() -> str:
    """Generate test data with random ID and value."""
    random_value = random.randint(0, 9)
    return json.dumps(
        {
            "id": random_value,
            "timestamp": datetime.utcnow().isoformat(),
            "value": np.random.normal(random_value, 0.5),
        }
    )


def main(argv=None):
    """Main entry point for the publisher."""
    parser = argparse.ArgumentParser(description="Publish test data to Pub/Sub")
    parser.add_argument(
        "--use_emulator",
        action="store_true",
        help="Use the Pub/Sub emulator instead of real Google Cloud",
    )
    parser.add_argument(
        "--project",
        default="ms-data-projects",
        help="Project ID (default: ms-data-projects)",
    )
    parser.add_argument(
        "--topic", default="demo-topic", help="Topic name (default: demo-topic)"
    )
    parser.add_argument(
        "--emulator_host",
        default="localhost:8085",
        help="Pub/Sub emulator host:port (only used with --use_emulator)",
    )

    args = parser.parse_args(argv)

    # Configure for emulator if requested
    if args.use_emulator:
        os.environ["PUBSUB_EMULATOR_HOST"] = args.emulator_host
        logging.info(f"Using Pub/Sub emulator at {args.emulator_host}")
        logging.info(
            "If the emulator is not running, start it with: gcloud beta emulators pubsub start"
        )

    # Create the publisher client
    publisher = pubsub_v1.PublisherClient()

    # Get the full topic path
    topic_path = publisher.topic_path(args.project, args.topic)
    logging.info(f"Publishing to topic: {topic_path}")

    # Try to create the topic if using emulator
    if args.use_emulator:
        try:
            publisher.create_topic(request={"name": topic_path})
            logging.info(f"Created topic: {topic_path}")
        except Exception as e:
            logging.info(f"Topic already exists or error: {e}")

    # Main publishing loop
    try:
        while True:
            # Generate a message
            data = build_data_packet()

            # Publish the message
            future = publisher.publish(
                topic_path, data.encode("utf-8"), origin="python-sample"
            )

            # Wait for the publish to complete
            message_id = future.result()

            # Log the message
            mode = "emulator" if args.use_emulator else "cloud"
            logging.info(f"Published to {mode} [{message_id}]: {data}")

            # Wait a bit before next message
            time.sleep(0.1)
    except KeyboardInterrupt:
        logging.info("Publisher stopped by user")


if __name__ == "__main__":
    # Configure logging
    logging.getLogger().setLevel(logging.INFO)

    formatter = logging.Formatter(
        fmt="%(asctime)s.%(msecs)03d %(levelname)-8s %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    ch = logging.StreamHandler()
    ch.setFormatter(formatter)
    logging.getLogger().addHandler(ch)

    # Run the publisher
    main()
