import argparse
import logging
import os

try:
    from google.cloud import pubsub_v1
except ImportError:
    logging.error("Google Cloud Pub/Sub library not installed. Run 'pip install google-cloud-pubsub'")
    raise


def setup_emulator(project_id="local-project", topic_id="demo-topic", 
                   subscription_id="demo-topic-sub", emulator_host="localhost:8085"):
    """Setup the Pub/Sub emulator with topic and subscription."""
    # Set the emulator host environment variable
    os.environ["PUBSUB_EMULATOR_HOST"] = emulator_host
    
    logging.info(f"Setting up Pub/Sub emulator at {emulator_host}")
    logging.info(f"Project: {project_id}, Topic: {topic_id}, Subscription: {subscription_id}")
    
    # Create publisher and subscriber clients
    publisher = pubsub_v1.PublisherClient()
    subscriber = pubsub_v1.SubscriberClient()
    
    # Get the topic and subscription paths
    topic_path = publisher.topic_path(project_id, topic_id)
    subscription_path = subscriber.subscription_path(project_id, subscription_id)
    
    # Create the topic if it doesn't exist
    try:
        publisher.create_topic(request={"name": topic_path})
        logging.info(f"Created topic: {topic_path}")
    except Exception as e:
        logging.info(f"Topic already exists or error: {e}")
    
    # Create the subscription if it doesn't exist
    try:
        subscriber.create_subscription(
            request={"name": subscription_path, "topic": topic_path}
        )
        logging.info(f"Created subscription: {subscription_path}")
    except Exception as e:
        logging.info(f"Subscription already exists or error: {e}")
    
    # Return the configured paths
    return topic_path, subscription_path


if __name__ == "__main__":
    # Parse arguments
    parser = argparse.ArgumentParser(description="Setup Pub/Sub emulator for local development")
    parser.add_argument("--project", default="local-project", help="Project ID for Pub/Sub emulator")
    parser.add_argument("--topic", default="demo-topic", help="Topic name for Pub/Sub emulator")
    parser.add_argument("--subscription", default="demo-topic-sub", help="Subscription name for Pub/Sub emulator")
    parser.add_argument("--emulator-host", default="localhost:8085", help="Pub/Sub emulator host:port")
    
    args = parser.parse_args()
    
    # Configure logging
    logging.getLogger().setLevel(logging.INFO)
    
    formatter = logging.Formatter(
        fmt="%(asctime)s.%(msecs)03d %(levelname)-8s %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    ch = logging.StreamHandler()
    ch.setFormatter(formatter)
    logging.getLogger().addHandler(ch)
    
    # Run the setup
    topic_path, subscription_path = setup_emulator(
        project_id=args.project,
        topic_id=args.topic,
        subscription_id=args.subscription,
        emulator_host=args.emulator_host
    )
    
    # Print instructions
    print("\n" + "=" * 80)
    print("Pub/Sub emulator setup complete!")
    print("=" * 80)
    print("\nTo use the local emulator:")
    print("\n1. Start the emulator in a separate terminal:")
    print("   gcloud beta emulators pubsub start --project=local-project")
    print("\n2. Run the setup script (this script):")
    print("   python util/setup_emulator.py")
    print("\n3. Run the local publisher:")
    print("   python util/local_publisher.py")
    print("\n4. Run the local pipeline:")
    print("   python -m beam_grouping.local_pipeline")
    print("\nAll commands accept the following arguments:")
    print("   --project, --topic, --subscription, --emulator-host")
    print("\nHappy local development!")
    print("=" * 80)