
# Pub/Sub emulator configuration
PUBSUB_PROJECT := local-project
PUBSUB_TOPIC := demo-topic
PUBSUB_SUBSCRIPTION := demo-topic-sub
PUBSUB_EMULATOR_HOST := localhost:8085

# Export environment variables for all commands
export PUBSUB_EMULATOR_HOST

run-test:
	python3 -m pytest --cov-report term-missing --cov beam_grouping

run-pipeline:
	python3 -m beam_grouping.beam_pipeline

run-publisher:
	python3 util/publisher.py

run-local-pipeline:
	python3 -m beam_grouping.beam_pipeline --use_emulator --project=$(PUBSUB_PROJECT) --subscription=$(PUBSUB_SUBSCRIPTION)

run-local-publisher:
	python3 util/publisher.py --use_emulator --project=$(PUBSUB_PROJECT) --topic=$(PUBSUB_TOPIC)

# Start the Pub/Sub emulator and set up topics/subscriptions
setup-emulator:
	@echo "Starting Pub/Sub emulator..."
	@if command -v gcloud > /dev/null; then \
		(gcloud beta emulators pubsub start --project=$(PUBSUB_PROJECT) --host-port=$(PUBSUB_EMULATOR_HOST) & echo $$! > /tmp/pubsub_emulator.pid & sleep 3); \
		python3 util/setup_emulator.py --project=$(PUBSUB_PROJECT) --topic=$(PUBSUB_TOPIC) --subscription=$(PUBSUB_SUBSCRIPTION) --emulator-host=$(PUBSUB_EMULATOR_HOST); \
	else \
		echo "Error: gcloud not found. Please install Google Cloud SDK."; \
		exit 1; \
	fi

# Stop the emulator if it's running
stop-emulator:
	@if [ -f /tmp/pubsub_emulator.pid ]; then \
		echo "Stopping Pub/Sub emulator..."; \
		kill -9 `cat /tmp/pubsub_emulator.pid` 2>/dev/null || true; \
		rm /tmp/pubsub_emulator.pid; \
	else \
		echo "No emulator PID file found."; \
	fi

.PHONY: run-test run-pipeline run-publisher run-local-pipeline run-local-publisher setup-emulator stop-emulator run-local
