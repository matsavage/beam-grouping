# Beam Grouping

A demonstration of Apache Beam stream processing capabilities with windowing, grouping, and aggregation patterns.

## Overview

This repository provides a complete example of a streaming data pipeline built with Apache Beam that:

1. Consumes messages from a streaming source (Google Cloud Pub/Sub or local simulator)
2. Processes records in fixed time windows
3. Groups and aggregates data points by their ID
4. Calculates statistics (means) for each group
5. Demonstrates robust error handling patterns

The pipeline showcases key Apache Beam concepts including windowing strategies, triggers, timestamping, and tagged outputs for exception handling.

## Architecture

### Pipeline Components

- **Main Pipeline** (`beam_pipeline.py`): 
  - Unified pipeline that can read from either Google Cloud Pub/Sub or local emulator
  - Configurable via command-line arguments
- **Transformers** (`transformers/extract_element.py`): 
  - Contains the parsing logic that converts raw messages to structured data objects
- **Data Models**:
  - `InputElement`: Structured representation of valid messages (ID, timestamp, value)
  - `ExceptionElement`: Standardized error reporting format
  - `PipelineTags`: Constants for tagged outputs (normal vs exception paths)

### Key Features

- **10-second Fixed Windows**: Groups records into discrete time buckets for processing
- **Event Time Processing**: Uses message timestamps rather than processing time
- **Tagged Outputs**: Separates successful parsing from exceptions
- **Pydantic Models**: Ensures data validation and type safety
- **Accumulating Triggers**: Allows for late data handling with updated aggregates

## Test Data Generation

The repository includes a unified publisher script:

- `util/publisher.py`: Publishes to either Google Cloud Pub/Sub or local emulator based on options

The publisher generates messages containing:
- An ID (0-9), timestamp, and value
- Values follow a normal distribution centered around the ID value (mean = ID, standard deviation = 0.5)
- Messages are published every 100ms
- Each ID represents a logical group that should have similar values

## Usage

### Prerequisites

- Python 3.8+
- Poetry (for dependency management)
- Google Cloud CLI (for the Pub/Sub emulator in local mode)
- Google Cloud project with Pub/Sub configured (for cloud mode only)

### Installation

```bash
# Install dependencies with Poetry
poetry install

# Install the gcloud CLI if you don't have it
# https://cloud.google.com/sdk/docs/install
```

### Running the Pipeline

#### Local Mode (Using Pub/Sub Emulator)

1. Start the Pub/Sub emulator and set up topics/subscriptions:

```bash
# Start the emulator and set up topics/subscriptions
make setup-emulator
```

2. Run the pipeline and publisher in separate terminals:

```bash
# Terminal 1: Run the local pipeline
make run-local-pipeline

# Terminal 2: Generate test data locally
make run-local-publisher
```

3. Stop the emulator when you're done:

```bash
# Stop the emulator
make stop-emulator
```

You can also customize the emulator settings:

```bash
# Custom project, topic and subscription
python util/setup_emulator.py --project=my-project --topic=my-topic --subscription=my-sub

# Custom emulator host
python util/publisher.py --use_emulator --emulator-host=localhost:8086

# Run pipeline with custom settings
python -m beam_grouping.beam_pipeline --use_emulator --project=my-project --subscription=my-sub
```

#### Cloud Mode (Google Cloud Required)

```bash
# Run the cloud pipeline
make run-pipeline

# Generate test data for Google Cloud Pub/Sub
make run-publisher
```

### Testing

```bash
# Run tests
make run-test
```

## Configuration

The cloud pipeline connects to a predefined Pub/Sub subscription:
```
projects/ms-data-projects/subscriptions/demo-topic-sub
```

Modify the subscription path in `beam_pipeline.py` if needed.

## Example Output

When running the pipeline with test data, you'll see output like:

```
2025-04-15 16:48:40.040 INFO     --------------------------------------------------------------------------------
2025-04-15 16:48:40.040 INFO     Window [2025-04-15 15:48:30.000000 - 2025-04-15 15:48:40.000000] has 86 elements
2025-04-15 16:48:40.045 INFO     >Group [2025-04-15 15:48:30.494083 - 2025-04-15 15:48:39.022577] id: 0 records: 10 mean: +0.198
2025-04-15 16:48:40.045 INFO     >Group [2025-04-15 15:48:30.958822 - 2025-04-15 15:48:39.605393] id: 2 records: 08 mean: +2.349
2025-04-15 16:48:40.045 INFO     >Group [2025-04-15 15:48:31.657540 - 2025-04-15 15:48:39.256653] id: 1 records: 13 mean: +0.985
2025-04-15 16:48:40.045 INFO     >Group [2025-04-15 15:48:30.147990 - 2025-04-15 15:48:39.722105] id: 7 records: 11 mean: +6.761
2025-04-15 16:48:40.045 INFO     >Group [2025-04-15 15:48:30.611088 - 2025-04-15 15:48:37.971975] id: 9 records: 11 mean: +9.088
2025-04-15 16:48:40.045 INFO     >Group [2025-04-15 15:48:30.029664 - 2025-04-15 15:48:39.959394] id: 4 records: 05 mean: +3.705
2025-04-15 16:48:40.045 INFO     >Group [2025-04-15 15:48:31.307512 - 2025-04-15 15:48:39.841404] id: 5 records: 09 mean: +4.951
2025-04-15 16:48:40.045 INFO     >Group [2025-04-15 15:48:30.381859 - 2025-04-15 15:48:38.555057] id: 8 records: 06 mean: +7.960
2025-04-15 16:48:40.045 INFO     >Group [2025-04-15 15:48:30.728942 - 2025-04-15 15:48:38.788574] id: 3 records: 06 mean: +2.992
2025-04-15 16:48:40.045 INFO     >Group [2025-04-15 15:48:30.265256 - 2025-04-15 15:48:39.490204] id: 6 records: 07 mean: +6.298
2025-04-15 16:48:50.109 INFO     --------------------------------------------------------------------------------
```

This shows:
1. The 10-second window boundaries
2. The total number of elements in each window
3. Grouped records by ID within the window
4. First and last timestamp for each group
5. Number of records per group
6. Mean value for each group

The output confirms that values cluster around their ID (e.g., ID 7 has a mean of approximately 7).