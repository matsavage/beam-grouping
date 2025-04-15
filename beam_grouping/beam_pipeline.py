import argparse
import logging
import os

import apache_beam as beam
from apache_beam.io import ReadFromPubSub
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions
from apache_beam.transforms import trigger
from apache_beam.utils.timestamp import Duration

from beam_grouping.pipeline_dataclasses import PipelineTags
from beam_grouping.transformers import ExtractElement


def human_readable_window(window) -> str:
    """Formats a window object into a human readable string."""
    if isinstance(window, beam.window.GlobalWindow):
        return str(window)
    return f"{window.start.to_utc_datetime():%Y-%m-%d %H:%M:%S.%f} - {window.end.to_utc_datetime():%Y-%m-%d %H:%M:%S.%f}"


class PrintElementInfo(beam.DoFn):
    """Prints an element with its Window information."""

    def process(
        self, element, timestamp=beam.DoFn.TimestampParam, window=beam.DoFn.WindowParam
    ):
        print(
            f"[{human_readable_window(window)}] {timestamp.to_utc_datetime()} -- {element[0]}"
        )
        yield element


@beam.ptransform_fn
def PrintWindowInfo(pcollection):
    """Prints the Window information with how many elements landed in that window."""

    class PrintCountsInfo(beam.DoFn):
        def process(self, num_elements, window=beam.DoFn.WindowParam):
            logging.info("-" * 80)
            logging.info(
                "Window [%s] has %s elements",
                human_readable_window(window),
                num_elements,
            )
            yield num_elements

    return (
        pcollection
        | "Count elements per window"
        >> beam.combiners.Count.Globally().without_defaults()
        | "Print counts info" >> beam.ParDo(PrintCountsInfo())
    )


class Logger(beam.DoFn):
    def __init__(self, label: str):
        super().__init__()
        self.label = label

    def process(self, element, *args, **kwargs):
        logging.info("%s: %s %s", self.label, type(element), element)
        yield element


def combine_function(values):
    try:
        if values is not None:
            data = [y for y in [x for x in values][0]]
            logging.info(
                ">Group [%s - %s] id: %s records: %02d mean: %+1.3f",
                data[0].value.timestamp,
                data[-1].value.timestamp,
                data[0].value.id,
                len(data),
                sum([d.value.value for d in data]) / len(data),
            )
    except Exception:
        pass


def main(argv=None):
    """Main entry point; defines and runs the pipeline."""
    arg_parser = argparse.ArgumentParser()
    arg_parser.add_argument(
        "--subscription",
        default="demo-topic-sub",
        help="Pub/Sub subscription to read from",
    )
    arg_parser.add_argument(
        "--use_emulator", action="store_true", help="Use the Pub/Sub emulator"
    )
    arg_parser.add_argument(
        "--project",
        default="local-project",
        help="Project ID for Pub/Sub emulator (only used with --use_emulator)",
    )
    arg_parser.add_argument(
        "--emulator_host",
        default="localhost:8085",
        help="Pub/Sub emulator host:port (only used with --use_emulator)",
    )

    known_args, pipeline_args = arg_parser.parse_known_args(argv)

    # Configure for emulator if requested
    if known_args.use_emulator:
        os.environ["PUBSUB_EMULATOR_HOST"] = known_args.emulator_host
        logging.info(f"Using Pub/Sub emulator at {known_args.emulator_host}")

        # If subscription doesn't contain project, add it
        if not known_args.subscription.startswith("projects/"):
            known_args.subscription = (
                f"projects/{known_args.project}/subscriptions/{known_args.subscription}"
            )

    logging.info(f"Reading from subscription: {known_args.subscription}")

    pipeline_options = PipelineOptions(pipeline_args, allow_unsafe_triggers=True)
    pipeline_options.view_as(StandardOptions).streaming = True

    with beam.Pipeline(options=pipeline_options) as p:
        input = (
            p
            | "Read from Pubsub" >> ReadFromPubSub(subscription=known_args.subscription)
            | "Unpack Message"
            >> beam.ParDo(ExtractElement()).with_outputs("InputElement", "Exception")
        )

        windows = input[PipelineTags.INPUT_ELEMENT] | beam.WindowInto(
            beam.window.FixedWindows(10),
            trigger=trigger.AfterWatermark(),
            accumulation_mode=trigger.AccumulationMode.ACCUMULATING,
            allowed_lateness=Duration.of(0),
        )

        # Log window info
        windows | "Print Window Info" >> PrintWindowInfo()

        _ = windows | beam.GroupByKey() | beam.CombinePerKey(combine_function)

        # Log exceptions
        input[PipelineTags.EXCEPTION] | "Log Exceptions" >> beam.ParDo(
            Logger("Exception")
        )


if __name__ == "__main__":
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)

    formatter = logging.Formatter(
        fmt="%(asctime)s.%(msecs)03d %(levelname)-8s %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    ch = logging.StreamHandler()
    ch.setFormatter(formatter)
    logger.addHandler(ch)
    main()
