import argparse
import logging
import json

from datetime import datetime

import apache_beam as beam
from apache_beam.io import ReadFromPubSub
from apache_beam.options.pipeline_options import PipelineOptions


class ExtractElement(beam.DoFn):
    def process(self, element, *args, **kwargs):
        return [(element["id"], element)]


class Logger(beam.DoFn):
    def __init__(self, label=str):
        super().__init__()
        self.label = label

    def process(self, element, *args, **kwargs):
        logging.info("%s: %s", self.label, element)
        yield element


def main(argv=None, save_main_session=True):
    """Main entry point; defines and runs the wordcount pipeline."""
    arg_parser = argparse.ArgumentParser()

    known_args, pipeline_args = arg_parser.parse_known_args(argv)

    pipeline_options = PipelineOptions(pipeline_args)

    with beam.Pipeline(options=pipeline_options) as p:
        input = (
            p
            | "Read from Pubsub"
            >> ReadFromPubSub(
                subscription="projects/ms-data-projects/subscriptions/demo-topic-sub"
            )
            | "To Json" >> beam.Map(lambda e: json.loads(e.decode("utf-8")))
            | "Timestamp elements"
            >> beam.Map(
                lambda elem: beam.window.TimestampedValue(
                    elem, datetime.fromisoformat(elem["timestamp"]).timestamp()
                )
            )
            | "Unpack" >> beam.ParDo(ExtractElement())
        )

        windows = (
            input
            | "GroupIntoBatches"
            >> beam.GroupIntoBatches(batch_size=100, max_buffering_duration_secs=1)
            | "LogWindow" >> beam.ParDo(Logger(label="raw_window"))
        )


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    main()
