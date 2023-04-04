import argparse
import logging
import json

from datetime import datetime
from collections import namedtuple

import apache_beam as beam
from apache_beam import window
from apache_beam.io import ReadFromPubSub, WriteToBigQuery
from apache_beam.options.pipeline_options import PipelineOptions

InputElement = namedtuple("InputElement", "id timestamp")


class ExtractElement(beam.DoFn):
    def produce_output(self, x):
        x_out = InputElement(
            id=x["id"],
            timestamp=datetime.fromisoformat(x["timestamp"]),
        )

        return (
            x_out.id,
            window.TimestampedValue(
                x_out,
                x_out.timestamp.timestamp(),
            ),
        )

    def process(self, element, *args, **kwargs):
        try:
            element_json = json.loads(element.decode("utf-8"))

            yield beam.pvalue.TaggedOutput(
                "InputElement", self.produce_output(element_json)
            )

        except Exception as exception:
            yield beam.pvalue.TaggedOutput("Exception", [(element, exception)])


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
            | "Unpack Message"
            >> beam.ParDo(ExtractElement()).with_outputs("InputElement", "Exception")
        )

        windows = (
            input.InputElement
            | "GroupIntoBatches"
            >> beam.GroupIntoBatches(batch_size=100, max_buffering_duration_secs=1)
            | "LogWindow" >> beam.ParDo(Logger(label="raw_window"))
        )

        # all_exceptions = beam.Flatten(
        #     input.Exception,
        #     ...
        # )

        # all_exceptions >> WriteToBigQuery(...)


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    main()
