import argparse
import logging
import json
from typing import Tuple
from datetime import datetime
from collections import namedtuple

import apache_beam as beam
from apache_beam.transforms.window import TimestampedValue
from apache_beam.io import ReadFromPubSub, WriteToBigQuery
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions
from apache_beam.utils.timestamp import Duration
from apache_beam.utils.timestamp import TimestampTypes
from apache_beam.transforms import trigger, window

InputElement = namedtuple("InputElement", "id timestamp value")


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
            logging.info("-"*80)
            logging.info(
                "Window [%s] has %s elements", human_readable_window(window), num_elements
            )
            # print(
            #     f">> Window [{human_readable_window(window)}] has {num_elements} elements"
            # )
            yield num_elements

    return (
        pcollection
        | "Count elements per window"
        >> beam.combiners.Count.Globally().without_defaults()
        | "Print counts info" >> beam.ParDo(PrintCountsInfo())
    )


class ExtractElement(beam.DoFn):
    def produce_output(self, x):
        x_out = InputElement(
            id=x["id"],
            timestamp=datetime.fromisoformat(x["timestamp"]),
            value=x["value"]
        )

        return (
            x_out.id,
            TimestampedValue(
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
    def __init__(self, label: str):
        super().__init__()
        self.label = label

    def process(self, element, *args, **kwargs):
        logging.info("%s: %s %s", self.label, type(element), element)

        # print(self.label, element)
        try:
            logging.info(
                "%s: %s %s",
                self.label,
                element[0],
                str([x.timestamp for x in element[1]]),
            )
        except Exception:
            pass

        try:
            logging.info("%s: %s %s", self.label, element[0], element[1].timestamp)
        except Exception:
            pass

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
                sum([d.value.value for d in data]) / len(data)
            )
    except Exception:
        pass


def main(argv=None):
    """Main entry point; defines and runs the wordcount pipeline."""
    arg_parser = argparse.ArgumentParser()

    known_args, pipeline_args = arg_parser.parse_known_args(argv)

    pipeline_options = PipelineOptions(pipeline_args, allow_unsafe_triggers=True)
    pipeline_options.view_as(StandardOptions).streaming = True

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
            | beam.WindowInto(
                beam.window.FixedWindows(10),
                trigger=trigger.AfterWatermark(),
                accumulation_mode=trigger.AccumulationMode.ACCUMULATING,
                allowed_lateness=Duration.of(0)
            )
        )

        # Log window info
        windows | "Print Window Info" >> PrintWindowInfo()

        grouping = (
            windows
                | beam.GroupByKey()
                | beam.CombinePerKey(combine_function)
        )

        # all_exceptions = beam.Flatten(
        #     input.Exception,
        #     ...
        # )

        # all_exceptions >> WriteToBigQuery(...)


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
