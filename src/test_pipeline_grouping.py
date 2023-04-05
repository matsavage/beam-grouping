import json
from datetime import datetime
import unittest

import apache_beam as beam
from apache_beam import window
from apache_beam.testing import test_pipeline, test_stream
from apache_beam.testing.util import assert_that, equal_to
from apache_beam.transforms import core
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions
from apache_beam.utils.timestamp import Duration

from apache_beam.transforms import trigger

from beam_pipeline import ExtractElement, InputElement, Logger


class ReifyWindowsFn(core.DoFn):
    def process(self, element, window=core.DoFn.WindowParam):
        key, values = element
        yield "%s @ %s" % (key, window), values


class TestTransformElement(unittest.TestCase):
    ELEMENTS = (
        {"id": 1, "timestamp": "1970-01-01T01:00:00.000000"},
        {"id": 1, "timestamp": "1970-01-01T01:00:00.001000"},
        {"id": 1, "timestamp": "1970-01-01T01:00:01.000000"},
        {"id": 1, "timestamp": "1970-01-01T01:00:01.005000"},
        {"id": 1, "timestamp": "1970-01-01T01:00:05.000000"},
        {"id": 1, "timestamp": "1970-01-01T01:00:05.000000"},
        {"id": 1, "timestamp": "1970-01-01T01:00:10.000000"},
    )

    TIMESTAMPED_ELEMENTS = [ExtractElement.produce_output("", x) for x in ELEMENTS]

    def test_pipeline_grouping(self):
        options = PipelineOptions(allow_unsafe_triggers=True)
        options.view_as(StandardOptions).streaming = True
        # options.view_as(StandardOptions).allow_unsafe_triggers = True

        p = test_pipeline.TestPipeline(options=options)

        input = p | (
            test_stream.TestStream()
            .advance_watermark_to(0)
            .add_elements(elements=[self.TIMESTAMPED_ELEMENTS[0]])
            .advance_processing_time(advance_by=2)
            # .advance_watermark_to(2)
            .add_elements(elements=self.TIMESTAMPED_ELEMENTS[2:])
            .advance_processing_time(advance_by=2)
            .advance_watermark_to(4)
            .add_elements(elements=self.TIMESTAMPED_ELEMENTS[3:])
            .advance_watermark_to_infinity()
        )
        # output = (
        #     input
        #     | beam.GroupIntoBatches(batch_size=100, max_buffering_duration_secs=1)
        #     | "LogWindow" >> beam.ParDo(Logger(label="raw_window"))
        #     | beam.ParDo(ReifyWindowsFn())
        # )

        output = (
            input
            | beam.WindowInto(
                window.FixedWindows(1),
                # trigger=trigger.AfterAny(
                #     trigger.AfterWatermark(),
                #     # trigger.AfterEach()
                # ),
                # trigger=trigger.AfterWatermark(
                #     early=trigger.AfterProcessingTime(delay=1),
                #     late=trigger.AfterCount(1)
                # ),
                trigger=trigger.AfterAny(
                    trigger.AfterProcessingTime(delay=1), trigger.AfterWatermark()
                ),
                accumulation_mode=trigger.AccumulationMode.DISCARDING,
                allowed_lateness=Duration.of(0.1),
            )
            | "LogWindow" >> beam.ParDo(Logger(label="windowed"))
            | beam.GroupByKey()
            # | beam.GroupIntoBatches(batch_size=100, max_buffering_duration_secs=1)
            | "LogWindow2" >> beam.ParDo(Logger(label="grouped"))
            # | beam.ParDo(ReifyWindowsFn())
            # | "LogWindow2" >> beam.ParDo(Logger(label="final"))
        )

        # assert_that(output, equal_to([]))

        p.run()

        assert False
