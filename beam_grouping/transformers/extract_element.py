
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

from beam_grouping.pipeline_dataclasses import InputElement

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
