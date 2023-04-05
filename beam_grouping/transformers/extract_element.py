import json
from datetime import datetime

import apache_beam as beam
from apache_beam.transforms.window import TimestampedValue

from beam_grouping.pipeline_dataclasses import InputElement


class ExtractElement(beam.DoFn):
    def produce_output(self, x):
        x_out = InputElement(
            id=x["id"],
            timestamp=datetime.fromisoformat(x["timestamp"]),
            value=x["value"],
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
