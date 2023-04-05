import json
from datetime import datetime
import unittest

import apache_beam as beam
from apache_beam import window
from apache_beam.testing import test_pipeline
from apache_beam.testing.util import assert_that, equal_to


from beam_grouping.transformers import ExtractElement, InputElement


class TestTransformElement(unittest.TestCase):
    ELEMENTS = (
        {"id": 1, "timestamp": "1970-01-01T01:00:00.000000", "value": 1},
        {"id": 1, "timestamp": "1970-01-01T01:00:00.001000", "value": 1},
    )

    EXPECTED_ELEMENTS = [ExtractElement.produce_output("", x) for x in ELEMENTS]

    def test_produce_output(self):
        expected_output = [
            (
                1,
                window.TimestampedValue(
                    InputElement(id=1, timestamp=datetime(1970, 1, 1, 1, 0, 0, 0), value=1),
                    0,
                ),
            ),
            (
                1,
                window.TimestampedValue(
                    InputElement(id=1, timestamp=datetime(1970, 1, 1, 1, 0, 0, 1000), value=1),
                    0.001000,
                ),
            ),
        ]

        generated_output = [ExtractElement.produce_output("", x) for x in self.ELEMENTS]

        self.assertEqual(expected_output, generated_output)
        self.assertEqual(expected_output, self.EXPECTED_ELEMENTS)

    def test_extract_element(self):
        with test_pipeline.TestPipeline() as p:
            input = p | beam.Create(
                [json.dumps(x).encode("utf-8") for x in self.ELEMENTS]
            )

            output = input | beam.ParDo(ExtractElement()).with_outputs(
                "InputElement", "Exception"
            )

        assert_that(output.Exception, equal_to([]), label="Expect no exceptions")

        assert_that(output.InputElement, equal_to(self.EXPECTED_ELEMENTS))
