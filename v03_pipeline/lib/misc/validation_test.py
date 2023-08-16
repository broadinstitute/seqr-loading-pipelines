import unittest

import hail as hl

from v03_pipeline.lib.misc.validation import validate_contigs
from v03_pipeline.lib.model import ReferenceGenome


class ValidationTest(unittest.TestCase):
    def test_validate_contigs(self) -> None:
        mt = hl.MatrixTable.from_parts(
            rows={'variants': [1, 2]},
            cols={'s': ['sample_1', 'sample_2', 'sample_3', 'sample_4']},
            entries={
                'HL': [
                    [0.0, hl.missing(hl.tfloat), 0.99, 0.01],
                    [0.1, 0.2, 0.94, 0.99],
                ],
            },
        )
        validate_contigs(mt, ReferenceGenome.GRCh38, 1)
