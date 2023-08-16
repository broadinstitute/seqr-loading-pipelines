import unittest

import hail as hl

from v03_pipeline.lib.misc.validation import SeqrValidationError, validate_contigs
from v03_pipeline.lib.model import ReferenceGenome


def _mt_from_contigs(contigs):
    return hl.MatrixTable.from_parts(
        rows={
            'locus': [
                hl.Locus(
                    contig=contig,
                    position=1,
                    reference_genome='GRCh38',
                )
                for contig in contigs
            ],
        },
        cols={'s': ['sample_1']},
        entries={'HL': [[0.0] for _ in range(len(contigs))]},
    )


class ValidationTest(unittest.TestCase):
    def test_validate_contigs(self) -> None:
        mt = _mt_from_contigs(ReferenceGenome.GRCh38.standard_contigs)
        self.assertIsNone(validate_contigs(mt, ReferenceGenome.GRCh38, 1))
        self.assertRaisesRegex(
            SeqrValidationError,
            'Missing the following expected contigs',
            validate_contigs,
            mt,
            ReferenceGenome.GRCh37,
            1,
        )
        self.assertRaisesRegex(
            SeqrValidationError,
            'which is lower than expected minimum count',
            validate_contigs,
            mt,
            ReferenceGenome.GRCh38,
            2,
        )

        # Drop an optional contig
        mt = _mt_from_contigs(ReferenceGenome.GRCh38.standard_contigs - {'chrY'})
        self.assertIsNone(validate_contigs(mt, ReferenceGenome.GRCh38, 1))

        # Drop a non-optional contig
        mt = _mt_from_contigs(ReferenceGenome.GRCh38.standard_contigs - {'chr3'})
        self.assertRaisesRegex(
            SeqrValidationError,
            'Missing the following expected contigs',
            validate_contigs,
            mt,
            ReferenceGenome.GRCh38,
            1,
        )

        # Add an unexpected contig
        mt = _mt_from_contigs(ReferenceGenome.GRCh38.standard_contigs | {'chr503'})
        self.assertRaisesRegex(
            SeqrValidationError,
            'Found the following unexpected contigs',
            validate_contigs,
            mt,
            ReferenceGenome.GRCh38,
            1,
        )
