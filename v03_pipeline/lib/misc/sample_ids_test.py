import unittest

import hail as hl
from hail import Struct

from v03_pipeline.lib.misc.sample_ids import MatrixTableSampleSetError, remap_sample_ids

CALLSET_MT = hl.MatrixTable.from_parts(
    rows={'variants': [1, 2]},
    cols={'s': ['HG00731', 'HG00732', 'HG00733']},
    entries={
        'HL': [
            [0.0, hl.missing(hl.tfloat), 0.3],
            [0.1, 0.2, 0.3],
        ],
    },
).key_cols_by('s')


class SampleLookupTest(unittest.TestCase):
    def test_remap_2_sample_ids(self) -> None:
        # remap 2 of 3 samples in callset
        project_remap_ht = hl.Table.parallelize(
            [
                {'s': 'HG00731', 'seqr_id': 'HG00731_1'},
                {'s': 'HG00732', 'seqr_id': 'HG00732_1'},
            ],
            hl.tstruct(
                s=hl.dtype('str'),
                seqr_id=hl.dtype('str'),
            ),
            key='s',
        )

        remapped_mt = remap_sample_ids(
            CALLSET_MT,
            project_remap_ht,
            ignore_missing_samples_when_remapping=True,
        )

        self.assertEqual(remapped_mt.cols().count(), 3)
        self.assertEqual(
            remapped_mt.cols().collect(),
            [
                Struct(col_idx=0, s='HG00731_1', seqr_id='HG00731_1', vcf_id='HG00731'),
                Struct(col_idx=1, s='HG00732_1', seqr_id='HG00732_1', vcf_id='HG00732'),
                Struct(col_idx=2, s='HG00733', seqr_id='HG00733', vcf_id='HG00733'),
            ],
        )

    def test_remap_sample_ids_remap_has_duplicate(self) -> None:
        # remap file has 2 rows for HG00732
        project_remap_ht = hl.Table.parallelize(
            [
                {'s': 'HG00731', 'seqr_id': 'HG00731_1'},
                {'s': 'HG00732', 'seqr_id': 'HG00732_1'},
                {'s': 'HG00732', 'seqr_id': 'HG00732_1'},  # duplicate
            ],
            hl.tstruct(
                s=hl.dtype('str'),
                seqr_id=hl.dtype('str'),
            ),
            key='s',
        )

        with self.assertRaises(ValueError):
            remap_sample_ids(
                CALLSET_MT,
                project_remap_ht,
                ignore_missing_samples_when_remapping=True,
            )

    def test_remap_sample_ids_remap_has_missing_samples(self) -> None:
        # remap file has 4 rows, but only 3 samples in callset
        project_remap_ht = hl.Table.parallelize(
            [
                {'s': 'HG00731', 'seqr_id': 'HG00731_1'},
                {'s': 'HG00732', 'seqr_id': 'HG00732_1'},
                {'s': 'HG00733', 'seqr_id': 'HG00733_1'},
                {'s': 'HG00734', 'seqr_id': 'HG00734_1'},  # missing in callset
            ],
            hl.tstruct(
                s=hl.dtype('str'),
                seqr_id=hl.dtype('str'),
            ),
            key='s',
        )

        with self.assertRaises(MatrixTableSampleSetError):
            remap_sample_ids(
                CALLSET_MT,
                project_remap_ht,
                ignore_missing_samples_when_remapping=False,
            )
