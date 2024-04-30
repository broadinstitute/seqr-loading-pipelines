import unittest

import hail as hl

from v03_pipeline.lib.misc.io import (
    annotate_vets,
    compute_hail_n_partitions,
    file_size_bytes,
    split_multi_hts,
)

TEST_MITO_MT = 'v03_pipeline/var/test/callsets/mito_1.mt'
TEST_SV_VCF = 'v03_pipeline/var/test/callsets/sv_1.vcf'


class IOTest(unittest.TestCase):
    def test_file_size_mb(self) -> None:
        # find v03_pipeline/var/test/callsets/mito_1.mt -type f | grep -v 'crc' | xargs ls -alt {} | awk '{sum += $5; print sum}'
        # 191310
        self.assertEqual(file_size_bytes(TEST_MITO_MT), 191310)
        self.assertEqual(file_size_bytes(TEST_SV_VCF), 20040)

    def test_compute_hail_n_partitions(self) -> None:
        self.assertEqual(compute_hail_n_partitions(23), 1)
        self.assertEqual(compute_hail_n_partitions(191310), 1)
        self.assertEqual(compute_hail_n_partitions(1913100000), 15)

    def test_annotate_vets(self) -> None:
        gatk_mt = hl.MatrixTable.from_parts(
            rows={
                'locus': [
                    hl.Locus(
                        contig='chr1',
                        position=1,
                        reference_genome='GRCh38',
                    ),
                ],
                'filters': [
                    hl.set(['PASS']),
                ],
            },
            cols={'s': ['sample_1']},
            entries={'HL': [[0.0]]},
        ).key_rows_by('locus')
        gatk_mt = annotate_vets(gatk_mt)
        dragen_mt = hl.MatrixTable.from_parts(
            rows={
                'locus': [
                    hl.Locus(
                        contig='chr1',
                        position=1,
                        reference_genome='GRCh38',
                    ),
                    hl.Locus(
                        contig='chr1',
                        position=2,
                        reference_genome='GRCh38',
                    ),
                    hl.Locus(
                        contig='chr1',
                        position=3,
                        reference_genome='GRCh38',
                    ),
                    hl.Locus(
                        contig='chr1',
                        position=4,
                        reference_genome='GRCh38',
                    ),
                    hl.Locus(
                        contig='chr1',
                        position=5,
                        reference_genome='GRCh38',
                    ),
                    hl.Locus(
                        contig='chr1',
                        position=5,
                        reference_genome='GRCh38',
                    ),
                ],
                'alleles': [
                    ['A', 'T'],
                    ['A', 'T'],
                    ['A', 'T'],
                    ['AC', 'T'],
                    ['AT', 'TC'],
                    ['AG', 'TG'],
                ],
                'filters': [
                    hl.set(['PASS']),
                    hl.empty_set(hl.tstr),
                    hl.missing(hl.tset(hl.tstr)),
                    hl.set(['PASS']),
                    hl.empty_set(hl.tstr),
                    hl.set(['PASS']),
                ],
                'info': [
                    hl.Struct(CALIBRATION_SENSITIVITY=['0.999']),
                    hl.Struct(CALIBRATION_SENSITIVITY=['0.995']),
                    hl.Struct(CALIBRATION_SENSITIVITY=['0.999']),
                    hl.Struct(CALIBRATION_SENSITIVITY=['0.98']),
                    hl.Struct(CALIBRATION_SENSITIVITY=['0.99']),
                    hl.Struct(CALIBRATION_SENSITIVITY=['0.991']),
                ],
            },
            cols={'s': ['sample_1']},
            entries={'HL': [[0.0], [0.0], [0.0], [0.0], [0.0], [0.0]]},
        ).key_rows_by('locus', 'alleles')
        dragen_mt = split_multi_hts(dragen_mt)
        dragen_mt = annotate_vets(dragen_mt)
        self.assertListEqual(
            dragen_mt.filters.collect(),
            [
                {'high_CALIBRATION_SENSITIVITY_SNP', 'PASS'},
                set(),
                {'high_CALIBRATION_SENSITIVITY_SNP'},
                {'PASS'},
                {'PASS'},
                {'high_CALIBRATION_SENSITIVITY_INDEL'},
            ],
        )
