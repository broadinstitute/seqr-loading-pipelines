import os
import shutil
import tempfile
import unittest
from unittest.mock import Mock, patch

import hail as hl

from v03_pipeline.lib.annotations.fields import get_fields
from v03_pipeline.lib.model import AnnotationType, DatasetType, Env, ReferenceGenome

TEST_COMBINED_1 = 'v03_pipeline/var/test/reference_data/test_combined_1.ht'
TEST_HGMD_1 = 'v03_pipeline/var/test/reference_data/test_hgmd_1.ht'
TEST_INTERVAL_REFERENCE_1 = (
    'v03_pipeline/var/test/reference_data/test_interval_reference_1.ht'
)
LIFTOVER = 'v03_pipeline/var/test/liftover/grch38_to_grch37.over.chain.gz'


@patch('v03_pipeline.lib.paths.DataRoot')
class FieldsTest(unittest.TestCase):
    def setUp(self) -> None:
        self._temp_local_reference_data = tempfile.TemporaryDirectory().name
        shutil.copytree(
            TEST_HGMD_1,
            f'{self._temp_local_reference_data}/GRCh38/v03/hgmd.ht',
        )
        shutil.copytree(
            TEST_INTERVAL_REFERENCE_1,
            f'{self._temp_local_reference_data}/GRCh38/v03/interval_reference.ht',
        )

    def tearDown(self) -> None:
        if os.path.isdir(self._temp_local_reference_data):
            shutil.rmtree(self._temp_local_reference_data)

    def test_get_rdc_fields(self, mock_dataroot: Mock) -> None:
        mock_dataroot.LOCAL_REFERENCE_DATA.value = self._temp_local_reference_data
        ht = hl.Table.parallelize(
            [],
            hl.tstruct(
                locus=hl.tlocus(ReferenceGenome.GRCh38.value),
                alleles=hl.tarray(hl.tstr),
            ),
            key=('locus', 'alleles'),
        )
        self.assertCountEqual(
            list(
                get_fields(
                    ht,
                    AnnotationType.REFERENCE_DATASET_COLLECTION,
                    env=Env.TEST,
                    dataset_type=DatasetType.SNV,
                    reference_genome=ReferenceGenome.GRCh38,
                ).keys(),
            ),
            ['hgmd', 'gnomad_non_coding_constraint', 'screen'],
        )
        self.assertCountEqual(
            list(
                get_fields(
                    ht,
                    AnnotationType.REFERENCE_DATASET_COLLECTION,
                    env=Env.LOCAL,
                    dataset_type=DatasetType.SNV,
                    reference_genome=ReferenceGenome.GRCh38,
                ).keys(),
            ),
            ['gnomad_non_coding_constraint', 'screen'],
        )
        self.assertCountEqual(
            list(
                get_fields(
                    ht,
                    AnnotationType.REFERENCE_DATASET_COLLECTION,
                    env=Env.TEST,
                    dataset_type=DatasetType.MITO,
                    reference_genome=ReferenceGenome.GRCh38,
                ).keys(),
            ),
            [],
        )

    def test_get_formatting_fields(self, mock_dataroot: Mock) -> None:
        mock_dataroot.LOCAL_REFERENCE_DATA.value = self._temp_local_reference_data
        ht = hl.read_table(TEST_COMBINED_1)
        self.assertCountEqual(
            list(
                get_fields(
                    ht,
                    AnnotationType.FORMATTING,
                    env=Env.TEST,
                    dataset_type=DatasetType.SNV,
                    reference_genome=ReferenceGenome.GRCh38,
                    liftover_ref_path=LIFTOVER,
                ).keys(),
            ),
            [
                'rg37_locus',
                'sorted_transcript_consequences',
                'variant_id',
                'xpos',
            ],
        )
        self.assertCountEqual(
            list(
                get_fields(
                    ht,
                    AnnotationType.FORMATTING,
                    env=Env.TEST,
                    dataset_type=DatasetType.SNV,
                    reference_genome=ReferenceGenome.GRCh37,
                    liftover_ref_path=LIFTOVER,
                ).keys(),
            ),
            [
                'sorted_transcript_consequences',
                'variant_id',
                'xpos',
            ],
        )

    @patch('v03_pipeline.lib.annotations.fields.hl.read_table')
    def test_get_sample_lookup_table_fields(
        self, mock_dataroot: Mock, mock_read_table: Mock,
    ) -> None:
        mock_dataroot.LOCAL_REFERENCE_DATA.value = self._temp_local_reference_data
        mock_read_table.return_value = hl.Table.parallelize(
            [
                {
                    'locus': hl.Locus('chr1', 1, ReferenceGenome.GRCh38.value),
                    'alleles': ['A', 'C'],
                    'ref_samples': {'a', 'c'},
                    'het_samples': {'b', 'd'},
                    'hom_samples': {'e', 'f'},
                },
            ],
            hl.tstruct(
                locus=hl.tlocus(ReferenceGenome.GRCh38.value),
                alleles=hl.tarray(hl.tstr),
                ref_samples=hl.tset(hl.tstr),
                het_samples=hl.tset(hl.tstr),
                hom_samples=hl.tset(hl.tstr),
            ),
            key=('locus', 'alleles'),
        )
        ht = hl.Table.parallelize(
            [],
            hl.tstruct(
                locus=hl.tlocus(ReferenceGenome.GRCh38.value),
                alleles=hl.tarray(hl.tstr),
            ),
            key=('locus', 'alleles'),
        )
        self.assertCountEqual(
            list(
                get_fields(
                    ht,
                    AnnotationType.SAMPLE_LOOKUP_TABLE,
                    env=Env.TEST,
                    dataset_type=DatasetType.SNV,
                    reference_genome=ReferenceGenome.GRCh38,
                ).keys(),
            ),
            [
                'AC',
                'AF',
                'AN',
                'hom',
            ],
        )
