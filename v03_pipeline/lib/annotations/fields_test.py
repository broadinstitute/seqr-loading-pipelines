import shutil
from unittest.mock import Mock, patch

import hail as hl

from v03_pipeline.lib.annotations.fields import get_fields
from v03_pipeline.lib.model import (
    DatasetType,
    ReferenceDatasetCollection,
    ReferenceGenome,
)
from v03_pipeline.lib.paths import valid_reference_dataset_collection_path
from v03_pipeline.lib.test.mocked_dataroot_testcase import MockedDatarootTestCase
from v03_pipeline.lib.vep import run_vep
from v03_pipeline.var.test.vep.mock_vep_data import MOCK_VEP_DATA

TEST_COMBINED_1 = 'v03_pipeline/var/test/reference_data/test_combined_1.ht'
TEST_INTERVAL_1 = 'v03_pipeline/var/test/reference_data/test_interval_1.ht'
LIFTOVER = 'v03_pipeline/var/test/liftover/grch38_to_grch37.over.chain.gz'


class FieldsTest(MockedDatarootTestCase):
    def setUp(self) -> None:
        super().setUp()
        shutil.copytree(
            TEST_INTERVAL_1,
            valid_reference_dataset_collection_path(
                ReferenceGenome.GRCh38,
                DatasetType.SNV_INDEL,
                ReferenceDatasetCollection.INTERVAL,
            ),
        )

    @patch('v03_pipeline.lib.vep.hl.vep')
    def test_get_formatting_fields(self, mock_vep: Mock) -> None:
        ht = hl.read_table(TEST_COMBINED_1)
        mock_vep.return_value = ht.annotate(vep=MOCK_VEP_DATA)
        ht = run_vep(
            ht,
            DatasetType.SNV_INDEL,
            None,
        )
        ht = ht.annotate(rsid='abcd')
        for reference_genome, expected_fields in [
            (
                ReferenceGenome.GRCh38,
                [
                    'screen',
                    'gnomad_non_coding_constraint',
                    'rg37_locus',
                    'rsid',
                    'sorted_transcript_consequences',
                    'variant_id',
                    'xpos',
                ],
            ),
            (
                ReferenceGenome.GRCh37,
                [
                    'rsid',
                    'sorted_transcript_consequences',
                    'variant_id',
                    'xpos',
                ],
            ),
        ]:
            self.assertCountEqual(
                list(
                    get_fields(
                        ht,
                        DatasetType.SNV_INDEL.formatting_annotation_fns,
                        **{
                            f'{rdc.value}_ht': hl.read_table(
                                valid_reference_dataset_collection_path(
                                    reference_genome,
                                    DatasetType.SNV_INDEL,
                                    rdc,
                                ),
                            )
                            for rdc in ReferenceDatasetCollection.for_reference_genome_dataset_type(
                                reference_genome,
                                DatasetType.SNV_INDEL,
                            )
                            if rdc.requires_annotation
                        },
                        dataset_type=DatasetType.SNV_INDEL,
                        reference_genome=reference_genome,
                        liftover_ref_path=LIFTOVER,
                    ).keys(),
                ),
                expected_fields,
            )

    def test_get_sample_lookup_table_fields(
        self,
    ) -> None:
        sample_lookup_ht = hl.Table.parallelize(
            [
                {
                    'locus': hl.Locus('chr1', 1, ReferenceGenome.GRCh38.value),
                    'alleles': ['A', 'C'],
                    'ref_samples': hl.Struct(project_1={'a', 'c'}),
                    'het_samples': hl.Struct(project_1={'b', 'd'}),
                    'hom_samples': hl.Struct(project_1={'e', 'f'}),
                },
            ],
            hl.tstruct(
                locus=hl.tlocus(ReferenceGenome.GRCh38.value),
                alleles=hl.tarray(hl.tstr),
                ref_samples=hl.tstruct(project_1=hl.tset(hl.tstr)),
                het_samples=hl.tstruct(project_1=hl.tset(hl.tstr)),
                hom_samples=hl.tstruct(project_1=hl.tset(hl.tstr)),
            ),
            key=('locus', 'alleles'),
            globals=hl.Struct(
                updates=hl.set([hl.Struct(callset='abc', project_guid='project_1')]),
            ),
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
                    DatasetType.SNV_INDEL.sample_lookup_table_annotation_fns,
                    sample_lookup_ht=sample_lookup_ht,
                    dataset_type=DatasetType.SNV_INDEL,
                    reference_genome=ReferenceGenome.GRCh38,
                ).keys(),
            ),
            ['gt_stats'],
        )
