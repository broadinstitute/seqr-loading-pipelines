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
from v03_pipeline.var.test.vep.mock_vep_data import MOCK_37_VEP_DATA, MOCK_38_VEP_DATA

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

    @patch('v03_pipeline.lib.vep.validate_vep_config_reference_genome')
    @patch('v03_pipeline.lib.vep.hl.vep')
    def test_get_formatting_fields(self, mock_vep: Mock, mock_validate: Mock) -> None:
        mock_validate.return_value = None
        ht = hl.read_table(TEST_COMBINED_1)
        ht = ht.annotate(rsid='abcd')
        for reference_genome, expected_fields in [
            (
                ReferenceGenome.GRCh38,
                [
                    'check_ref',
                    'screen',
                    'gnomad_non_coding_constraint',
                    'rg37_locus',
                    'rsid',
                    'sorted_motif_feature_consequences',
                    'sorted_regulatory_feature_consequences',
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
            mock_vep.return_value = ht.annotate(
                vep=MOCK_37_VEP_DATA
                if reference_genome == ReferenceGenome.GRCh37
                else MOCK_38_VEP_DATA,
            )
            ht = run_vep(
                ht,
                DatasetType.SNV_INDEL,
                reference_genome,
            )
            self.assertCountEqual(
                list(
                    get_fields(
                        ht,
                        DatasetType.SNV_INDEL.formatting_annotation_fns(
                            reference_genome,
                        ),
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
                        **(
                            {
                                'gencode_ensembl_to_refseq_id_mapping': hl.dict(
                                    {'a': 'b'},
                                ),
                            }
                            if reference_genome == ReferenceGenome.GRCh38
                            else {}
                        ),
                        dataset_type=DatasetType.SNV_INDEL,
                        reference_genome=reference_genome,
                        liftover_ref_path=LIFTOVER,
                    ).keys(),
                ),
                expected_fields,
            )

    def test_get_lookup_table_fields(
        self,
    ) -> None:
        lookup_ht = hl.Table.parallelize(
            [
                {
                    'locus': hl.Locus('chr1', 1, ReferenceGenome.GRCh38.value),
                    'alleles': ['A', 'C'],
                    'project_stats': [
                        [
                            hl.Struct(
                                ref_samples=2,
                                het_samples=2,
                                hom_samples=2,
                            ),
                        ],
                    ],
                },
            ],
            hl.tstruct(
                locus=hl.tlocus(ReferenceGenome.GRCh38.value),
                alleles=hl.tarray(hl.tstr),
                project_stats=hl.tarray(
                    hl.tarray(
                        hl.tstruct(
                            **{
                                field: hl.tint32
                                for field in DatasetType.SNV_INDEL.lookup_table_fields_and_genotype_filter_fns
                            },
                        ),
                    ),
                ),
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
                    DatasetType.SNV_INDEL.lookup_table_annotation_fns,
                    lookup_ht=lookup_ht,
                    dataset_type=DatasetType.SNV_INDEL,
                    reference_genome=ReferenceGenome.GRCh38,
                ).keys(),
            ),
            ['gt_stats'],
        )
