import shutil
from unittest.mock import Mock, patch

import hail as hl

from v03_pipeline.lib.annotations.fields import get_fields
from v03_pipeline.lib.model import (
    DatasetType,
    ReferenceGenome,
)
from v03_pipeline.lib.paths import valid_reference_dataset_path
from v03_pipeline.lib.reference_datasets.reference_dataset import ReferenceDataset
from v03_pipeline.lib.test.mocked_dataroot_testcase import MockedDatarootTestCase
from v03_pipeline.lib.vep import run_vep
from v03_pipeline.var.test.vep.mock_vep_data import MOCK_37_VEP_DATA, MOCK_38_VEP_DATA

TEST_GNOMAD_NONCODING_CONSTRAINT_38_HT = 'v03_pipeline/var/test/reference_datasets/GRCh38/gnomad_non_coding_constraint/1.0.ht'
TEST_SCREEN_38_HT = 'v03_pipeline/var/test/reference_datasets/GRCh38/screen/1.0.ht'


class FieldsTest(MockedDatarootTestCase):
    def setUp(self) -> None:
        super().setUp()
        shutil.copytree(
            TEST_GNOMAD_NONCODING_CONSTRAINT_38_HT,
            valid_reference_dataset_path(
                ReferenceGenome.GRCh38,
                ReferenceDataset.gnomad_non_coding_constraint,
            ),
        )
        shutil.copytree(
            TEST_SCREEN_38_HT,
            valid_reference_dataset_path(
                ReferenceGenome.GRCh38,
                ReferenceDataset.screen,
            ),
        )

    @patch('v03_pipeline.lib.vep.hl.vep')
    def test_get_formatting_fields(self, mock_vep: Mock) -> None:
        for reference_genome, ht, expected_fields in [
            (
                ReferenceGenome.GRCh38,
                hl.Table.parallelize(
                    [
                        {
                            'locus': hl.Locus(
                                contig='chr1',
                                position=1,
                                reference_genome='GRCh38',
                            ),
                            'alleles': ['A', 'C'],
                            'rsid': 'abcd',
                        },
                    ],
                    hl.tstruct(
                        locus=hl.tlocus('GRCh38'),
                        alleles=hl.tarray(hl.tstr),
                        rsid=hl.tstr,
                    ),
                    key=['locus', 'alleles'],
                ),
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
                hl.Table.parallelize(
                    [
                        {
                            'locus': hl.Locus(
                                contig='1',
                                position=1,
                                reference_genome='GRCh37',
                            ),
                            'alleles': ['A', 'C'],
                            'rsid': 'abcd',
                        },
                    ],
                    hl.tstruct(
                        locus=hl.tlocus('GRCh37'),
                        alleles=hl.tarray(hl.tstr),
                        rsid=hl.tstr,
                    ),
                    key=['locus', 'alleles'],
                ),
                [
                    'rg38_locus',
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
            ht = run_vep(  # noqa: PLW2901
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
                            f'{reference_dataset}_ht': hl.read_table(
                                valid_reference_dataset_path(
                                    reference_genome,
                                    reference_dataset,
                                ),
                            )
                            for reference_dataset in ReferenceDataset.for_reference_genome_dataset_type_annotations(
                                reference_genome,
                                DatasetType.SNV_INDEL,
                            )
                            if reference_dataset.formatting_annotation
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
                            **dict.fromkeys(DatasetType.SNV_INDEL.lookup_table_fields_and_genotype_filter_fns, hl.tint32),
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
                    DatasetType.SNV_INDEL.variant_frequency_annotation_fns,
                    lookup_ht=lookup_ht,
                    dataset_type=DatasetType.SNV_INDEL,
                    reference_genome=ReferenceGenome.GRCh38,
                ).keys(),
            ),
            ['gt_stats'],
        )
