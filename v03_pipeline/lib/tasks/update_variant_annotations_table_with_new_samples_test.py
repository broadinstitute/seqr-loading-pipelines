import shutil
from functools import partial
from unittest.mock import Mock, PropertyMock, patch

import hail as hl
import luigi.worker

from v03_pipeline.lib.annotations.enums import (
    BIOTYPES,
    CLINVAR_PATHOGENICITIES,
    CONSEQUENCE_TERMS,
    LOF_FILTERS,
    MITOTIP_PATHOGENICITIES,
    SV_CONSEQUENCE_RANKS,
    SV_TYPE_DETAILS,
    SV_TYPES,
)
from v03_pipeline.lib.misc.validation import validate_expected_contig_frequency
from v03_pipeline.lib.model import (
    CachedReferenceDatasetQuery,
    DatasetType,
    ReferenceDatasetCollection,
    ReferenceGenome,
    SampleType,
)
from v03_pipeline.lib.paths import (
    valid_cached_reference_dataset_query_path,
    valid_reference_dataset_collection_path,
)
from v03_pipeline.lib.reference_data.clinvar import CLINVAR_ASSERTIONS
from v03_pipeline.lib.tasks.base.base_update_variant_annotations_table import (
    BaseUpdateVariantAnnotationsTableTask,
)
from v03_pipeline.lib.tasks.files import GCSorLocalFolderTarget
from v03_pipeline.lib.tasks.update_variant_annotations_table_with_new_samples import (
    UpdateVariantAnnotationsTableWithNewSamplesTask,
)
from v03_pipeline.lib.test.mock_complete_task import MockCompleteTask
from v03_pipeline.lib.test.mocked_dataroot_testcase import MockedDatarootTestCase
from v03_pipeline.var.test.vep.mock_vep_data import MOCK_VEP_DATA

TEST_LIFTOVER = 'v03_pipeline/var/test/liftover/grch38_to_grch37.over.chain.gz'
TEST_MITO_MT = 'v03_pipeline/var/test/callsets/mito_1.mt'
TEST_SNV_INDEL_VCF = 'v03_pipeline/var/test/callsets/1kg_30variants.vcf'
TEST_SV_VCF = 'v03_pipeline/var/test/callsets/sv_1.vcf'
TEST_GCNV_BED_FILE = 'v03_pipeline/var/test/callsets/gcnv_1.tsv'
TEST_REMAP = 'v03_pipeline/var/test/remaps/test_remap_1.tsv'
TEST_PEDIGREE_3 = 'v03_pipeline/var/test/pedigrees/test_pedigree_3.tsv'
TEST_PEDIGREE_4 = 'v03_pipeline/var/test/pedigrees/test_pedigree_4.tsv'
TEST_PEDIGREE_5 = 'v03_pipeline/var/test/pedigrees/test_pedigree_5.tsv'
TEST_COMBINED_1 = 'v03_pipeline/var/test/reference_data/test_combined_1.ht'
TEST_COMBINED_37 = 'v03_pipeline/var/test/reference_data/test_combined_37.ht'
TEST_COMBINED_MITO_1 = 'v03_pipeline/var/test/reference_data/test_combined_mito_1.ht'
TEST_HGMD_1 = 'v03_pipeline/var/test/reference_data/test_hgmd_1.ht'
TEST_HGMD_37 = 'v03_pipeline/var/test/reference_data/test_hgmd_37.ht'
TEST_INTERVAL_1 = 'v03_pipeline/var/test/reference_data/test_interval_1.ht'
TEST_INTERVAL_MITO_1 = 'v03_pipeline/var/test/reference_data/test_interval_mito_1.ht'

GENE_ID_MAPPING = {
    'OR4F5': 'ENSG00000186092',
    'PLEKHG4B': 'ENSG00000153404',
    'OR4F16': 'ENSG00000186192',
    'OR4F29': 'ENSG00000284733',
    'FBXO28': 'ENSG00000143756',
    'SAMD11': 'ENSG00000187634',
    'C1orf174': 'ENSG00000198912',
    'TAS1R1': 'ENSG00000173662',
    'FAM131C': 'ENSG00000185519',
    'RCC2': 'ENSG00000179051',
    'NBPF3': 'ENSG00000142794',
    'AGBL4': 'ENSG00000186094',
    'KIAA1614': 'ENSG00000135835',
    'MR1': 'ENSG00000153029',
    'STX6': 'ENSG00000135823',
    'XPR1': 'ENSG00000143324',
}

TEST_RUN_ID = 'manual__2024-04-03'


@patch(
    'v03_pipeline.lib.tasks.base.base_update_variant_annotations_table.UpdatedReferenceDatasetCollectionTask',
)
class UpdateVariantAnnotationsTableWithNewSamplesTaskTest(MockedDatarootTestCase):
    def setUp(self) -> None:
        super().setUp()
        shutil.copytree(
            TEST_COMBINED_1,
            valid_reference_dataset_collection_path(
                ReferenceGenome.GRCh38,
                DatasetType.SNV_INDEL,
                ReferenceDatasetCollection.COMBINED,
            ),
        )
        shutil.copytree(
            TEST_COMBINED_37,
            valid_reference_dataset_collection_path(
                ReferenceGenome.GRCh37,
                DatasetType.SNV_INDEL,
                ReferenceDatasetCollection.COMBINED,
            ),
        )
        shutil.copytree(
            TEST_HGMD_1,
            valid_reference_dataset_collection_path(
                ReferenceGenome.GRCh38,
                DatasetType.SNV_INDEL,
                ReferenceDatasetCollection.HGMD,
            ),
        )
        shutil.copytree(
            TEST_HGMD_37,
            valid_reference_dataset_collection_path(
                ReferenceGenome.GRCh37,
                DatasetType.SNV_INDEL,
                ReferenceDatasetCollection.HGMD,
            ),
        )
        shutil.copytree(
            TEST_INTERVAL_1,
            valid_reference_dataset_collection_path(
                ReferenceGenome.GRCh38,
                DatasetType.SNV_INDEL,
                ReferenceDatasetCollection.INTERVAL,
            ),
        )
        shutil.copytree(
            TEST_COMBINED_MITO_1,
            valid_reference_dataset_collection_path(
                ReferenceGenome.GRCh38,
                DatasetType.MITO,
                ReferenceDatasetCollection.COMBINED,
            ),
        )
        shutil.copytree(
            TEST_INTERVAL_MITO_1,
            valid_reference_dataset_collection_path(
                ReferenceGenome.GRCh38,
                DatasetType.MITO,
                ReferenceDatasetCollection.INTERVAL,
            ),
        )

    @patch(
        'v03_pipeline.lib.tasks.write_new_variants_table.UpdateVariantAnnotationsTableWithUpdatedReferenceDataset',
    )
    def test_missing_pedigree(
        self,
        mock_update_vat_with_rdc_task,
        mock_update_rdc_task,
    ) -> None:
        mock_update_rdc_task.return_value = MockCompleteTask()
        mock_update_vat_with_rdc_task.return_value = MockCompleteTask()
        uvatwns_task = UpdateVariantAnnotationsTableWithNewSamplesTask(
            reference_genome=ReferenceGenome.GRCh38,
            dataset_type=DatasetType.SNV_INDEL,
            sample_type=SampleType.WGS,
            callset_paths=[TEST_SNV_INDEL_VCF],
            project_guids=['R0113_test_project'],
            project_remap_paths=[TEST_REMAP],
            project_pedigree_paths=['bad_pedigree'],
            validate=False,
            liftover_ref_path=TEST_LIFTOVER,
            run_id=TEST_RUN_ID,
        )
        worker = luigi.worker.Worker()
        worker.add(uvatwns_task)
        worker.run()
        self.assertFalse(uvatwns_task.complete())

    @patch(
        'v03_pipeline.lib.tasks.write_new_variants_table.UpdateVariantAnnotationsTableWithUpdatedReferenceDataset',
    )
    def test_missing_interval_reference(
        self,
        mock_update_vat_with_rdc_task,
        mock_update_rdc_task,
    ) -> None:
        mock_update_rdc_task.return_value = MockCompleteTask()
        mock_update_vat_with_rdc_task.return_value = MockCompleteTask()
        shutil.rmtree(
            valid_reference_dataset_collection_path(
                ReferenceGenome.GRCh38,
                DatasetType.SNV_INDEL,
                ReferenceDatasetCollection.INTERVAL,
            ),
        )
        uvatwns_task = UpdateVariantAnnotationsTableWithNewSamplesTask(
            reference_genome=ReferenceGenome.GRCh38,
            dataset_type=DatasetType.SNV_INDEL,
            sample_type=SampleType.WGS,
            callset_paths=[TEST_SNV_INDEL_VCF],
            project_guids=['R0113_test_project'],
            project_remap_paths=[TEST_REMAP],
            project_pedigree_paths=[TEST_PEDIGREE_3],
            validate=False,
            liftover_ref_path=TEST_LIFTOVER,
            run_id=TEST_RUN_ID,
        )
        worker = luigi.worker.Worker()
        worker.add(uvatwns_task)
        worker.run()
        self.assertFalse(uvatwns_task.complete())

    @patch(
        'v03_pipeline.lib.tasks.write_new_variants_table.UpdateVariantAnnotationsTableWithUpdatedReferenceDataset',
    )
    @patch(
        'v03_pipeline.lib.tasks.write_imported_callset.validate_expected_contig_frequency',
        partial(validate_expected_contig_frequency, min_rows_per_contig=25),
    )
    @patch.object(ReferenceGenome, 'standard_contigs', new_callable=PropertyMock)
    @patch('v03_pipeline.lib.vep.hl.vep')
    @patch('v03_pipeline.lib.vep.validate_vep_config_reference_genome')
    def test_multiple_update_vat(
        self,
        mock_vep_validate: Mock,
        mock_vep: Mock,
        mock_standard_contigs: Mock,
        mock_update_vat_with_rdc_task: Mock,
        mock_update_rdc_task: Mock,
    ) -> None:
        mock_update_rdc_task.return_value = MockCompleteTask()
        mock_update_vat_with_rdc_task.return_value = (
            BaseUpdateVariantAnnotationsTableTask(
                reference_genome=ReferenceGenome.GRCh38,
                dataset_type=DatasetType.SNV_INDEL,
                sample_type=SampleType.WGS,
            )
        )
        mock_vep.side_effect = lambda ht, **_: ht.annotate(vep=MOCK_VEP_DATA)
        mock_vep_validate.return_value = None
        mock_standard_contigs.return_value = {'chr1'}
        # This creates a mock validation table with 1 coding and 1 non-coding variant
        # explicitly chosen from the VCF.
        coding_and_noncoding_variants_ht = hl.Table.parallelize(
            [
                {
                    'locus': hl.Locus(
                        contig='chr1',
                        position=871269,
                        reference_genome='GRCh38',
                    ),
                    'coding': False,
                    'noncoding': True,
                },
                {
                    'locus': hl.Locus(
                        contig='chr1',
                        position=876499,
                        reference_genome='GRCh38',
                    ),
                    'coding': True,
                    'noncoding': False,
                },
            ],
            hl.tstruct(
                locus=hl.tlocus('GRCh38'),
                coding=hl.tbool,
                noncoding=hl.tbool,
            ),
            key='locus',
        )
        coding_and_noncoding_variants_ht.write(
            valid_cached_reference_dataset_query_path(
                ReferenceGenome.GRCh38,
                DatasetType.SNV_INDEL,
                CachedReferenceDatasetQuery.GNOMAD_CODING_AND_NONCODING_VARIANTS,
            ),
        )
        worker = luigi.worker.Worker()
        uvatwns_task_3 = UpdateVariantAnnotationsTableWithNewSamplesTask(
            reference_genome=ReferenceGenome.GRCh38,
            dataset_type=DatasetType.SNV_INDEL,
            sample_type=SampleType.WGS,
            callset_paths=[TEST_SNV_INDEL_VCF],
            project_guids=['R0113_test_project'],
            project_remap_paths=[TEST_REMAP],
            project_pedigree_paths=[TEST_PEDIGREE_3],
            validate=True,
            liftover_ref_path=TEST_LIFTOVER,
            run_id=TEST_RUN_ID,
        )
        worker.add(uvatwns_task_3)
        worker.run()
        self.assertTrue(uvatwns_task_3.complete())
        ht = hl.read_table(uvatwns_task_3.output().path)
        self.assertEqual(ht.count(), 30)
        self.assertEqual(
            [
                x
                for x in ht.select(
                    'gt_stats',
                ).collect()
                if x.locus.position <= 871269  # noqa: PLR2004
            ],
            [
                hl.Struct(
                    locus=hl.Locus(
                        contig='chr1',
                        position=871269,
                        reference_genome='GRCh38',
                    ),
                    alleles=['A', 'C'],
                    gt_stats=hl.Struct(AC=0, AN=6, AF=0.0, hom=0),
                ),
            ],
        )
        self.assertEqual(
            ht.globals.updates.collect(),
            [
                {
                    hl.Struct(
                        callset=TEST_SNV_INDEL_VCF,
                        project_guid='R0113_test_project',
                    ),
                },
            ],
        )

        # Ensure that new variants are added correctly to the table.
        uvatwns_task_4 = UpdateVariantAnnotationsTableWithNewSamplesTask(
            reference_genome=ReferenceGenome.GRCh38,
            dataset_type=DatasetType.SNV_INDEL,
            sample_type=SampleType.WGS,
            callset_paths=[TEST_SNV_INDEL_VCF],
            project_guids=['R0114_project4'],
            project_remap_paths=[TEST_REMAP],
            project_pedigree_paths=[TEST_PEDIGREE_4],
            validate=True,
            liftover_ref_path=TEST_LIFTOVER,
            run_id=TEST_RUN_ID,
        )
        worker.add(uvatwns_task_4)
        worker.run()
        self.assertTrue(uvatwns_task_4.complete())
        ht = hl.read_table(uvatwns_task_4.output().path)
        self.assertCountEqual(
            [
                x
                for x in ht.select(
                    'cadd',
                    'clinvar',
                    'hgmd',
                    'variant_id',
                    'xpos',
                    'gt_stats',
                    'screen',
                ).collect()
                if x.locus.position <= 878809  # noqa: PLR2004
            ],
            [
                hl.Struct(
                    locus=hl.Locus(
                        contig='chr1',
                        position=871269,
                        reference_genome='GRCh38',
                    ),
                    alleles=['A', 'C'],
                    cadd=hl.Struct(PHRED=2),
                    clinvar=hl.Struct(
                        alleleId=None,
                        conflictingPathogenicities=None,
                        goldStars=None,
                        pathogenicity_id=None,
                        assertion_ids=None,
                        submitters=None,
                        conditions=None,
                    ),
                    hgmd=hl.Struct(
                        accession='abcdefg',
                        class_id=3,
                    ),
                    variant_id='1-871269-A-C',
                    xpos=1000871269,
                    gt_stats=hl.Struct(AC=1, AN=32, AF=0.03125, hom=0),
                    screen=hl.Struct(region_type_ids=[1]),
                ),
                hl.Struct(
                    locus=hl.Locus(
                        contig='chr1',
                        position=874734,
                        reference_genome='GRCh38',
                    ),
                    alleles=['C', 'T'],
                    cadd=None,
                    clinvar=None,
                    hgmd=None,
                    variant_id='1-874734-C-T',
                    xpos=1000874734,
                    gt_stats=hl.Struct(AC=1, AN=32, AF=0.03125, hom=0),
                    screen=hl.Struct(region_type_ids=[]),
                ),
                hl.Struct(
                    locus=hl.Locus(
                        contig='chr1',
                        position=876499,
                        reference_genome='GRCh38',
                    ),
                    alleles=['A', 'G'],
                    cadd=None,
                    clinvar=None,
                    hgmd=None,
                    variant_id='1-876499-A-G',
                    xpos=1000876499,
                    gt_stats=hl.Struct(AC=31, AN=32, AF=0.96875, hom=15),
                    screen=hl.Struct(region_type_ids=[]),
                ),
                hl.Struct(
                    locus=hl.Locus(
                        contig='chr1',
                        position=878314,
                        reference_genome='GRCh38',
                    ),
                    alleles=['G', 'C'],
                    cadd=None,
                    clinvar=None,
                    hgmd=None,
                    variant_id='1-878314-G-C',
                    xpos=1000878314,
                    gt_stats=hl.Struct(AC=3, AN=32, AF=0.09375, hom=0),
                    screen=hl.Struct(region_type_ids=[]),
                ),
                hl.Struct(
                    locus=hl.Locus(
                        contig='chr1',
                        position=878809,
                        reference_genome='GRCh38',
                    ),
                    alleles=['C', 'T'],
                    cadd=None,
                    clinvar=None,
                    hgmd=None,
                    variant_id='1-878809-C-T',
                    xpos=1000878809,
                    gt_stats=hl.Struct(AC=1, AN=32, AF=0.03125, hom=0),
                    screen=hl.Struct(region_type_ids=[]),
                ),
            ],
        )
        self.assertCountEqual(
            ht.filter(
                ht.locus.position <= 878809,  # noqa: PLR2004
            ).sorted_transcript_consequences.consequence_term_ids.collect(),
            [
                [[11], [22, 26], [22, 26]],
                [[11], [22, 26], [22, 26]],
                [[11], [22, 26], [22, 26]],
                [[11], [22, 26], [22, 26]],
                [[11], [22, 26], [22, 26]],
            ],
        )
        self.assertCountEqual(
            ht.globals.collect(),
            [
                hl.Struct(
                    updates={
                        hl.Struct(
                            callset='v03_pipeline/var/test/callsets/1kg_30variants.vcf',
                            project_guid='R0113_test_project',
                        ),
                        hl.Struct(
                            callset='v03_pipeline/var/test/callsets/1kg_30variants.vcf',
                            project_guid='R0114_project4',
                        ),
                    },
                    paths=hl.Struct(
                        cadd='gs://seqr-reference-data/GRCh37/CADD/CADD_snvs_and_indels.v1.6.ht',
                        clinvar='ftp://ftp.ncbi.nlm.nih.gov/pub/clinvar/vcf_GRCh37/clinvar.vcf.gz',
                        dbnsfp='gs://seqr-reference-data/GRCh37/dbNSFP/v2.9.3/dbNSFP2.9.3_variant.ht',
                        eigen='gs://seqr-reference-data/GRCh37/eigen/EIGEN_coding_noncoding.grch37.ht',
                        exac='gs://seqr-reference-data/GRCh37/gnomad/ExAC.r1.sites.vep.ht',
                        gnomad_exomes='gs://gcp-public-data--gnomad/release/4.1/ht/exomes/gnomad.exomes.v4.1.sites.ht',
                        gnomad_genomes='gs://gcp-public-data--gnomad/release/4.1/ht/genomes/gnomad.genomes.v4.1.sites.ht',
                        mpc='gs://seqr-reference-data/GRCh37/MPC/fordist_constraint_official_mpc_values.ht',
                        primate_ai='gs://seqr-reference-data/GRCh37/primate_ai/PrimateAI_scores_v0.2.ht',
                        splice_ai='gs://seqr-reference-data/GRCh37/spliceai/spliceai_scores.ht',
                        topmed='gs://seqr-reference-data/GRCh37/TopMed/bravo-dbsnp-all.removed_chr_prefix.liftunder_GRCh37.ht',
                        gnomad_non_coding_constraint='gs://seqr-reference-data/GRCh38/gnomad_nc_constraint/gnomad_non-coding_constraint_z_scores.ht',
                        screen='gs://seqr-reference-data/GRCh38/ccREs/GRCh38-ccREs.ht',
                        hgmd='gs://seqr-reference-data-private/GRCh38/HGMD/HGMD_Pro_2023.1_hg38.vcf.gz',
                    ),
                    versions=hl.Struct(
                        cadd='v1.6',
                        clinvar='2023-11-26',
                        dbnsfp='2.9.3',
                        eigen=None,
                        exac=None,
                        gnomad_exomes='v4.1',
                        gnomad_genomes='v4.1',
                        mpc=None,
                        primate_ai='v0.2',
                        splice_ai=None,
                        topmed=None,
                        gnomad_non_coding_constraint=None,
                        screen=None,
                        hgmd='HGMD_Pro_2023',
                    ),
                    enums=hl.Struct(
                        cadd=hl.Struct(),
                        clinvar=hl.Struct(
                            assertion=CLINVAR_ASSERTIONS,
                            pathogenicity=CLINVAR_PATHOGENICITIES,
                        ),
                        dbnsfp=hl.Struct(
                            MutationTaster_pred=['D', 'A', 'N', 'P'],
                        ),
                        eigen=hl.Struct(),
                        exac=hl.Struct(),
                        gnomad_exomes=hl.Struct(),
                        gnomad_genomes=hl.Struct(),
                        mpc=hl.Struct(),
                        primate_ai=hl.Struct(),
                        splice_ai=hl.Struct(
                            splice_consequence=[
                                'Acceptor gain',
                                'Acceptor loss',
                                'Donor gain',
                                'Donor loss',
                                'No consequence',
                            ],
                        ),
                        topmed=hl.Struct(),
                        hgmd=hl.Struct(
                            **{'class': ['DM', 'DM?', 'DP', 'DFP', 'FP', 'R']},
                        ),
                        gnomad_non_coding_constraint=hl.Struct(),
                        screen=hl.Struct(
                            region_type=[
                                'CTCF-bound',
                                'CTCF-only',
                                'DNase-H3K4me3',
                                'PLS',
                                'dELS',
                                'pELS',
                                'DNase-only',
                                'low-DNase',
                            ],
                        ),
                        sorted_transcript_consequences=hl.Struct(
                            biotype=BIOTYPES,
                            consequence_term=CONSEQUENCE_TERMS,
                            lof_filter=LOF_FILTERS,
                        ),
                    ),
                ),
            ],
        )

    @patch(
        'v03_pipeline.lib.tasks.write_new_variants_table.UpdateVariantAnnotationsTableWithUpdatedReferenceDataset',
    )
    @patch('v03_pipeline.lib.vep.hl.vep')
    @patch('v03_pipeline.lib.vep.validate_vep_config_reference_genome')
    def test_update_vat_grch37(
        self,
        mock_vep_validate: Mock,
        mock_vep: Mock,
        mock_update_vat_with_rdc_task: Mock,
        mock_update_rdc_task: Mock,
    ) -> None:
        mock_update_rdc_task.return_value = MockCompleteTask()
        mock_update_vat_with_rdc_task.return_value = (
            BaseUpdateVariantAnnotationsTableTask(
                reference_genome=ReferenceGenome.GRCh37,
                dataset_type=DatasetType.SNV_INDEL,
                sample_type=SampleType.WGS,
            )
        )
        mock_vep.side_effect = lambda ht, **_: ht.annotate(vep=MOCK_VEP_DATA)
        mock_vep_validate.return_value = None
        worker = luigi.worker.Worker()
        uvatwns_task = UpdateVariantAnnotationsTableWithNewSamplesTask(
            reference_genome=ReferenceGenome.GRCh37,
            dataset_type=DatasetType.SNV_INDEL,
            sample_type=SampleType.WGS,
            callset_paths=[TEST_SNV_INDEL_VCF],
            project_guids=['R0113_test_project'],
            project_remap_paths=[TEST_REMAP],
            project_pedigree_paths=[TEST_PEDIGREE_3],
            validate=False,
            liftover_ref_path=TEST_LIFTOVER,
            run_id=TEST_RUN_ID,
        )
        worker.add(uvatwns_task)
        worker.run()
        self.assertTrue(uvatwns_task.complete())
        ht = hl.read_table(uvatwns_task.output().path)
        self.assertEqual(ht.count(), 30)
        self.assertCountEqual(
            ht.globals.paths.collect(),
            [
                hl.Struct(
                    cadd='gs://seqr-reference-data/GRCh37/CADD/CADD_snvs_and_indels.v1.6.ht',
                    clinvar='ftp://ftp.ncbi.nlm.nih.gov/pub/clinvar/vcf_GRCh37/clinvar.vcf.gz',
                    dbnsfp='gs://seqr-reference-data/GRCh37/dbNSFP/v2.9.3/dbNSFP2.9.3_variant.ht',
                    eigen='gs://seqr-reference-data/GRCh37/eigen/EIGEN_coding_noncoding.grch37.ht',
                    exac='gs://seqr-reference-data/GRCh37/gnomad/ExAC.r1.sites.vep.ht',
                    hgmd='gs://seqr-reference-data-private/GRCh37/HGMD/HGMD_Pro_2023.1_hg19.vcf.gz',
                    gnomad_exomes='gs://gcp-public-data--gnomad/release/2.1.1/ht/exomes/gnomad.exomes.r2.1.1.sites.ht',
                    gnomad_genomes='gs://gcp-public-data--gnomad/release/2.1.1/ht/genomes/gnomad.genomes.r2.1.1.sites.ht',
                    mpc='gs://seqr-reference-data/GRCh37/MPC/fordist_constraint_official_mpc_values.ht',
                    primate_ai='gs://seqr-reference-data/GRCh37/primate_ai/PrimateAI_scores_v0.2.ht',
                    splice_ai='gs://seqr-reference-data/GRCh37/spliceai/spliceai_scores.ht',
                    topmed='gs://seqr-reference-data/GRCh37/TopMed/bravo-dbsnp-all.removed_chr_prefix.liftunder_GRCh37.ht',
                ),
            ],
        )
        self.assertFalse(hasattr(ht, 'rg37_locus'))

    @patch(
        'v03_pipeline.lib.tasks.write_new_variants_table.UpdateVariantAnnotationsTableWithUpdatedReferenceDataset',
    )
    @patch('v03_pipeline.lib.model.reference_dataset_collection.Env')
    @patch('v03_pipeline.lib.vep.hl.vep')
    @patch('v03_pipeline.lib.vep.validate_vep_config_reference_genome')
    def test_update_vat_without_accessing_private_datasets(
        self,
        mock_vep_validate: Mock,
        mock_vep: Mock,
        mock_rdc_env: Mock,
        mock_update_vat_with_rdc_task: Mock,
        mock_update_rdc_task: Mock,
    ) -> None:
        mock_update_rdc_task.return_value = MockCompleteTask()
        mock_update_vat_with_rdc_task.return_value = (
            BaseUpdateVariantAnnotationsTableTask(
                reference_genome=ReferenceGenome.GRCh38,
                dataset_type=DatasetType.SNV_INDEL,
                sample_type=SampleType.WGS,
            )
        )
        shutil.rmtree(
            valid_reference_dataset_collection_path(
                ReferenceGenome.GRCh38,
                DatasetType.SNV_INDEL,
                ReferenceDatasetCollection.HGMD,
            ),
        )
        mock_rdc_env.ACCESS_PRIVATE_REFERENCE_DATASETS = False
        mock_vep.side_effect = lambda ht, **_: ht.annotate(vep=MOCK_VEP_DATA)
        mock_vep_validate.return_value = None
        worker = luigi.worker.Worker()
        uvatwns_task = UpdateVariantAnnotationsTableWithNewSamplesTask(
            reference_genome=ReferenceGenome.GRCh38,
            dataset_type=DatasetType.SNV_INDEL,
            sample_type=SampleType.WGS,
            callset_paths=[TEST_SNV_INDEL_VCF],
            project_guids=['R0113_test_project'],
            project_remap_paths=[TEST_REMAP],
            project_pedigree_paths=[TEST_PEDIGREE_3],
            validate=False,
            liftover_ref_path=TEST_LIFTOVER,
            run_id=TEST_RUN_ID,
        )
        worker.add(uvatwns_task)
        worker.run()
        self.assertTrue(uvatwns_task.complete())
        ht = hl.read_table(uvatwns_task.output().path)
        self.assertEqual(ht.count(), 30)
        self.assertCountEqual(
            ht.globals.versions.collect(),
            [
                hl.Struct(
                    cadd='v1.6',
                    clinvar='2023-11-26',
                    dbnsfp='2.9.3',
                    eigen=None,
                    exac=None,
                    gnomad_exomes='r2.1.1',
                    gnomad_genomes='r2.1.1',
                    mpc=None,
                    primate_ai='v0.2',
                    splice_ai=None,
                    topmed=None,
                    gnomad_non_coding_constraint=None,
                    screen=None,
                ),
            ],
        )

    @patch(
        'v03_pipeline.lib.tasks.write_new_variants_table.UpdateVariantAnnotationsTableWithUpdatedReferenceDataset',
    )
    def test_mito_update_vat(
        self,
        mock_update_vat_with_rdc_task: Mock,
        mock_update_rdc_task: Mock,
    ) -> None:
        mock_update_rdc_task.return_value = MockCompleteTask()
        mock_update_vat_with_rdc_task.return_value = (
            BaseUpdateVariantAnnotationsTableTask(
                reference_genome=ReferenceGenome.GRCh38,
                dataset_type=DatasetType.MITO,
                sample_type=SampleType.WGS,
            )
        )
        worker = luigi.worker.Worker()
        update_variant_annotations_task = (
            UpdateVariantAnnotationsTableWithNewSamplesTask(
                reference_genome=ReferenceGenome.GRCh38,
                dataset_type=DatasetType.MITO,
                sample_type=SampleType.WGS,
                callset_paths=[TEST_MITO_MT],
                project_guids=['R0115_test_project2'],
                project_remap_paths=['not_a_real_file'],
                project_pedigree_paths=[TEST_PEDIGREE_5],
                validate=False,
                liftover_ref_path=TEST_LIFTOVER,
                run_id=TEST_RUN_ID,
            )
        )
        worker.add(update_variant_annotations_task)
        worker.run()
        self.assertTrue(update_variant_annotations_task.complete())
        ht = hl.read_table(update_variant_annotations_task.output().path)
        self.assertEqual(ht.count(), 5)
        self.assertCountEqual(
            ht.globals.collect(),
            [
                hl.Struct(
                    paths=hl.Struct(
                        high_constraint_region_mito='gs://seqr-reference-data/GRCh38/mitochondrial/Helix high constraint intervals Feb-15-2022.tsv',
                        clinvar_mito='ftp://ftp.ncbi.nlm.nih.gov/pub/clinvar/vcf_GRCh38/clinvar.vcf.gz',
                        dbnsfp_mito='gs://seqr-reference-data/GRCh38/dbNSFP/v4.2/dbNSFP4.2a_variant.with_new_scores.ht',
                        gnomad_mito='gs://gcp-public-data--gnomad/release/3.1/ht/genomes/gnomad.genomes.v3.1.sites.chrM.ht',
                        helix_mito='gs://seqr-reference-data/GRCh38/mitochondrial/Helix/HelixMTdb_20200327.ht',
                        hmtvar='gs://seqr-reference-data/GRCh38/mitochondrial/HmtVar/HmtVar%20Jan.%2010%202022.ht',
                        mitomap='gs://seqr-reference-data/GRCh38/mitochondrial/MITOMAP/mitomap-confirmed-mutations-2022-02-04.ht',
                        mitimpact='gs://seqr-reference-data/GRCh38/mitochondrial/MitImpact/MitImpact_db_3.0.7.ht',
                    ),
                    versions=hl.Struct(
                        high_constraint_region_mito='Feb-15-2022',
                        clinvar_mito='2023-07-22',
                        dbnsfp_mito='4.2',
                        gnomad_mito='v3.1',
                        helix_mito='20200327',
                        hmtvar='Jan. 10 2022',
                        mitomap='Feb. 04 2022',
                        mitimpact='3.0.7',
                    ),
                    enums=hl.Struct(
                        high_constraint_region_mito=hl.Struct(),
                        clinvar_mito=hl.Struct(
                            assertion=CLINVAR_ASSERTIONS,
                            pathogenicity=CLINVAR_PATHOGENICITIES,
                        ),
                        dbnsfp_mito=hl.Struct(
                            MutationTaster_pred=['D', 'A', 'N', 'P'],
                        ),
                        gnomad_mito=hl.Struct(),
                        helix_mito=hl.Struct(),
                        hmtvar=hl.Struct(),
                        mitomap=hl.Struct(),
                        mitimpact=hl.Struct(),
                        sorted_transcript_consequences=hl.Struct(
                            biotype=BIOTYPES,
                            consequence_term=CONSEQUENCE_TERMS,
                            lof_filter=LOF_FILTERS,
                        ),
                        mitotip=hl.Struct(trna_prediction=MITOTIP_PATHOGENICITIES),
                    ),
                    updates={
                        hl.Struct(
                            callset='v03_pipeline/var/test/callsets/mito_1.mt',
                            project_guid='R0115_test_project2',
                        ),
                    },
                ),
            ],
        )
        self.assertCountEqual(
            ht.collect(),
            [
                hl.Struct(
                    locus=hl.Locus(
                        contig='chrM',
                        position=3,
                        reference_genome='GRCh38',
                    ),
                    alleles=['T', 'C'],
                    common_low_heteroplasmy=False,
                    haplogroup=hl.Struct(is_defining=False),
                    high_constraint_region_mito=True,
                    mitotip=hl.Struct(trna_prediction_id=None),
                    rg37_locus=hl.Locus(
                        contig='MT',
                        position=3,
                        reference_genome='GRCh37',
                    ),
                    rsid=None,
                    sorted_transcript_consequences=None,
                    variant_id='M-3-T-C',
                    xpos=25000000003,
                    clinvar_mito=None,
                    dbnsfp_mito=None,
                    gnomad_mito=None,
                    helix_mito=None,
                    hmtvar=None,
                    mitomap=None,
                    mitimpact=None,
                    gt_stats=hl.Struct(
                        AC_het=1,
                        AF_het=0.25,
                        AC_hom=0,
                        AF_hom=0.0,
                        AN=4,
                    ),
                ),
                hl.Struct(
                    locus=hl.Locus(
                        contig='chrM',
                        position=8,
                        reference_genome='GRCh38',
                    ),
                    alleles=['G', 'T'],
                    common_low_heteroplasmy=False,
                    haplogroup=hl.Struct(is_defining=False),
                    high_constraint_region_mito=True,
                    mitotip=hl.Struct(trna_prediction_id=None),
                    rg37_locus=hl.Locus(
                        contig='MT',
                        position=8,
                        reference_genome='GRCh37',
                    ),
                    rsid=None,
                    sorted_transcript_consequences=None,
                    variant_id='M-8-G-T',
                    xpos=25000000008,
                    clinvar_mito=None,
                    dbnsfp_mito=None,
                    gnomad_mito=None,
                    helix_mito=None,
                    hmtvar=None,
                    mitomap=None,
                    mitimpact=None,
                    gt_stats=hl.Struct(
                        AC_het=1,
                        AF_het=0.25,
                        AC_hom=0,
                        AF_hom=0.0,
                        AN=4,
                    ),
                ),
                hl.Struct(
                    locus=hl.Locus(
                        contig='chrM',
                        position=12,
                        reference_genome='GRCh38',
                    ),
                    alleles=['T', 'C'],
                    common_low_heteroplasmy=False,
                    haplogroup=hl.Struct(is_defining=False),
                    high_constraint_region_mito=False,
                    mitotip=hl.Struct(trna_prediction_id=None),
                    rg37_locus=hl.Locus(
                        contig='MT',
                        position=12,
                        reference_genome='GRCh37',
                    ),
                    rsid=None,
                    sorted_transcript_consequences=None,
                    variant_id='M-12-T-C',
                    xpos=25000000012,
                    clinvar_mito=None,
                    dbnsfp_mito=None,
                    gnomad_mito=None,
                    helix_mito=None,
                    hmtvar=None,
                    mitomap=None,
                    mitimpact=None,
                    gt_stats=hl.Struct(
                        AC_het=1,
                        AF_het=0.25,
                        AC_hom=0,
                        AF_hom=0.0,
                        AN=4,
                    ),
                ),
                hl.Struct(
                    locus=hl.Locus(
                        contig='chrM',
                        position=16,
                        reference_genome='GRCh38',
                    ),
                    alleles=['A', 'T'],
                    common_low_heteroplasmy=False,
                    haplogroup=hl.Struct(is_defining=True),
                    high_constraint_region_mito=False,
                    mitotip=hl.Struct(trna_prediction_id=None),
                    rg37_locus=hl.Locus(
                        contig='MT',
                        position=16,
                        reference_genome='GRCh37',
                    ),
                    rsid='rs1556422363',
                    sorted_transcript_consequences=None,
                    variant_id='M-16-A-T',
                    xpos=25000000016,
                    clinvar_mito=None,
                    dbnsfp_mito=None,
                    gnomad_mito=None,
                    helix_mito=None,
                    hmtvar=None,
                    mitomap=None,
                    mitimpact=None,
                    gt_stats=hl.Struct(
                        AC_het=1,
                        AF_het=0.25,
                        AC_hom=0,
                        AF_hom=0.0,
                        AN=4,
                    ),
                ),
                hl.Struct(
                    locus=hl.Locus(
                        contig='chrM',
                        position=18,
                        reference_genome='GRCh38',
                    ),
                    alleles=['C', 'T'],
                    common_low_heteroplasmy=False,
                    haplogroup=hl.Struct(is_defining=False),
                    high_constraint_region_mito=False,
                    mitotip=hl.Struct(trna_prediction_id=None),
                    rg37_locus=hl.Locus(
                        contig='MT',
                        position=18,
                        reference_genome='GRCh37',
                    ),
                    rsid=None,
                    sorted_transcript_consequences=None,
                    variant_id='M-18-C-T',
                    xpos=25000000018,
                    clinvar_mito=None,
                    dbnsfp_mito=None,
                    gnomad_mito=None,
                    helix_mito=None,
                    hmtvar=None,
                    mitomap=None,
                    mitimpact=None,
                    gt_stats=hl.Struct(
                        AC_het=1,
                        AF_het=0.25,
                        AC_hom=0,
                        AF_hom=0.0,
                        AN=4,
                    ),
                ),
            ],
        )

    @patch(
        'v03_pipeline.lib.tasks.write_new_variants_table.load_gencode',
    )
    def test_sv_update_vat(
        self,
        mock_load_gencode: Mock,
        mock_update_rdc_task: Mock,
    ) -> None:
        mock_update_rdc_task.return_value = MockCompleteTask()
        mock_load_gencode.return_value = GENE_ID_MAPPING
        worker = luigi.worker.Worker()
        update_variant_annotations_task = (
            UpdateVariantAnnotationsTableWithNewSamplesTask(
                reference_genome=ReferenceGenome.GRCh38,
                dataset_type=DatasetType.SV,
                sample_type=SampleType.WGS,
                callset_paths=[TEST_SV_VCF],
                project_guids=['R0115_test_project2'],
                project_remap_paths=['not_a_real_file'],
                project_pedigree_paths=[TEST_PEDIGREE_5],
                validate=False,
                liftover_ref_path=TEST_LIFTOVER,
                run_id=TEST_RUN_ID,
            )
        )
        worker.add(update_variant_annotations_task)
        worker.run()
        self.assertTrue(update_variant_annotations_task.complete())
        self.assertFalse(
            GCSorLocalFolderTarget(
                f'{self.mock_env.REFERENCE_DATASETS}/v03/GRCh38/SV/lookup.ht',
            ).exists(),
        )
        ht = hl.read_table(update_variant_annotations_task.output().path)
        self.assertEqual(ht.count(), 11)
        self.assertCountEqual(
            ht.globals.collect(),
            [
                hl.Struct(
                    paths=hl.Struct(),
                    versions=hl.Struct(),
                    enums=hl.Struct(
                        sv_type=SV_TYPES,
                        sv_type_detail=SV_TYPE_DETAILS,
                        sorted_gene_consequences=hl.Struct(
                            major_consequence=SV_CONSEQUENCE_RANKS,
                        ),
                    ),
                    updates={
                        hl.Struct(
                            callset=TEST_SV_VCF,
                            project_guid='R0115_test_project2',
                        ),
                    },
                ),
            ],
        )
        self.assertCountEqual(
            ht.collect(),
            [
                hl.Struct(
                    variant_id='BND_chr1_6',
                    algorithms='manta',
                    bothsides_support=False,
                    cpx_intervals=None,
                    end_locus=hl.Locus(
                        contig='chr5',
                        position=20404,
                        reference_genome='GRCh38',
                    ),
                    gt_stats=hl.Struct(
                        AF=hl.eval(hl.float32(0.04775)),
                        AC=1,
                        AN=8,
                        Hom=0,
                        Het=278,
                    ),
                    gnomad_svs=None,
                    rg37_locus=hl.Locus(
                        contig=1,
                        position=10367,
                        reference_genome='GRCh37',
                    ),
                    rg37_locus_end=hl.Locus(
                        contig=5,
                        position=20404,
                        reference_genome='GRCh37',
                    ),
                    sorted_gene_consequences=[
                        hl.Struct(gene_id='ENSG00000186092', major_consequence_id=12),
                        hl.Struct(gene_id='ENSG00000153404', major_consequence_id=12),
                    ],
                    start_locus=hl.Locus(
                        contig='chr1',
                        position=180928,
                        reference_genome='GRCh38',
                    ),
                    strvctvre=hl.Struct(score=None),
                    sv_type_id=2,
                    sv_type_detail_id=None,
                    xpos=1000180928,
                ),
                hl.Struct(
                    variant_id='BND_chr1_9',
                    algorithms='manta',
                    bothsides_support=False,
                    cpx_intervals=None,
                    end_locus=hl.Locus(
                        contig='chr1',
                        position=789481,
                        reference_genome='GRCh38',
                    ),
                    gt_stats=hl.Struct(
                        AF=hl.eval(hl.float32(0.910684)),
                        AC=7,
                        AN=8,
                        Hom=2391,
                        Het=520,
                    ),
                    gnomad_svs=None,
                    rg37_locus=hl.Locus(
                        contig=1,
                        position=724861,
                        reference_genome='GRCh37',
                    ),
                    rg37_locus_end=hl.Locus(
                        contig=1,
                        position=724861,
                        reference_genome='GRCh37',
                    ),
                    sorted_gene_consequences=[
                        hl.Struct(gene_id='ENSG00000143756', major_consequence_id=12),
                        hl.Struct(gene_id='ENSG00000186192', major_consequence_id=12),
                    ],
                    start_locus=hl.Locus(
                        contig='chr1',
                        position=789481,
                        reference_genome='GRCh38',
                    ),
                    strvctvre=hl.Struct(score=None),
                    sv_type_id=2,
                    sv_type_detail_id=None,
                    xpos=1000789481,
                ),
                hl.Struct(
                    variant_id='CPX_chr1_22',
                    algorithms='manta',
                    bothsides_support=True,
                    cpx_intervals=[
                        hl.Struct(
                            type_id=8,
                            start=hl.Locus('chr1', 6558902, 'GRCh38'),
                            end=hl.Locus('chr1', 6559723, 'GRCh38'),
                        ),
                        hl.Struct(
                            type_id=6,
                            start=hl.Locus('chr1', 6559655, 'GRCh38'),
                            end=hl.Locus('chr1', 6559723, 'GRCh38'),
                        ),
                    ],
                    end_locus=hl.Locus(
                        contig='chr1',
                        position=6559723,
                        reference_genome='GRCh38',
                    ),
                    gt_stats=hl.Struct(
                        AF=hl.eval(hl.float32(0.169873)),
                        AC=2,
                        AN=8,
                        Hom=3,
                        Het=983,
                    ),
                    gnomad_svs=None,
                    rg37_locus=hl.Locus(
                        contig=1,
                        position=6618962,
                        reference_genome='GRCh37',
                    ),
                    rg37_locus_end=hl.Locus(
                        contig=1,
                        position=6619783,
                        reference_genome='GRCh37',
                    ),
                    sorted_gene_consequences=[
                        hl.Struct(gene_id='ENSG00000173662', major_consequence_id=11),
                    ],
                    start_locus=hl.Locus(
                        contig='chr1',
                        position=6558902,
                        reference_genome='GRCh38',
                    ),
                    strvctvre=hl.Struct(score=None),
                    sv_type_id=3,
                    sv_type_detail_id=2,
                    xpos=1006558902,
                ),
                hl.Struct(
                    variant_id='CPX_chr1_251',
                    algorithms='manta',
                    bothsides_support=False,
                    cpx_intervals=[
                        hl.Struct(
                            type_id=5,
                            start=hl.Locus('chr1', 180540234, 'GRCh38'),
                            end=hl.Locus('chr1', 181074767, 'GRCh38'),
                        ),
                        hl.Struct(
                            type_id=8,
                            start=hl.Locus('chr1', 181074767, 'GRCh38'),
                            end=hl.Locus('chr1', 181074938, 'GRCh38'),
                        ),
                    ],
                    end_locus=hl.Locus(
                        contig='chr1',
                        position=181074952,
                        reference_genome='GRCh38',
                    ),
                    gt_stats=hl.Struct(
                        AF=hl.eval(hl.float32(0.251804)),
                        AC=3,
                        AN=8,
                        Hom=114,
                        Het=1238,
                    ),
                    gnomad_svs=None,
                    rg37_locus=hl.Locus(
                        contig=1,
                        position=180509370,
                        reference_genome='GRCh37',
                    ),
                    rg37_locus_end=hl.Locus(
                        contig=1,
                        position=181044088,
                        reference_genome='GRCh37',
                    ),
                    sorted_gene_consequences=[
                        hl.Struct(gene_id='ENSG00000135835', major_consequence_id=0),
                        hl.Struct(gene_id='ENSG00000153029', major_consequence_id=0),
                        hl.Struct(gene_id='ENSG00000135823', major_consequence_id=0),
                        hl.Struct(gene_id='ENSG00000143324', major_consequence_id=0),
                    ],
                    start_locus=hl.Locus(
                        contig='chr1',
                        position=180540234,
                        reference_genome='GRCh38',
                    ),
                    strvctvre=hl.Struct(score=None),
                    sv_type_id=3,
                    sv_type_detail_id=9,
                    xpos=1180540234,
                ),
                hl.Struct(
                    variant_id='CPX_chr1_41',
                    algorithms='manta',
                    bothsides_support=False,
                    cpx_intervals=[
                        hl.Struct(
                            type_id=6,
                            start=hl.Locus('chr1', 16088760, 'GRCh38'),
                            end=hl.Locus('chr1', 16088835, 'GRCh38'),
                        ),
                        hl.Struct(
                            type_id=8,
                            start=hl.Locus('chr1', 16088760, 'GRCh38'),
                            end=hl.Locus('chr1', 16089601, 'GRCh38'),
                        ),
                    ],
                    end_locus=hl.Locus(
                        contig='chr1',
                        position=16089601,
                        reference_genome='GRCh38',
                    ),
                    gt_stats=hl.Struct(
                        AF=hl.eval(hl.float32(0.218138)),
                        AC=2,
                        AN=8,
                        Hom=18,
                        Het=1234,
                    ),
                    gnomad_svs=None,
                    rg37_locus=hl.Locus(
                        contig=1,
                        position=16415255,
                        reference_genome='GRCh37',
                    ),
                    rg37_locus_end=hl.Locus(
                        contig=1,
                        position=16416096,
                        reference_genome='GRCh37',
                    ),
                    sorted_gene_consequences=[
                        hl.Struct(gene_id='ENSG00000185519', major_consequence_id=12),
                    ],
                    start_locus=hl.Locus(
                        contig='chr1',
                        position=16088760,
                        reference_genome='GRCh38',
                    ),
                    strvctvre=hl.Struct(score=None),
                    sv_type_id=3,
                    sv_type_detail_id=12,
                    xpos=1016088760,
                ),
                hl.Struct(
                    variant_id='CPX_chr1_54',
                    algorithms='manta',
                    bothsides_support=False,
                    cpx_intervals=[
                        hl.Struct(
                            type_id=6,
                            start=hl.Locus('chr1', 21427498, 'GRCh38'),
                            end=hl.Locus('chr1', 21427959, 'GRCh38'),
                        ),
                        hl.Struct(
                            type_id=8,
                            start=hl.Locus('chr1', 21427498, 'GRCh38'),
                            end=hl.Locus('chr1', 21480073, 'GRCh38'),
                        ),
                        hl.Struct(
                            type_id=5,
                            start=hl.Locus('chr1', 21480073, 'GRCh38'),
                            end=hl.Locus('chr1', 21480419, 'GRCh38'),
                        ),
                    ],
                    end_locus=hl.Locus(
                        contig='chr1',
                        position=21480419,
                        reference_genome='GRCh38',
                    ),
                    gt_stats=hl.Struct(
                        AF=hl.eval(hl.float32(0.499656)),
                        AC=4,
                        AN=8,
                        Hom=49,
                        Het=2811,
                    ),
                    gnomad_svs=None,
                    rg37_locus=hl.Locus(
                        contig=1,
                        position=21753991,
                        reference_genome='GRCh37',
                    ),
                    rg37_locus_end=hl.Locus(
                        contig=1,
                        position=21806912,
                        reference_genome='GRCh37',
                    ),
                    sorted_gene_consequences=[
                        hl.Struct(gene_id='ENSG00000142794', major_consequence_id=0),
                    ],
                    start_locus=hl.Locus(
                        contig='chr1',
                        position=21427498,
                        reference_genome='GRCh38',
                    ),
                    strvctvre=hl.Struct(score=None),
                    sv_type_id=3,
                    sv_type_detail_id=13,
                    xpos=1021427498,
                ),
                hl.Struct(
                    variant_id='DEL_chr1_12',
                    algorithms='depth',
                    bothsides_support=False,
                    cpx_intervals=None,
                    end_locus=hl.Locus(
                        contig='chr1',
                        position=428500,
                        reference_genome='GRCh38',
                    ),
                    gt_stats=hl.Struct(
                        AF=hl.eval(hl.float32(0.064926)),
                        AC=1,
                        AN=8,
                        Hom=5,
                        Het=368,
                    ),
                    gnomad_svs=None,
                    rg37_locus=hl.Locus(
                        contig=5,
                        position=180831919,
                        reference_genome='GRCh37',
                    ),
                    rg37_locus_end=hl.Locus(
                        contig=5,
                        position=180817389,
                        reference_genome='GRCh37',
                    ),
                    sorted_gene_consequences=[
                        hl.Struct(gene_id='ENSG00000284733', major_consequence_id=12),
                    ],
                    start_locus=hl.Locus(
                        contig='chr1',
                        position=413968,
                        reference_genome='GRCh38',
                    ),
                    strvctvre=hl.Struct(score=None),
                    sv_type_id=5,
                    sv_type_detail_id=None,
                    xpos=1000413968,
                ),
                hl.Struct(
                    variant_id='DUP_chr1_5',
                    algorithms='depth',
                    bothsides_support=False,
                    cpx_intervals=None,
                    end_locus=hl.Locus(
                        contig='chr1',
                        position=263666,
                        reference_genome='GRCh38',
                    ),
                    gt_stats=hl.Struct(
                        AF=hl.eval(hl.float32(0.115596)),
                        AC=1,
                        AN=8,
                        Hom=110,
                        Het=453,
                    ),
                    gnomad_svs=None,
                    rg37_locus=None,
                    rg37_locus_end=hl.Locus(
                        contig=1,
                        position=233417,
                        reference_genome='GRCh37',
                    ),
                    sorted_gene_consequences=[
                        hl.Struct(gene_id='ENSG00000284733', major_consequence_id=12),
                    ],
                    start_locus=hl.Locus(
                        contig='chr1',
                        position=257666,
                        reference_genome='GRCh38',
                    ),
                    strvctvre=hl.Struct(score=None),
                    sv_type_id=6,
                    sv_type_detail_id=None,
                    xpos=1000257666,
                ),
                hl.Struct(
                    variant_id='INS_chr1_268',
                    algorithms='melt',
                    bothsides_support=False,
                    cpx_intervals=None,
                    end_locus=hl.Locus(
                        contig='chr1',
                        position=17465723,
                        reference_genome='GRCh38',
                    ),
                    gt_stats=hl.Struct(
                        AF=hl.eval(hl.float32(0.004466)),
                        AC=1,
                        AN=8,
                        Hom=0,
                        Het=26,
                    ),
                    gnomad_svs=None,
                    rg37_locus=hl.Locus(
                        contig=1,
                        position=17792203,
                        reference_genome='GRCh37',
                    ),
                    rg37_locus_end=hl.Locus(
                        contig=1,
                        position=17792219,
                        reference_genome='GRCh37',
                    ),
                    sorted_gene_consequences=[
                        hl.Struct(gene_id='ENSG00000179051', major_consequence_id=12),
                    ],
                    start_locus=hl.Locus(
                        contig='chr1',
                        position=17465707,
                        reference_genome='GRCh38',
                    ),
                    strvctvre=hl.Struct(score=None),
                    sv_type_id=7,
                    sv_type_detail_id=6,
                    xpos=1017465707,
                ),
                hl.Struct(
                    variant_id='INS_chr1_65',
                    algorithms='manta,melt',
                    bothsides_support=False,
                    cpx_intervals=None,
                    end_locus=hl.Locus(
                        contig='chr1',
                        position=4228448,
                        reference_genome='GRCh38',
                    ),
                    gt_stats=hl.Struct(
                        AF=hl.eval(hl.float32(0.10237)),
                        AC=1,
                        AN=8,
                        Hom=3,
                        Het=590,
                    ),
                    gnomad_svs=hl.Struct(
                        AF=hl.eval(hl.float32(0.06896299868822098)),
                        ID='gnomAD-SV_v2.1_INS_chr1_65',
                    ),
                    rg37_locus=hl.Locus(
                        contig=1,
                        position=4288465,
                        reference_genome='GRCh37',
                    ),
                    rg37_locus_end=hl.Locus(
                        contig=1,
                        position=4288508,
                        reference_genome='GRCh37',
                    ),
                    sorted_gene_consequences=[
                        hl.Struct(gene_id='ENSG00000198912', major_consequence_id=12),
                    ],
                    start_locus=hl.Locus(
                        contig='chr1',
                        position=4228405,
                        reference_genome='GRCh38',
                    ),
                    strvctvre=hl.Struct(score=hl.eval(hl.float32(0.1255))),
                    sv_type_id=7,
                    sv_type_detail_id=4,
                    xpos=1004228405,
                ),
                hl.Struct(
                    variant_id='INS_chr1_688',
                    algorithms='melt',
                    bothsides_support=False,
                    cpx_intervals=None,
                    end_locus=hl.Locus(
                        contig='chr1',
                        position=48963135,
                        reference_genome='GRCh38',
                    ),
                    gt_stats=hl.Struct(
                        AF=hl.eval(hl.float32(0.06338)),
                        AC=1,
                        AN=8,
                        Hom=2,
                        Het=365,
                    ),
                    gnomad_svs=None,
                    rg37_locus=hl.Locus(
                        contig=1,
                        position=49428756,
                        reference_genome='GRCh37',
                    ),
                    rg37_locus_end=hl.Locus(
                        contig=1,
                        position=49428807,
                        reference_genome='GRCh37',
                    ),
                    sorted_gene_consequences=[
                        hl.Struct(gene_id='ENSG00000186094', major_consequence_id=11),
                    ],
                    start_locus=hl.Locus(
                        contig='chr1',
                        position=48963084,
                        reference_genome='GRCh38',
                    ),
                    strvctvre=hl.Struct(score=None),
                    sv_type_id=7,
                    sv_type_detail_id=5,
                    xpos=1048963084,
                ),
            ],
        )

    def test_gcnv_update_vat(
        self,
        mock_update_rdc_task,
    ) -> None:
        mock_update_rdc_task.return_value = MockCompleteTask()
        worker = luigi.worker.Worker()
        update_variant_annotations_task = (
            UpdateVariantAnnotationsTableWithNewSamplesTask(
                reference_genome=ReferenceGenome.GRCh38,
                dataset_type=DatasetType.GCNV,
                sample_type=SampleType.WES,
                callset_paths=[TEST_GCNV_BED_FILE],
                project_guids=['R0115_test_project2'],
                project_remap_paths=['not_a_real_file'],
                project_pedigree_paths=[TEST_PEDIGREE_5],
                validate=False,
                liftover_ref_path=TEST_LIFTOVER,
                run_id=TEST_RUN_ID,
            )
        )
        worker.add(update_variant_annotations_task)
        worker.run()
        self.assertTrue(update_variant_annotations_task.complete())
        self.assertFalse(
            GCSorLocalFolderTarget(
                f'{self.mock_env.REFERENCE_DATASETS}/v03/GRCh38/GCNV/lookup.ht',
            ).exists(),
        )
        ht = hl.read_table(update_variant_annotations_task.output().path)
        self.assertEqual(ht.count(), 2)
        self.assertCountEqual(
            ht.globals.collect(),
            [
                hl.Struct(
                    paths=hl.Struct(),
                    versions=hl.Struct(),
                    enums=hl.Struct(
                        sv_type=SV_TYPES,
                        sorted_gene_consequences=hl.Struct(
                            major_consequence=SV_CONSEQUENCE_RANKS,
                        ),
                    ),
                    updates={
                        hl.Struct(
                            callset=TEST_GCNV_BED_FILE,
                            project_guid='R0115_test_project2',
                        ),
                    },
                ),
            ],
        )
        self.assertCountEqual(
            ht.collect(),
            [
                hl.Struct(
                    variant_id='suffix_16456_DEL',
                    end_locus=hl.Locus(
                        contig='chr1',
                        position=100023213,
                        reference_genome='GRCh38',
                    ),
                    gt_stats=hl.Struct(
                        AF=hl.eval(hl.float32(4.401408e-05)),
                        AC=1,
                        AN=22720,
                        Hom=None,
                        Het=None,
                    ),
                    num_exon=3,
                    rg37_locus=hl.Locus(
                        contig=1,
                        position=100472493,
                        reference_genome='GRCh37',
                    ),
                    rg37_locus_end=hl.Locus(
                        contig=1,
                        position=100488769,
                        reference_genome='GRCh37',
                    ),
                    sorted_gene_consequences=[
                        hl.Struct(gene_id='ENSG00000117620', major_consequence_id=0),
                        hl.Struct(gene_id='ENSG00000283761', major_consequence_id=0),
                        hl.Struct(gene_id='ENSG22222222222', major_consequence_id=None),
                    ],
                    start_locus=hl.Locus(
                        contig='chr1',
                        position=100006937,
                        reference_genome='GRCh38',
                    ),
                    strvctvre=hl.Struct(score=hl.eval(hl.float32(0.583))),
                    sv_type_id=5,
                    xpos=1100006937,
                ),
                hl.Struct(
                    variant_id='suffix_16457_DEL',
                    end_locus=hl.Locus(
                        contig='chr1',
                        position=100023212,
                        reference_genome='GRCh38',
                    ),
                    gt_stats=hl.Struct(
                        AF=8.802817319519818e-05,
                        AC=2,
                        AN=22719,
                        Hom=None,
                        Het=None,
                    ),
                    num_exon=2,
                    rg37_locus=hl.Locus(
                        contig=1,
                        position=100483142,
                        reference_genome='GRCh37',
                    ),
                    rg37_locus_end=hl.Locus(
                        contig=1,
                        position=100488768,
                        reference_genome='GRCh37',
                    ),
                    sorted_gene_consequences=[
                        hl.Struct(gene_id='ENSG00000283761', major_consequence_id=0),
                        hl.Struct(gene_id='ENSG22222222222', major_consequence_id=None),
                    ],
                    start_locus=hl.Locus(
                        contig='chr1',
                        position=100017586,
                        reference_genome='GRCh38',
                    ),
                    strvctvre=hl.Struct(score=0.5070000290870667),
                    sv_type_id=5,
                    xpos=1100017586,
                ),
            ],
        )
