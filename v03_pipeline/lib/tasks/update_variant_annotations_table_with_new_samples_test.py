import os
import shutil
import tempfile
import unittest
from unittest.mock import Mock, patch

import hail as hl
import luigi.worker

from hail_scripts.reference_data.clinvar import (
    CLINVAR_ASSERTIONS,
    CLINVAR_PATHOGENICITIES,
)

from v03_pipeline.lib.annotations.enums import (
    BIOTYPES,
    CONSEQUENCE_TERMS,
    LOF_FILTERS,
    MITOTIP_PATHOGENICITIES,
)
from v03_pipeline.lib.model import DatasetType, Env, ReferenceGenome
from v03_pipeline.lib.tasks.update_variant_annotations_table_with_new_samples import (
    UpdateVariantAnnotationsTableWithNewSamplesTask,
)

TEST_SNV_VCF = 'v03_pipeline/var/test/callsets/1kg_30variants.vcf.bgz'
TEST_MITO_MT = 'v03_pipeline/var/test/callsets/mito_1.mt'
TEST_REMAP = 'v03_pipeline/var/test/remaps/test_remap_1.tsv'
TEST_PEDIGREE_3 = 'v03_pipeline/var/test/pedigrees/test_pedigree_3.tsv'
TEST_PEDIGREE_4 = 'v03_pipeline/var/test/pedigrees/test_pedigree_4.tsv'
TEST_PEDIGREE_5 = 'v03_pipeline/var/test/pedigrees/test_pedigree_5.tsv'
TEST_COMBINED_1 = 'v03_pipeline/var/test/reference_data/test_combined_1.ht'
TEST_COMBINED_MITO_1 = 'v03_pipeline/var/test/reference_data/test_combined_mito_1.ht'
TEST_HGMD_1 = 'v03_pipeline/var/test/reference_data/test_hgmd_1.ht'
TEST_INTERVAL_1 = 'v03_pipeline/var/test/reference_data/test_interval_1.ht'
TEST_INTERVAL_MITO_1 = 'v03_pipeline/var/test/reference_data/test_interval_mito_1.ht'


@patch('v03_pipeline.lib.paths.DataRoot')
class UpdateVariantAnnotationsTableWithNewSamplesTaskTest(unittest.TestCase):
    maxDiff = None

    def setUp(self) -> None:
        self._temp_local_datasets = tempfile.TemporaryDirectory().name
        self._temp_local_reference_data = tempfile.TemporaryDirectory().name
        shutil.copytree(
            TEST_COMBINED_1,
            f'{self._temp_local_reference_data}/v03/GRCh38/reference_datasets/combined.ht',
        )
        shutil.copytree(
            TEST_HGMD_1,
            f'{self._temp_local_reference_data}/v03/GRCh38/reference_datasets/hgmd.ht',
        )
        shutil.copytree(
            TEST_COMBINED_MITO_1,
            f'{self._temp_local_reference_data}/v03/GRCh38/reference_datasets/combined_mito.ht',
        )
        shutil.copytree(
            TEST_INTERVAL_MITO_1,
            f'{self._temp_local_reference_data}/v03/GRCh38/reference_datasets/interval_mito.ht',
        )

    def tearDown(self) -> None:
        if os.path.isdir(self._temp_local_datasets):
            shutil.rmtree(self._temp_local_datasets)

        if os.path.isdir(self._temp_local_reference_data):
            shutil.rmtree(self._temp_local_reference_data)

    def test_missing_pedigree(self, mock_dataroot: Mock) -> None:
        mock_dataroot.LOCAL_DATASETS.value = self._temp_local_datasets
        mock_dataroot.LOCAL_REFERENCE_DATA.value = self._temp_local_reference_data
        uvatwns_task = UpdateVariantAnnotationsTableWithNewSamplesTask(
            env=Env.TEST,
            reference_genome=ReferenceGenome.GRCh38,
            dataset_type=DatasetType.SNV,
            callset_path=TEST_SNV_VCF,
            project_guids=['R0113_test_project'],
            project_remap_paths=[TEST_REMAP],
            project_pedigree_paths=['bad_pedigree'],
        )
        worker = luigi.worker.Worker()
        worker.add(uvatwns_task)
        worker.run()
        self.assertFalse(uvatwns_task.complete())

    def test_missing_interval_reference(self, mock_dataroot: Mock) -> None:
        mock_dataroot.LOCAL_DATASETS.value = self._temp_local_datasets
        mock_dataroot.LOCAL_REFERENCE_DATA.value = self._temp_local_reference_data
        uvatwns_task = UpdateVariantAnnotationsTableWithNewSamplesTask(
            env=Env.TEST,
            reference_genome=ReferenceGenome.GRCh38,
            dataset_type=DatasetType.SNV,
            callset_path=TEST_SNV_VCF,
            project_guids=['R0113_test_project'],
            project_remap_paths=[TEST_REMAP],
            project_pedigree_paths=[TEST_PEDIGREE_3],
        )
        worker = luigi.worker.Worker()
        worker.add(uvatwns_task)
        worker.run()
        self.assertFalse(uvatwns_task.complete())

    def test_mulitiple_update_vat(self, mock_dataroot: Mock) -> None:
        shutil.copytree(
            TEST_INTERVAL_1,
            f'{self._temp_local_reference_data}/v03/GRCh38/reference_datasets/interval.ht',
        )
        mock_dataroot.LOCAL_DATASETS.value = self._temp_local_datasets
        mock_dataroot.LOCAL_REFERENCE_DATA.value = self._temp_local_reference_data
        worker = luigi.worker.Worker()

        uvatwns_task_3 = UpdateVariantAnnotationsTableWithNewSamplesTask(
            env=Env.TEST,
            reference_genome=ReferenceGenome.GRCh38,
            dataset_type=DatasetType.SNV,
            callset_path=TEST_SNV_VCF,
            project_guids=['R0113_test_project'],
            project_remap_paths=[TEST_REMAP],
            project_pedigree_paths=[TEST_PEDIGREE_3],
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
            ][0],
            hl.Struct(
                locus=hl.Locus(
                    contig='chr1',
                    position=871269,
                    reference_genome='GRCh38',
                ),
                alleles=['A', 'C'],
                gt_stats=hl.Struct(AC=0, AN=6, AF=0.0, hom=0),
            ),
        )
        self.assertEqual(
            ht.globals.updates.collect(),
            [
                {
                    hl.Struct(
                        callset=TEST_SNV_VCF,
                        project_guid='R0113_test_project',
                    ),
                },
            ],
        )

        # Ensure that new variants are added correctly to the table.
        uvatwns_task_4 = UpdateVariantAnnotationsTableWithNewSamplesTask(
            env=Env.TEST,
            reference_genome=ReferenceGenome.GRCh38,
            dataset_type=DatasetType.SNV,
            callset_path=TEST_SNV_VCF,
            project_guids=['R0114_project4'],
            project_remap_paths=[TEST_REMAP],
            project_pedigree_paths=[TEST_PEDIGREE_4],
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
                    cadd=1,
                    clinvar=2,
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
            ht.globals.collect(),
            [
                hl.Struct(
                    updates={
                        hl.Struct(
                            callset='v03_pipeline/var/test/callsets/1kg_30variants.vcf.bgz',
                            project_guid='R0113_test_project',
                        ),
                        hl.Struct(
                            callset='v03_pipeline/var/test/callsets/1kg_30variants.vcf.bgz',
                            project_guid='R0114_project4',
                        ),
                    },
                    paths=hl.Struct(
                        cadd='gs://seqr-reference-data/GRCh38/CADD/CADD_snvs_and_indels.v1.6.ht',
                        clinvar='ftp://ftp.ncbi.nlm.nih.gov/pub/clinvar/vcf_GRCh38/clinvar.vcf.gz',
                        hgmd='gs://seqr-reference-data-private/GRCh38/HGMD/HGMD_Pro_2023.1_hg38.vcf.gz',
                        gnomad_non_coding_constraint='gs://seqr-reference-data/GRCh38/gnomad_nc_constraint/gnomad_non-coding_constraint_z_scores.ht',
                        screen='gs://seqr-reference-data/GRCh38/ccREs/GRCh38-ccREs.ht',
                    ),
                    versions=hl.Struct(
                        cadd='v1.6',
                        clinvar='2023-07-02',
                        hgmd=None,
                        gnomad_non_coding_constraint=None,
                        screen=None,
                    ),
                    enums=hl.Struct(
                        cadd=hl.Struct(),
                        clinvar=hl.Struct(
                            assertion=CLINVAR_ASSERTIONS,
                            pathogenicity=CLINVAR_PATHOGENICITIES,
                        ),
                        hgmd=hl.Struct(
                            **{'class': ['DFP', 'DM', 'DM?', 'DP', 'FP', 'R']},
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

    def test_mito_update_vat(self, mock_dataroot: Mock) -> None:
        mock_dataroot.LOCAL_DATASETS.value = self._temp_local_datasets
        mock_dataroot.LOCAL_REFERENCE_DATA.value = self._temp_local_reference_data
        worker = luigi.worker.Worker()

        update_variant_annotations_task = (
            UpdateVariantAnnotationsTableWithNewSamplesTask(
                env=Env.TEST,
                reference_genome=ReferenceGenome.GRCh38,
                dataset_type=DatasetType.MITO,
                callset_path=TEST_MITO_MT,
                project_guids=['R0115_test_project2'],
                project_remap_paths=['not_a_real_file'],
                project_pedigree_paths=[TEST_PEDIGREE_5],
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
                        clinvar='ftp://ftp.ncbi.nlm.nih.gov/pub/clinvar/vcf_GRCh38/clinvar.vcf.gz',
                        dbnsfp='gs://seqr-reference-data/GRCh38/dbNSFP/v4.2/dbNSFP4.2a_variant.ht',
                        gnomad_mito='gs://gcp-public-data--gnomad/release/3.1/ht/genomes/gnomad.genomes.v3.1.sites.chrM.ht',
                        helix_mito='gs://seqr-reference-data/GRCh38/mitochondrial/Helix/HelixMTdb_20200327.ht',
                        hmtvar='gs://seqr-reference-data/GRCh38/mitochondrial/HmtVar/HmtVar%20Jan.%2010%202022.ht',
                        mitomap='gs://seqr-reference-data/GRCh38/mitochondrial/MITOMAP/mitomap-confirmed-mutations-2022-02-04.ht',
                        mitimpact='gs://seqr-reference-data/GRCh38/mitochondrial/MitImpact/MitImpact_db_3.0.7.ht',
                    ),
                    versions=hl.Struct(
                        high_constraint_region_mito='Feb-15-2022',
                        clinvar='2023-07-22',
                        dbnsfp='4.2',
                        gnomad_mito='v3.1',
                        helix_mito='20200327',
                        hmtvar='Jan. 10 2022',
                        mitomap='Feb. 04 2022',
                        mitimpact='3.0.7',
                    ),
                    enums=hl.Struct(
                        high_constraint_region_mito=hl.Struct(),
                        clinvar=hl.Struct(
                            assertion=CLINVAR_ASSERTIONS,
                            pathogenicity=CLINVAR_PATHOGENICITIES,
                        ),
                        dbnsfp=hl.Struct(
                            SIFT_pred=['D', 'T'],
                            Polyphen2_HVAR_pred=['D', 'P', 'B'],
                            MutationTaster_pred=['D', 'A', 'N', 'P'],
                            fathmm_MKL_coding_pred=['D', 'N'],
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
                        mitotip=hl.Struct(pathogenicities=MITOTIP_PATHOGENICITIES),
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
                    callset_heteroplasmy=hl.Struct(AF=0.0, AC=0, AN=2520),
                    haplogroup=hl.Struct(is_defining=None),
                    high_constraint_region=True,
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
                    clinvar=None,
                    dbnsfp=None,
                    gnomad_mito=None,
                    helix_mito=None,
                    hmtvar=None,
                    mitomap=None,
                    mitimpact=None,
                    gt_stats=hl.Struct(AC=0, AN=8, AF=0.0, hom=0),
                ),
                hl.Struct(
                    locus=hl.Locus(
                        contig='chrM',
                        position=8,
                        reference_genome='GRCh38',
                    ),
                    alleles=['G', 'T'],
                    common_low_heteroplasmy=False,
                    callset_heteroplasmy=hl.Struct(AF=0.0, AC=0, AN=2520),
                    haplogroup=hl.Struct(is_defining=None),
                    high_constraint_region=True,
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
                    clinvar=None,
                    dbnsfp=None,
                    gnomad_mito=None,
                    helix_mito=None,
                    hmtvar=None,
                    mitomap=None,
                    mitimpact=None,
                    gt_stats=hl.Struct(AC=0, AN=8, AF=0.0, hom=0),
                ),
                hl.Struct(
                    locus=hl.Locus(
                        contig='chrM',
                        position=12,
                        reference_genome='GRCh38',
                    ),
                    alleles=['T', 'C'],
                    common_low_heteroplasmy=False,
                    callset_heteroplasmy=hl.Struct(AF=0.0, AC=0, AN=2520),
                    haplogroup=hl.Struct(is_defining=None),
                    high_constraint_region=False,
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
                    clinvar=None,
                    dbnsfp=None,
                    gnomad_mito=None,
                    helix_mito=None,
                    hmtvar=None,
                    mitomap=None,
                    mitimpact=None,
                    gt_stats=hl.Struct(AC=0, AN=8, AF=0.0, hom=0),
                ),
                hl.Struct(
                    locus=hl.Locus(
                        contig='chrM',
                        position=16,
                        reference_genome='GRCh38',
                    ),
                    alleles=['A', 'T'],
                    common_low_heteroplasmy=False,
                    callset_heteroplasmy=hl.Struct(
                        AF=0.0003968253968253968,
                        AC=1,
                        AN=2520,
                    ),
                    haplogroup=hl.Struct(is_defining=0),
                    high_constraint_region=False,
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
                    clinvar=None,
                    dbnsfp=None,
                    gnomad_mito=None,
                    helix_mito=None,
                    hmtvar=None,
                    mitomap=None,
                    mitimpact=None,
                    gt_stats=hl.Struct(AC=0, AN=8, AF=0.0, hom=0),
                ),
                hl.Struct(
                    locus=hl.Locus(
                        contig='chrM',
                        position=18,
                        reference_genome='GRCh38',
                    ),
                    alleles=['C', 'T'],
                    common_low_heteroplasmy=False,
                    callset_heteroplasmy=hl.Struct(AF=0.0, AC=0, AN=2520),
                    haplogroup=hl.Struct(is_defining=None),
                    high_constraint_region=False,
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
                    clinvar=None,
                    dbnsfp=None,
                    gnomad_mito=None,
                    helix_mito=None,
                    hmtvar=None,
                    mitomap=None,
                    mitimpact=None,
                    gt_stats=hl.Struct(AC=0, AN=8, AF=0.0, hom=0),
                ),
            ],
        )
