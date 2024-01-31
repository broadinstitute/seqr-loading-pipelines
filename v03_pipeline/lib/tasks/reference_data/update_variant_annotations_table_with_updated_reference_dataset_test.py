import shutil
from unittest import mock

import hail as hl
import luigi.worker

from v03_pipeline.lib.model import (
    DatasetType,
    ReferenceDatasetCollection,
    ReferenceGenome,
    SampleType,
)
from v03_pipeline.lib.paths import valid_reference_dataset_collection_path
from v03_pipeline.lib.tasks.files import GCSorLocalFolderTarget
from v03_pipeline.lib.tasks.reference_data.update_variant_annotations_table_with_updated_reference_dataset import (
    UpdateVariantAnnotationsTableWithUpdatedReferenceDataset,
)
from v03_pipeline.lib.test.mocked_dataroot_testcase import MockedDatarootTestCase
from v03_pipeline.var.test.tasks.mock_complete_task import MockCompleteTask

TEST_COMBINED_1 = 'v03_pipeline/var/test/reference_data/test_combined_1.ht'
TEST_HGMD_1 = 'v03_pipeline/var/test/reference_data/test_hgmd_1.ht'
TEST_INTERVAL_1 = 'v03_pipeline/var/test/reference_data/test_interval_1.ht'
TEST_COMBINED_MITO_1 = 'v03_pipeline/var/test/reference_data/test_combined_mito_1.ht'
TEST_INTERVAL_MITO_1 = 'v03_pipeline/var/test/reference_data/test_interval_mito_1.ht'
TEST_COMBINED_37 = 'v03_pipeline/var/test/reference_data/test_combined_37.ht'
TEST_HGMD_37 = 'v03_pipeline/var/test/reference_data/test_hgmd_37.ht'


@mock.patch(
    'v03_pipeline.lib.tasks.base.base_variant_annotations_table.UpdatedReferenceDatasetCollectionTask',
)
class UpdateVATWithUpdatedRDC(MockedDatarootTestCase):
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
            TEST_HGMD_1,
            valid_reference_dataset_collection_path(
                ReferenceGenome.GRCh38,
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
        shutil.copytree(
            TEST_COMBINED_37,
            valid_reference_dataset_collection_path(
                ReferenceGenome.GRCh37,
                DatasetType.SNV_INDEL,
                ReferenceDatasetCollection.COMBINED,
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

    @mock.patch.object(ReferenceDatasetCollection, 'datasets')
    def test_update_vat_with_updated_rdc_snv_indel_combined(
        self,
        mock_rdc_datasets,
        mock_update_rdc_task,
    ):
        mock_update_rdc_task.return_value = MockCompleteTask()
        mock_rdc_datasets.return_value = ['cadd', 'clinvar']

        task = UpdateVariantAnnotationsTableWithUpdatedReferenceDataset(
            reference_genome=ReferenceGenome.GRCh38,
            dataset_type=DatasetType.SNV_INDEL,
            sample_type=SampleType.WGS,
            rdc=ReferenceDatasetCollection.COMBINED,
        )
        self.assertTrue('annotations.ht' in task.output().path)
        self.assertTrue(DatasetType.SNV_INDEL.value in task.output().path)
        self.assertFalse(task.output().exists())
        self.assertFalse(task.complete())

        worker = luigi.worker.Worker()
        worker.add(task)
        worker.run()
        self.assertTrue(GCSorLocalFolderTarget(task.output().path).exists())
        self.assertTrue(task.complete())

        ht = hl.read_table(task.output().path)
        self.assertCountEqual(
            ht.collect(),
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
                ),
            ],
        )
        self.assertCountEqual(
            ht.globals.collect(),
            [
                hl.Struct(
                    paths=hl.Struct(
                        cadd='gs://seqr-reference-data/GRCh38/CADD/CADD_snvs_and_indels.v1.6.ht',
                        clinvar='ftp://ftp.ncbi.nlm.nih.gov/pub/clinvar/vcf_GRCh38/clinvar.vcf.gz',
                    ),
                    versions=hl.Struct(cadd='v1.6', clinvar='2023-07-02'),
                    enums=hl.Struct(
                        cadd=hl.Struct(),
                        clinvar=hl.Struct(
                            assertion=[
                                'Affects',
                                'association',
                                'association_not_found',
                                'confers_sensitivity',
                                'drug_response',
                                'low_penetrance',
                                'not_provided',
                                'other',
                                'protective',
                                'risk_factor',
                            ],
                            pathogenicity=[
                                'Pathogenic',
                                'Pathogenic/Likely_pathogenic',
                                'Pathogenic/Likely_pathogenic/Likely_risk_allele',
                                'Pathogenic/Likely_risk_allele',
                                'Likely_pathogenic',
                                'Likely_pathogenic/Likely_risk_allele',
                                'Established_risk_allele',
                                'Likely_risk_allele',
                                'Conflicting_interpretations_of_pathogenicity',
                                'Uncertain_risk_allele',
                                'Uncertain_significance/Uncertain_risk_allele',
                                'Uncertain_significance',
                                'No_pathogenic_assertion',
                                'Likely_benign',
                                'Benign/Likely_benign',
                                'Benign',
                            ],
                        ),
                    ),
                    updates=set(),
                ),
            ],
        )

    def test_update_vat_with_updated_rdc_snv_indel_hgmd(
        self,
        mock_update_rdc_task,
    ):
        mock_update_rdc_task.return_value = MockCompleteTask()

        task = UpdateVariantAnnotationsTableWithUpdatedReferenceDataset(
            reference_genome=ReferenceGenome.GRCh38,
            dataset_type=DatasetType.SNV_INDEL,
            sample_type=SampleType.WGS,
            rdc=ReferenceDatasetCollection.HGMD,
        )
        self.assertFalse(task.output().exists())
        self.assertFalse(task.complete())

        worker = luigi.worker.Worker()
        worker.add(task)
        worker.run()
        self.assertTrue(GCSorLocalFolderTarget(task.output().path).exists())
        self.assertTrue(task.complete())

        ht = hl.read_table(task.output().path)
        self.assertCountEqual(
            ht.collect(),
            [
                hl.Struct(
                    locus=hl.Locus(
                        contig='chr1',
                        position=871269,
                        reference_genome='GRCh38',
                    ),
                    alleles=['A', 'C'],
                    hgmd=hl.Struct(accession='abcdefg', class_id=3),
                ),
            ],
        )
        ht_globals = ht.globals.collect()
        self.assertCountEqual(
            ht_globals[0].paths,
            hl.Struct(
                hgmd='gs://seqr-reference-data-private/GRCh38/HGMD/HGMD_Pro_2023.1_hg38.vcf.gz',
            ),
        )
        self.assertCountEqual(
            ht_globals[0].versions,
            hl.Struct(hgmd=None),
        )

    @mock.patch.object(ReferenceDatasetCollection, 'datasets')
    def test_update_vat_with_updated_rdc_mito_combined(
        self,
        mock_rdc_datasets,
        mock_update_rdc_task,
    ):
        mock_update_rdc_task.return_value = MockCompleteTask()
        # dataset names differ slightly in mock rdc
        mock_rdc_datasets.return_value = [
            'clinvar',
            'dbnsfp',
            'gnomad_mito',
            'helix_mito',
            'hmtvar',
            'mitimpact',
            'mitomap',
        ]

        task = UpdateVariantAnnotationsTableWithUpdatedReferenceDataset(
            reference_genome=ReferenceGenome.GRCh38,
            dataset_type=DatasetType.MITO,
            sample_type=SampleType.WGS,
            rdc=ReferenceDatasetCollection.COMBINED,
        )
        self.assertFalse(task.output().exists())
        self.assertFalse(task.complete())

        worker = luigi.worker.Worker()
        worker.add(task)
        worker.run()
        self.assertTrue(GCSorLocalFolderTarget(task.output().path).exists())
        self.assertTrue(task.complete())

        ht = hl.read_table(task.output().path)
        self.assertCountEqual(
            ht.globals.collect(),
            [
                hl.Struct(
                    paths=hl.Struct(
                        clinvar='ftp://ftp.ncbi.nlm.nih.gov/pub/clinvar/vcf_GRCh38/clinvar.vcf.gz',
                        dbnsfp='gs://seqr-reference-data/GRCh38/dbNSFP/v4.2/dbNSFP4.2a_variant.ht',
                        gnomad_mito='gs://gcp-public-data--gnomad/release/3.1/ht/genomes/gnomad.genomes.v3.1.sites.chrM.ht',
                        helix_mito='gs://seqr-reference-data/GRCh38/mitochondrial/Helix/HelixMTdb_20200327.ht',
                        hmtvar='gs://seqr-reference-data/GRCh38/mitochondrial/HmtVar/HmtVar%20Jan.%2010%202022.ht',
                        mitomap='gs://seqr-reference-data/GRCh38/mitochondrial/MITOMAP/mitomap-confirmed-mutations-2022-02-04.ht',
                        mitimpact='gs://seqr-reference-data/GRCh38/mitochondrial/MitImpact/MitImpact_db_3.0.7.ht',
                    ),
                    versions=hl.Struct(
                        clinvar='2023-07-22',
                        dbnsfp='4.2',
                        gnomad_mito='v3.1',
                        helix_mito='20200327',
                        hmtvar='Jan. 10 2022',
                        mitomap='Feb. 04 2022',
                        mitimpact='3.0.7',
                    ),
                    enums=hl.Struct(
                        clinvar=hl.Struct(
                            pathogenicity=[
                                'Pathogenic',
                                'Pathogenic/Likely_pathogenic',
                                'Pathogenic/Likely_pathogenic/Likely_risk_allele',
                                'Pathogenic/Likely_risk_allele',
                                'Likely_pathogenic',
                                'Likely_pathogenic/Likely_risk_allele',
                                'Established_risk_allele',
                                'Likely_risk_allele',
                                'Conflicting_interpretations_of_pathogenicity',
                                'Uncertain_risk_allele',
                                'Uncertain_significance/Uncertain_risk_allele',
                                'Uncertain_significance',
                                'No_pathogenic_assertion',
                                'Likely_benign',
                                'Benign/Likely_benign',
                                'Benign',
                            ],
                            assertion=[
                                'Affects',
                                'association',
                                'association_not_found',
                                'confers_sensitivity',
                                'drug_response',
                                'low_penetrance',
                                'not_provided',
                                'other',
                                'protective',
                                'risk_factor',
                            ],
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
                    ),
                    updates=set(),
                ),
            ],
        )
        self.assertCountEqual(
            ht.collect(),
            [
                hl.Struct(
                    locus=hl.Locus(
                        contig='chrM',
                        position=1,
                        reference_genome='GRCh38',
                    ),
                    alleles=['A', 'C'],
                    clinvar=None,
                    dbnsfp=hl.Struct(
                        REVEL_score=None,
                        VEST4_score=None,
                        MutPred_score=None,
                        SIFT_pred_id=None,
                        Polyphen2_HVAR_pred_id=None,
                        MutationTaster_pred_id=None,
                        fathmm_MKL_coding_pred_id=None,
                    ),
                    gnomad_mito=None,
                    helix_mito=hl.Struct(
                        AC=1,
                        AF=5.102483555674553e-06,
                        AC_het=0,
                        AF_het=0.0,
                        AN=195982,
                        max_hl=None,
                    ),
                    hmtvar=hl.Struct(score=0.6700000166893005),
                    mitomap=None,
                    mitimpact=hl.Struct(score=0.5199999809265137),
                ),
                hl.Struct(
                    locus=hl.Locus(
                        contig='chrM',
                        position=1,
                        reference_genome='GRCh38',
                    ),
                    alleles=['A', 'G'],
                    clinvar=None,
                    dbnsfp=hl.Struct(
                        REVEL_score=None,
                        VEST4_score=None,
                        MutPred_score=None,
                        SIFT_pred_id=None,
                        Polyphen2_HVAR_pred_id=None,
                        MutationTaster_pred_id=None,
                        fathmm_MKL_coding_pred_id=None,
                    ),
                    gnomad_mito=None,
                    helix_mito=hl.Struct(
                        AC=1,
                        AF=5.102483555674553e-06,
                        AC_het=2,
                        AF_het=1.0204967111349106e-05,
                        AN=195982,
                        max_hl=0.31428998708724976,
                    ),
                    hmtvar=hl.Struct(score=0.699999988079071),
                    mitomap=None,
                    mitimpact=hl.Struct(score=0.36000001430511475),
                ),
                hl.Struct(
                    locus=hl.Locus(
                        contig='chrM',
                        position=1,
                        reference_genome='GRCh38',
                    ),
                    alleles=['A', 'T'],
                    clinvar=hl.Struct(
                        alleleId=680864,
                        conflictingPathogenicities=None,
                        goldStars=1,
                        pathogenicity_id=11,
                        assertion_ids=[],
                    ),
                    dbnsfp=hl.Struct(
                        REVEL_score=None,
                        VEST4_score=None,
                        MutPred_score=None,
                        SIFT_pred_id=None,
                        Polyphen2_HVAR_pred_id=None,
                        MutationTaster_pred_id=None,
                        fathmm_MKL_coding_pred_id=None,
                    ),
                    gnomad_mito=None,
                    helix_mito=None,
                    hmtvar=hl.Struct(score=0.6700000166893005),
                    mitomap=None,
                    mitimpact=hl.Struct(score=0.5199999809265137),
                ),
                hl.Struct(
                    locus=hl.Locus(
                        contig='chrM',
                        position=2,
                        reference_genome='GRCh38',
                    ),
                    alleles=['T', 'A'],
                    clinvar=None,
                    dbnsfp=hl.Struct(
                        REVEL_score=None,
                        VEST4_score=None,
                        MutPred_score=None,
                        SIFT_pred_id=None,
                        Polyphen2_HVAR_pred_id=None,
                        MutationTaster_pred_id=1,
                        fathmm_MKL_coding_pred_id=None,
                    ),
                    gnomad_mito=hl.Struct(
                        AN=56434,
                        AC=0,
                        AC_het=1,
                        AF=0.0,
                        AF_het=1.7719814422889613e-05,
                        max_hl=0.46000000834465027,
                    ),
                    helix_mito=None,
                    hmtvar=hl.Struct(score=0.7599999904632568),
                    mitomap=None,
                    mitimpact=hl.Struct(score=0.7099999785423279),
                ),
                hl.Struct(
                    locus=hl.Locus(
                        contig='chrM',
                        position=2,
                        reference_genome='GRCh38',
                    ),
                    alleles=['T', 'C'],
                    clinvar=hl.Struct(
                        alleleId=24767,
                        conflictingPathogenicities=None,
                        goldStars=2,
                        pathogenicity_id=14,
                        assertion_ids=[],
                    ),
                    dbnsfp=hl.Struct(
                        REVEL_score=None,
                        VEST4_score=None,
                        MutPred_score=None,
                        SIFT_pred_id=None,
                        Polyphen2_HVAR_pred_id=None,
                        MutationTaster_pred_id=1,
                        fathmm_MKL_coding_pred_id=None,
                    ),
                    gnomad_mito=hl.Struct(
                        AN=56409,
                        AC=1609,
                        AC_het=5,
                        AF=0.028523817658424377,
                        AF_het=8.86383350007236e-05,
                        max_hl=1.0,
                    ),
                    helix_mito=hl.Struct(
                        AC=1126,
                        AF=0.005745396483689547,
                        AC_het=26,
                        AF_het=0.00013266457244753838,
                        AN=195982,
                        max_hl=0.9464300274848938,
                    ),
                    hmtvar=hl.Struct(score=0.7400000095367432),
                    mitomap=None,
                    mitimpact=hl.Struct(score=0.75),
                ),
                hl.Struct(
                    locus=hl.Locus(
                        contig='chrM',
                        position=2,
                        reference_genome='GRCh38',
                    ),
                    alleles=['T', 'G'],
                    clinvar=hl.Struct(
                        alleleId=24768,
                        conflictingPathogenicities=None,
                        goldStars=1,
                        pathogenicity_id=13,
                        assertion_ids=[],
                    ),
                    dbnsfp=hl.Struct(
                        REVEL_score=None,
                        VEST4_score=None,
                        MutPred_score=None,
                        SIFT_pred_id=None,
                        Polyphen2_HVAR_pred_id=None,
                        MutationTaster_pred_id=1,
                        fathmm_MKL_coding_pred_id=None,
                    ),
                    gnomad_mito=hl.Struct(
                        AN=56433,
                        AC=7,
                        AC_het=0,
                        AF=0.00012404090375639498,
                        AF_het=0.0,
                        max_hl=1.0,
                    ),
                    helix_mito=hl.Struct(
                        AC=89,
                        AF=0.00045412100735120475,
                        AC_het=0,
                        AF_het=0.0,
                        AN=195983,
                        max_hl=None,
                    ),
                    hmtvar=hl.Struct(score=None),
                    mitomap=None,
                    mitimpact=None,
                ),
                hl.Struct(
                    locus=hl.Locus(
                        contig='chrM',
                        position=3,
                        reference_genome='GRCh38',
                    ),
                    alleles=['A', 'G'],
                    clinvar=None,
                    dbnsfp=hl.Struct(
                        REVEL_score=None,
                        VEST4_score=None,
                        MutPred_score=None,
                        SIFT_pred_id=None,
                        Polyphen2_HVAR_pred_id=None,
                        MutationTaster_pred_id=2,
                        fathmm_MKL_coding_pred_id=None,
                    ),
                    gnomad_mito=hl.Struct(
                        AN=56432,
                        AC=1,
                        AC_het=1,
                        AF=1.7720441974233836e-05,
                        AF_het=1.7720441974233836e-05,
                        max_hl=0.9940000176429749,
                    ),
                    helix_mito=hl.Struct(
                        AC=1,
                        AF=5.102483555674553e-06,
                        AC_het=0,
                        AF_het=0.0,
                        AN=195982,
                        max_hl=None,
                    ),
                    hmtvar=hl.Struct(score=None),
                    mitomap=None,
                    mitimpact=None,
                ),
                hl.Struct(
                    locus=hl.Locus(
                        contig='chrM',
                        position=4,
                        reference_genome='GRCh38',
                    ),
                    alleles=['C', 'A'],
                    clinvar=None,
                    dbnsfp=hl.Struct(
                        REVEL_score=None,
                        VEST4_score=None,
                        MutPred_score=None,
                        SIFT_pred_id=None,
                        Polyphen2_HVAR_pred_id=None,
                        MutationTaster_pred_id=2,
                        fathmm_MKL_coding_pred_id=None,
                    ),
                    gnomad_mito=None,
                    helix_mito=hl.Struct(
                        AC=0,
                        AF=0.0,
                        AC_het=1,
                        AF_het=5.102483555674553e-06,
                        AN=195982,
                        max_hl=0.23711000382900238,
                    ),
                    hmtvar=hl.Struct(score=0.07000000029802322),
                    mitomap=None,
                    mitimpact=hl.Struct(score=0.5299999713897705),
                ),
                hl.Struct(
                    locus=hl.Locus(
                        contig='chrM',
                        position=4,
                        reference_genome='GRCh38',
                    ),
                    alleles=['C', 'G'],
                    clinvar=None,
                    dbnsfp=hl.Struct(
                        REVEL_score=None,
                        VEST4_score=None,
                        MutPred_score=None,
                        SIFT_pred_id=None,
                        Polyphen2_HVAR_pred_id=None,
                        MutationTaster_pred_id=2,
                        fathmm_MKL_coding_pred_id=None,
                    ),
                    gnomad_mito=None,
                    helix_mito=None,
                    hmtvar=hl.Struct(score=0.11999999731779099),
                    mitomap=None,
                    mitimpact=hl.Struct(score=0.550000011920929),
                ),
                hl.Struct(
                    locus=hl.Locus(
                        contig='chrM',
                        position=4,
                        reference_genome='GRCh38',
                    ),
                    alleles=['C', 'T'],
                    clinvar=hl.Struct(
                        alleleId=680866,
                        conflictingPathogenicities=None,
                        goldStars=1,
                        pathogenicity_id=11,
                        assertion_ids=[],
                    ),
                    dbnsfp=hl.Struct(
                        REVEL_score=None,
                        VEST4_score=None,
                        MutPred_score=None,
                        SIFT_pred_id=None,
                        Polyphen2_HVAR_pred_id=None,
                        MutationTaster_pred_id=2,
                        fathmm_MKL_coding_pred_id=None,
                    ),
                    gnomad_mito=hl.Struct(
                        AN=56430,
                        AC=6,
                        AC_het=1,
                        AF=0.00010632642079144716,
                        AF_het=1.772106952557806e-05,
                        max_hl=1.0,
                    ),
                    helix_mito=hl.Struct(
                        AC=27,
                        AF=0.00013776705600321293,
                        AC_het=3,
                        AF_het=1.530745066702366e-05,
                        AN=195982,
                        max_hl=0.7451000213623047,
                    ),
                    hmtvar=hl.Struct(score=0.15000000596046448),
                    mitomap=None,
                    mitimpact=hl.Struct(score=0.6899999976158142),
                ),
            ],
        )

    def test_update_vat_with_updated_rdc_snv_indel_37_combined(
        self,
        mock_update_rdc_task,
    ):
        mock_update_rdc_task.return_value = MockCompleteTask()

        task = UpdateVariantAnnotationsTableWithUpdatedReferenceDataset(
            reference_genome=ReferenceGenome.GRCh37,
            dataset_type=DatasetType.SNV_INDEL,
            sample_type=SampleType.WGS,
            rdc=ReferenceDatasetCollection.COMBINED,
        )
        self.assertFalse(task.output().exists())
        self.assertFalse(task.complete())

        worker = luigi.worker.Worker()
        worker.add(task)
        worker.run()
        self.assertTrue(GCSorLocalFolderTarget(task.output().path).exists())
        self.assertTrue(task.complete())

        ht = hl.read_table(task.output().path)

        self.assertCountEqual(
            ht.globals.collect(),
            [
                hl.Struct(
                    paths=hl.Struct(
                        cadd='gs://seqr-reference-data/GRCh37/CADD/CADD_snvs_and_indels.v1.6.ht',
                        clinvar='ftp://ftp.ncbi.nlm.nih.gov/pub/clinvar/vcf_GRCh37/clinvar.vcf.gz',
                        dbnsfp='gs://seqr-reference-data/GRCh37/dbNSFP/v2.9.3/dbNSFP2.9.3_variant.ht',
                        eigen='gs://seqr-reference-data/GRCh37/eigen/EIGEN_coding_noncoding.grch37.ht',
                        exac='gs://seqr-reference-data/GRCh37/gnomad/ExAC.r1.sites.vep.ht',
                        gnomad_exomes='gs://gcp-public-data--gnomad/release/2.1.1/ht/exomes/gnomad.exomes.r2.1.1.sites.ht',
                        gnomad_genomes='gs://gcp-public-data--gnomad/release/2.1.1/ht/genomes/gnomad.genomes.r2.1.1.sites.ht',
                        mpc='gs://seqr-reference-data/GRCh37/MPC/fordist_constraint_official_mpc_values.ht',
                        primate_ai='gs://seqr-reference-data/GRCh37/primate_ai/PrimateAI_scores_v0.2.ht',
                        splice_ai='gs://seqr-reference-data/GRCh37/spliceai/spliceai_scores.ht',
                        topmed='gs://seqr-reference-data/GRCh37/TopMed/bravo-dbsnp-all.removed_chr_prefix.liftunder_GRCh37.ht',
                    ),
                    versions=hl.Struct(
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
                    ),
                    enums=hl.Struct(
                        cadd=hl.Struct(),
                        clinvar=hl.Struct(
                            pathogenicity=[
                                'Pathogenic',
                                'Pathogenic/Likely_pathogenic',
                                'Pathogenic/Likely_pathogenic/Likely_risk_allele',
                                'Pathogenic/Likely_risk_allele',
                                'Likely_pathogenic',
                                'Likely_pathogenic/Likely_risk_allele',
                                'Established_risk_allele',
                                'Likely_risk_allele',
                                'Conflicting_interpretations_of_pathogenicity',
                                'Uncertain_risk_allele',
                                'Uncertain_significance/Uncertain_risk_allele',
                                'Uncertain_significance',
                                'No_pathogenic_assertion',
                                'Likely_benign',
                                'Benign/Likely_benign',
                                'Benign',
                            ],
                            assertion=[
                                'Affects',
                                'association',
                                'association_not_found',
                                'confers_sensitivity',
                                'drug_response',
                                'low_penetrance',
                                'not_provided',
                                'other',
                                'protective',
                                'risk_factor',
                            ],
                        ),
                        dbnsfp=hl.Struct(
                            SIFT_pred=['D', 'T'],
                            Polyphen2_HVAR_pred=['D', 'P', 'B'],
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
                    ),
                    updates=set(),
                ),
            ],
        )
        self.assertCountEqual(
            ht.collect(),
            [
                hl.Struct(
                    locus=hl.Locus(
                        contig=1,
                        position=871269,
                        reference_genome='GRCh37',
                    ),
                    alleles=['A', 'AA'],
                    cadd=None,
                    clinvar=None,
                    dbnsfp=None,
                    eigen=None,
                    exac=None,
                    gnomad_exomes=None,
                    gnomad_genomes=None,
                    mpc=None,
                    primate_ai=None,
                    splice_ai=hl.Struct(
                        delta_score=0.009999999776482582,
                        splice_consequence_id=3,
                    ),
                    topmed=None,
                ),
                hl.Struct(
                    locus=hl.Locus(
                        contig=1,
                        position=871269,
                        reference_genome='GRCh37',
                    ),
                    alleles=['A', 'AC'],
                    cadd=None,
                    clinvar=None,
                    dbnsfp=None,
                    eigen=None,
                    exac=None,
                    gnomad_exomes=None,
                    gnomad_genomes=None,
                    mpc=None,
                    primate_ai=None,
                    splice_ai=hl.Struct(
                        delta_score=0.019999999552965164,
                        splice_consequence_id=3,
                    ),
                    topmed=None,
                ),
                hl.Struct(
                    locus=hl.Locus(
                        contig=1,
                        position=871269,
                        reference_genome='GRCh37',
                    ),
                    alleles=['A', 'AG'],
                    cadd=None,
                    clinvar=None,
                    dbnsfp=None,
                    eigen=None,
                    exac=None,
                    gnomad_exomes=None,
                    gnomad_genomes=None,
                    mpc=None,
                    primate_ai=None,
                    splice_ai=hl.Struct(
                        delta_score=0.019999999552965164,
                        splice_consequence_id=3,
                    ),
                    topmed=None,
                ),
                hl.Struct(
                    locus=hl.Locus(
                        contig=1,
                        position=871269,
                        reference_genome='GRCh37',
                    ),
                    alleles=['A', 'AT'],
                    cadd=None,
                    clinvar=None,
                    dbnsfp=None,
                    eigen=None,
                    exac=None,
                    gnomad_exomes=None,
                    gnomad_genomes=None,
                    mpc=None,
                    primate_ai=None,
                    splice_ai=hl.Struct(
                        delta_score=0.009999999776482582,
                        splice_consequence_id=3,
                    ),
                    topmed=None,
                ),
                hl.Struct(
                    locus=hl.Locus(
                        contig=1,
                        position=871269,
                        reference_genome='GRCh37',
                    ),
                    alleles=['A', 'C'],
                    cadd=hl.Struct(PHRED=9.699999809265137),
                    clinvar=None,
                    dbnsfp=hl.Struct(
                        REVEL_score=0.0430000014603138,
                        SIFT_pred_id=None,
                        Polyphen2_HVAR_pred_id=None,
                        MutationTaster_pred_id=0,
                    ),
                    eigen=hl.Struct(Eigen_phred=1.5880000591278076),
                    exac=hl.Struct(
                        AF_POPMAX=0.0004100881633348763,
                        AF=0.0004633000062312931,
                        AC_Adj=51,
                        AC_Het=51,
                        AC_Hom=0,
                        AC_Hemi=None,
                        AN_Adj=108288,
                    ),
                    gnomad_exomes=hl.Struct(
                        AF=0.00012876000255346298,
                        AN=240758,
                        AC=31,
                        Hom=0,
                        AF_POPMAX_OR_GLOBAL=0.0001119549197028391,
                        FAF_AF=9.315000352216884e-05,
                        Hemi=0,
                    ),
                    gnomad_genomes=None,
                    mpc=None,
                    primate_ai=None,
                    splice_ai=hl.Struct(
                        delta_score=0.029999999329447746,
                        splice_consequence_id=3,
                    ),
                    topmed=None,
                ),
                hl.Struct(
                    locus=hl.Locus(
                        contig=1,
                        position=871269,
                        reference_genome='GRCh37',
                    ),
                    alleles=['A', 'G'],
                    cadd=hl.Struct(PHRED=9.121000289916992),
                    clinvar=None,
                    dbnsfp=hl.Struct(
                        REVEL_score=0.08500000089406967,
                        SIFT_pred_id=None,
                        Polyphen2_HVAR_pred_id=None,
                        MutationTaster_pred_id=0,
                    ),
                    eigen=hl.Struct(Eigen_phred=1.5880000591278076),
                    exac=None,
                    gnomad_exomes=None,
                    gnomad_genomes=None,
                    mpc=None,
                    primate_ai=None,
                    splice_ai=hl.Struct(
                        delta_score=0.009999999776482582,
                        splice_consequence_id=3,
                    ),
                    topmed=None,
                ),
                hl.Struct(
                    locus=hl.Locus(
                        contig=1,
                        position=871269,
                        reference_genome='GRCh37',
                    ),
                    alleles=['A', 'T'],
                    cadd=hl.Struct(PHRED=9.36299991607666),
                    clinvar=None,
                    dbnsfp=hl.Struct(
                        REVEL_score=0.04600000008940697,
                        SIFT_pred_id=None,
                        Polyphen2_HVAR_pred_id=None,
                        MutationTaster_pred_id=0,
                    ),
                    eigen=hl.Struct(Eigen_phred=1.5880000591278076),
                    exac=None,
                    gnomad_exomes=None,
                    gnomad_genomes=None,
                    mpc=None,
                    primate_ai=None,
                    splice_ai=hl.Struct(
                        delta_score=0.019999999552965164,
                        splice_consequence_id=3,
                    ),
                    topmed=None,
                ),
                hl.Struct(
                    locus=hl.Locus(
                        contig=1,
                        position=871269,
                        reference_genome='GRCh37',
                    ),
                    alleles=['AC', 'A'],
                    cadd=None,
                    clinvar=None,
                    dbnsfp=None,
                    eigen=None,
                    exac=None,
                    gnomad_exomes=None,
                    gnomad_genomes=None,
                    mpc=None,
                    primate_ai=None,
                    splice_ai=hl.Struct(delta_score=0.0, splice_consequence_id=4),
                    topmed=None,
                ),
                hl.Struct(
                    locus=hl.Locus(
                        contig=1,
                        position=871269,
                        reference_genome='GRCh37',
                    ),
                    alleles=['ACC', 'A'],
                    cadd=None,
                    clinvar=None,
                    dbnsfp=None,
                    eigen=None,
                    exac=None,
                    gnomad_exomes=None,
                    gnomad_genomes=None,
                    mpc=None,
                    primate_ai=None,
                    splice_ai=hl.Struct(delta_score=0.0, splice_consequence_id=4),
                    topmed=None,
                ),
                hl.Struct(
                    locus=hl.Locus(
                        contig=1,
                        position=871269,
                        reference_genome='GRCh37',
                    ),
                    alleles=['ACCC', 'A'],
                    cadd=None,
                    clinvar=None,
                    dbnsfp=None,
                    eigen=None,
                    exac=None,
                    gnomad_exomes=None,
                    gnomad_genomes=None,
                    mpc=None,
                    primate_ai=None,
                    splice_ai=hl.Struct(delta_score=0.0, splice_consequence_id=4),
                    topmed=None,
                ),
                hl.Struct(
                    locus=hl.Locus(
                        contig=1,
                        position=871269,
                        reference_genome='GRCh37',
                    ),
                    alleles=['ACCCG', 'A'],
                    cadd=None,
                    clinvar=None,
                    dbnsfp=None,
                    eigen=None,
                    exac=None,
                    gnomad_exomes=None,
                    gnomad_genomes=None,
                    mpc=None,
                    primate_ai=None,
                    splice_ai=hl.Struct(
                        delta_score=0.03999999910593033,
                        splice_consequence_id=3,
                    ),
                    topmed=None,
                ),
            ],
        )
