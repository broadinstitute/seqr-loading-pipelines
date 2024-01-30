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

    @mock.patch.object(ReferenceDatasetCollection, 'datasets')
    def test_update_variant_annotations_table_with_updated_reference_dataset(
        self,
        mock_rdc_datasets,
        mock_update_rdc_task,
    ):
        mock_rdc_datasets.return_value = ['cadd', 'clinvar']
        mock_update_rdc_task.return_value = MockCompleteTask()

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
                ),
            ],
        )
