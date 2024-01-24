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
from v03_pipeline.lib.tasks.base.base_variant_annotations_table import (
    BaseVariantAnnotationsTableTask,
)
from v03_pipeline.lib.tasks.files import GCSorLocalFolderTarget
from v03_pipeline.lib.test.mocked_dataroot_testcase import MockedDatarootTestCase

TEST_COMBINED_1 = 'v03_pipeline/var/test/reference_data/test_combined_1.ht'
TEST_HGMD_1 = 'v03_pipeline/var/test/reference_data/test_hgmd_1.ht'
TEST_INTERVAL_1 = 'v03_pipeline/var/test/reference_data/test_interval_1.ht'


class BaseVariantAnnotationsTableTest(MockedDatarootTestCase):
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

    @mock.patch(
        'v03_pipeline.lib.tasks.reference_data.updated_reference_dataset_collection.get_datasets_to_update',
    )
    def test_should_create_initialized_table(self, mock_get_datasets_to_update) -> None:
        mock_get_datasets_to_update.return_value = []

        vat_task = BaseVariantAnnotationsTableTask(
            reference_genome=ReferenceGenome.GRCh38,
            dataset_type=DatasetType.SNV_INDEL,
            sample_type=SampleType.WGS,
        )
        self.assertTrue('annotations.ht' in vat_task.output().path)
        self.assertTrue(DatasetType.SNV_INDEL.value in vat_task.output().path)
        self.assertFalse(vat_task.output().exists())
        self.assertFalse(vat_task.complete())

        worker = luigi.worker.Worker()
        worker.add(vat_task)
        worker.run()
        self.assertTrue(GCSorLocalFolderTarget(vat_task.output().path).exists())
        self.assertTrue(vat_task.complete())

        ht = hl.read_table(vat_task.output().path)
        self.assertEqual(ht.count(), 0)
        self.assertEqual(list(ht.key.keys()), ['locus', 'alleles'])
