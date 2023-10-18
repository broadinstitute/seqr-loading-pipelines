import shutil

import hail as hl
import luigi.worker

from v03_pipeline.lib.model import DatasetType, ReferenceGenome, SampleType
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
            f'{self.mock_env.REFERENCE_DATASETS}/v03/GRCh38/reference_datasets/SNV_INDEL/combined.ht',
        )
        shutil.copytree(
            TEST_HGMD_1,
            f'{self.mock_env.PRIVATE_REFERENCE_DATASETS}/v03/GRCh38/reference_datasets/SNV_INDEL/hgmd.ht',
        )
        shutil.copytree(
            TEST_INTERVAL_1,
            f'{self.mock_env.REFERENCE_DATASETS}/v03/GRCh38/reference_datasets/SNV_INDEL/interval.ht',
        )

    def test_should_create_initialized_table(self) -> None:
        vat_task = BaseVariantAnnotationsTableTask(
            reference_genome=ReferenceGenome.GRCh38,
            dataset_type=DatasetType.SNV_INDEL,
            sample_type=SampleType.WGS,
        )
        self.assertEqual(
            vat_task.output().path,
            f'{self.mock_env.DATASETS}/v03/GRCh38/SNV_INDEL/annotations.ht',
        )
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
