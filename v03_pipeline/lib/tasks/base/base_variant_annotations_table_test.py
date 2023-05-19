import os
import shutil
import tempfile
import unittest
from unittest.mock import Mock, patch

import hail as hl
import luigi.worker

from v03_pipeline.lib.definitions import DatasetType, Env, ReferenceGenome
from v03_pipeline.lib.tasks.base.base_variant_annotations_table import (
    BaseVariantAnnotationsTableTask,
)
from v03_pipeline.lib.tasks.files import GCSorLocalFolderTarget


@patch('v03_pipeline.lib.paths.DataRoot')
class BaseVariantAnnotationsTableTest(unittest.TestCase):
    def setUp(self) -> None:
        self._temp_dir = tempfile.TemporaryDirectory().name

    def tearDown(self) -> None:
        if os.path.isdir(self._temp_dir):
            shutil.rmtree(self._temp_dir)

    def test_should_create_empty_vat(self, mock_dataroot: Mock) -> None:
        mock_dataroot.TEST_DATASETS.value = self._temp_dir
        vat_task = BaseVariantAnnotationsTableTask(
            env=Env.TEST,
            reference_genome=ReferenceGenome.GRCh38,
            dataset_type=DatasetType.SNV,
        )
        self.assertEqual(
            vat_task.output().path,
            f'{self._temp_dir}/GRCh38/v03/SNV/annotations.ht',
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

        