import os
import shutil
import tempfile
import unittest
from unittest.mock import patch

import hail as hl
import luigi.worker

from v03_pipeline.lib.definitions import DataRoot, DatasetType, Env, ReferenceGenome
from v03_pipeline.lib.tasks.base_variant_annotations_table import (
    BaseVariantAnnotationsTableTask,
)
from v03_pipeline.lib.tasks.files import GCSorLocalFolderTarget, GCSorLocalTarget


@patch('v03_pipeline.lib.definitions.DataRoot')
class BaseVariantAnnotationsTableTest(unittest.TestCase):

    def setUp(self):
        self._temp_dir = tempfile.TemporaryDirectory().name

    def tearDown(self):
        shutil.rmtree(self._temp_dir)

    def test_base_variant_annotations_table(self, mock_dataroot) -> None:
        mock_dataroot.LOCAL_DATASETS.return_value = self._temp_dir
        vat_task = BaseVariantAnnotationsTableTask(
            env=Env.TEST,
            reference_genome=ReferenceGenome.GRCh38,
            dataset_type=DatasetType.SNV,
        )
        self.assertEqual(vat_task.output().path, 'seqr-datasets/GRCh38/v03/SNV/annotations.ht')
        self.assertFalse(vat_task.output().exists())
        self.assertFalse(vat_task.complete())

        worker = luigi.worker.Worker()
        worker.add(vat_task)
        worker.run()
        self.assertTrue(GCSorLocalFolderTarget(vat_task.output().path).exists())
        self.assertTrue(vat_task.complete())
        
        mt = hl.read_matrix_table(vat_task.output().path)
        self.assertEqual(mt.count(), (0, 0))
        self.assertEqual(mt.row_key.fields, ('locus', 'alleles'))
