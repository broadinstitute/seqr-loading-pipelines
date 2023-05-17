import os
import tempfile
import unittest

import luigi.worker

from v03_pipeline.lib.definitions import DataRoot, Env, ReferenceGenome, DatasetType, SampleType
from v03_pipeline.lib.tasks.base_variant_annotations_table import BaseVariantAnnotationsTableTask
from v03_pipeline.lib.tasks.files import GCSorLocalTarget, GCSorLocalFolderTarget


class BaseVariantAnnotationsTableTest(unittest.TestCase):
    
    def test_base_variant_annotations_table(self) -> None:
        vat_task = BaseVariantAnnotationsTableTask(
            env=Env.LOCAL,
            reference_genome=ReferenceGenome.GRCh38,
            dataset_type=DatasetType.SNV,
            sample_type=SampleType.WGS,
        )
        self.assertEqual(
            vat_task._variant_annotations_table_path,
            '/seqr-datasets/GRCh38/v03/SNV/annotations.ht'
        )
        self.assertFalse(
            GCSorLocalTarget(vat_task._variant_annotations_table_path).exists()
        )
        self.assertFalse(
            GCSorLocalFolderTarget(vat_task._variant_annotations_table_path).exists()
        )

        worker = luigi.worker.Worker()
        worker.add(vat_task)
        worker.run()

        self.assertTrue(
            GCSorLocalFolderTarget(vat_task._variant_annotations_table_path).exists()
        )

