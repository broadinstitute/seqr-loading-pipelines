import os
import shutil
import tempfile
import unittest
from unittest.mock import Mock, patch

import hail as hl
import luigi.worker

from v03_pipeline.lib.definitions import DatasetType, Env, ReferenceGenome
from v03_pipeline.lib.misc.io import write_ht
from v03_pipeline.lib.tasks.base.base_variant_annotations_table import (
    BaseVariantAnnotationsTableTask,
)
from v03_pipeline.lib.tasks.files import GCSorLocalFolderTarget


@patch('v03_pipeline.lib.paths.DataRoot')
class BaseVariantAnnotationsTableTest(unittest.TestCase):
    def setUp(self) -> None:
        self._temp_local_datasets = tempfile.TemporaryDirectory().name
        self._temp_local_reference_data = tempfile.TemporaryDirectory().name
        write_ht(
            env=Env.LOCAL,
            ht=hl.Table.parallelize(
                [
                    {
                        'locus': hl.Locus(
                            contig='chr1',
                            position=871269,
                            reference_genome='GRCh38',
                        ),
                        'alleles': ['A', 'C'],
                        'cadd': 1,
                        'clinvar': 2,
                    },
                ],
                hl.tstruct(
                    locus=hl.tlocus('GRCh38'),
                    alleles=hl.tarray(hl.tstr),
                    cadd=hl.tint32,
                    clinvar=hl.tint32,
                ),
                ['locus', 'alleles'],
            ),
            destination_path=os.path.join(
                f'{self._temp_local_reference_data}/GRCh38/v03/combined.ht',
            ),
            checkpoint=False,
        )

    def tearDown(self) -> None:
        if os.path.isdir(self._temp_local_datasets):
            shutil.rmtree(self._temp_local_datasets)

        if os.path.isdir(self._temp_local_reference_data):
            shutil.rmtree(self._temp_local_reference_data)

    def test_should_create_initialized_table(self, mock_dataroot: Mock) -> None:
        mock_dataroot.LOCAL_DATASETS.value = self._temp_local_datasets
        mock_dataroot.LOCAL_REFERENCE_DATA.value = self._temp_local_reference_data
        vat_task = BaseVariantAnnotationsTableTask(
            env=Env.TEST,
            reference_genome=ReferenceGenome.GRCh38,
            dataset_type=DatasetType.SNV,
        )
        self.assertEqual(
            vat_task.output().path,
            f'{self._temp_local_datasets}/GRCh38/v03/SNV/annotations.ht',
        )
        self.assertFalse(vat_task.output().exists())
        self.assertFalse(vat_task.complete())

        worker = luigi.worker.Worker()
        worker.add(vat_task)
        worker.run()
        self.assertTrue(GCSorLocalFolderTarget(vat_task.output().path).exists())
        self.assertTrue(vat_task.complete())

        ht = hl.read_table(vat_task.output().path)
        self.assertEqual(ht.count(), 1)
        self.assertEqual(list(ht.key.keys()), ['locus', 'alleles'])
