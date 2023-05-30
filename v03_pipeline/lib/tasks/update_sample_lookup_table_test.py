import os
import shutil
import tempfile
import unittest
from unittest.mock import Mock, patch

import hail as hl
import luigi.worker

from v03_pipeline.lib.model import DatasetType, Env, ReferenceGenome, SampleType
from v03_pipeline.lib.tasks.update_sample_lookup_table import (
    UpdateSampleLookupTableTask,
)

TEST_VCF = 'v03_pipeline/var/test/vcfs/1kg_30variants.vcf.bgz'
TEST_REMAP = 'v03_pipeline/var/test/remaps/test_remap_1.tsv'
TEST_PEDIGREE_3 = 'v03_pipeline/var/test/pedigrees/test_pedigree_3.tsv'
TEST_PEDIGREE_4 = 'v03_pipeline/var/test/pedigrees/test_pedigree_4.tsv'
TEST_COMBINED_1 = 'v03_pipeline/var/test/reference_data/test_combined_1.ht'
TEST_HGMD_1 = 'v03_pipeline/var/test/reference_data/test_hgmd_1.ht'
TEST_INTERVAL_REFERENCE_1 = (
    'v03_pipeline/var/test/reference_data/test_interval_reference_1.ht'
)


@patch('v03_pipeline.lib.paths.DataRoot')
class UpdateSampleLookupTableTest(unittest.TestCase):
    def setUp(self) -> None:
        self._temp_local_datasets = tempfile.TemporaryDirectory().name
        self._temp_local_reference_data = tempfile.TemporaryDirectory().name
        shutil.copytree(
            TEST_COMBINED_1,
            f'{self._temp_local_reference_data}/GRCh38/v03/combined.ht',
        )
        shutil.copytree(
            TEST_HGMD_1,
            f'{self._temp_local_reference_data}/GRCh38/v03/hgmd.ht',
        )
        shutil.copytree(
            TEST_INTERVAL_REFERENCE_1,
            f'{self._temp_local_reference_data}/GRCh38/v03/interval_reference.ht',
        )

    def tearDown(self) -> None:
        if os.path.isdir(self._temp_local_datasets):
            shutil.rmtree(self._temp_local_datasets)

        if os.path.isdir(self._temp_local_reference_data):
            shutil.rmtree(self._temp_local_reference_data)

    def test_mulitiple_update_vat(self, mock_dataroot: Mock) -> None:
        mock_dataroot.LOCAL_DATASETS.value = self._temp_local_datasets
        mock_dataroot.LOCAL_REFERENCE_DATA.value = self._temp_local_reference_data
        worker = luigi.worker.Worker()

        uslt_task = UpdateSampleLookupTableTask(
            env=Env.TEST,
            reference_genome=ReferenceGenome.GRCh38,
            dataset_type=DatasetType.SNV,
            sample_type=SampleType.WGS,
            callset_path=TEST_VCF,
            project_remap_path=TEST_REMAP,
            project_pedigree_path=TEST_PEDIGREE_3,
        )
        worker.add(uslt_task)
        worker.run()
        self.assertTrue(uslt_task.complete())
        self.assertEqual(
            hl.read_table(uslt_task.output().path).count(),
            0,
        )
