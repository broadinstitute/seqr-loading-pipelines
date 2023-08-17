import json
import os
import shutil
import tempfile
import unittest
from unittest.mock import Mock, patch

import luigi.worker

from v03_pipeline.lib.model import DatasetType, Env, ReferenceGenome
from v03_pipeline.lib.tasks.write_metadata_for_run import WriteMetadataForRunTask

TEST_VCF = 'v03_pipeline/var/test/callsets/1kg_30variants.vcf.bgz'
TEST_REMAP = 'v03_pipeline/var/test/remaps/test_remap_1.tsv'
TEST_PEDIGREE_3 = 'v03_pipeline/var/test/pedigrees/test_pedigree_3.tsv'
TEST_PEDIGREE_4 = 'v03_pipeline/var/test/pedigrees/test_pedigree_4.tsv'


@patch('v03_pipeline.lib.paths.DataRoot')
class WriteMetadataForRunTaskTest(unittest.TestCase):
    def setUp(self) -> None:
        self._temp_local_datasets = tempfile.TemporaryDirectory().name

    def tearDown(self) -> None:
        if os.path.isdir(self._temp_local_datasets):
            shutil.rmtree(self._temp_local_datasets)

    def test_write_metadata_for_run_task(self, mock_dataroot: Mock) -> None:
        mock_dataroot.LOCAL_DATASETS.value = self._temp_local_datasets
        worker = luigi.worker.Worker()

        write_metadata_for_run_task = WriteMetadataForRunTask(
            env=Env.TEST,
            reference_genome=ReferenceGenome.GRCh38,
            dataset_type=DatasetType.SNV,
            callset_path=TEST_VCF,
            project_guids=['R0113_test_project', 'R0114_project4'],
            project_remap_paths=[TEST_REMAP, TEST_REMAP],
            project_pedigree_paths=[TEST_PEDIGREE_3, TEST_PEDIGREE_4],
            validate=False,
            run_id='run_123456',
        )
        worker.add(write_metadata_for_run_task)
        worker.run()
        self.assertEqual(
            write_metadata_for_run_task.output().path,
            f'{self._temp_local_datasets}/v03/GRCh38/SNV/runs/run_123456/metadata.json',
        )
        self.assertTrue(write_metadata_for_run_task.complete())
        with write_metadata_for_run_task.output().open('r') as f:
            self.assertDictEqual(
                json.load(f),
                {
                    'callset': TEST_VCF,
                    'projects': {
                        'R0113_test_project': [
                            'HG00731_1',
                            'HG00732_1',
                            'HG00733_1',
                        ],
                        'R0114_project4': [
                            'NA19675_1',
                            'NA19678_1',
                            'NA19679_1',
                            'NA20870_1',
                            'NA20872_1',
                            'NA20874_1',
                            'NA20875_1',
                            'NA20876_1',
                            'NA20877_1',
                            'NA20878_1',
                            'NA20881_1',
                            'NA20885_1',
                            'NA20888_1',
                        ],
                    },
                },
            )
