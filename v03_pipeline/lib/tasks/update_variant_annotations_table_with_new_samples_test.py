import os
import shutil
import tempfile
import unittest
from unittest.mock import Mock, patch

import luigi.worker

from v03_pipeline.lib.definitions import DatasetType, Env, ReferenceGenome, SampleType
from v03_pipeline.lib.tasks.update_variant_annotations_table_with_new_samples import (
    UpdateVariantAnnotationsTableWithNewSamples,
)

TEST_VCF = 'v03_pipeline/var/test/vcfs/1kg_30variants.vcf.bgz'
TEST_REMAP = 'v03_pipeline/var/test/remaps/test_remap_1.tsv'
TEST_PEDIGREE = 'v03_pipeline/var/test/pedigrees/test_pedigree_3.tsv'


@patch('v03_pipeline.lib.paths.DataRoot')
class UpdateVariantAnnotationsTableWithNewSamplesTest(unittest.TestCase):
    def setUp(self) -> None:
        self._temp_dir = tempfile.TemporaryDirectory().name

    def tearDown(self) -> None:
        if os.path.isdir(self._temp_dir):
            shutil.rmtree(self._temp_dir)

    def test_missing_pedigree(self, mock_dataroot: Mock) -> None:
        mock_dataroot.TEST_DATASETS.value = self._temp_dir
        uvatwns_task = UpdateVariantAnnotationsTableWithNewSamples(
            env=Env.TEST,
            reference_genome=ReferenceGenome.GRCh38,
            dataset_type=DatasetType.SNV,
            sample_type=SampleType.WGS,
            vcf_path=TEST_VCF,
            project_remap_path=TEST_REMAP,
            project_pedigree_path='bad_pedigree',
        )

        worker = luigi.worker.Worker()
        worker.add(uvatwns_task)
        worker.run()
        self.assertFalse(uvatwns_task.complete())

    def test_update_vat(self, mock_dataroot: Mock) -> None:
        mock_dataroot.TEST_DATASETS.value = self._temp_dir
        uvatwns_task = UpdateVariantAnnotationsTableWithNewSamples(
            env=Env.TEST,
            reference_genome=ReferenceGenome.GRCh38,
            dataset_type=DatasetType.SNV,
            sample_type=SampleType.WGS,
            vcf_path=TEST_VCF,
            project_remap_path=TEST_REMAP,
            project_pedigree_path=TEST_PEDIGREE,
        )

        worker = luigi.worker.Worker()
        worker.add(uvatwns_task)
        worker.run()
        self.assertFalse(uvatwns_task.complete())
