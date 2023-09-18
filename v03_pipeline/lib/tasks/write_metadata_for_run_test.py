import json
import shutil

import luigi.worker

from v03_pipeline.lib.model import DatasetType, ReferenceGenome, SampleType
from v03_pipeline.lib.tasks.write_metadata_for_run import WriteMetadataForRunTask
from v03_pipeline.lib.test.mocked_dataroot_testcase import MockedDatarootTestCase

TEST_VCF = 'v03_pipeline/var/test/callsets/1kg_30variants.vcf.bgz'
TEST_REMAP = 'v03_pipeline/var/test/remaps/test_remap_1.tsv'
TEST_PEDIGREE_3 = 'v03_pipeline/var/test/pedigrees/test_pedigree_3.tsv'
TEST_PEDIGREE_4 = 'v03_pipeline/var/test/pedigrees/test_pedigree_4.tsv'
TEST_SEX_CHECK_1 = 'v03_pipeline/var/test/sex_check/test_sex_check_1.ht'
TEST_RELATEDNESS_CHECK_1 = (
    'v03_pipeline/var/test/relatedness_check/test_relatedness_check_1.ht'
)


class WriteMetadataForRunTaskTest(MockedDatarootTestCase):
    def setUp(self) -> None:
        super().setUp()
        shutil.copytree(
            TEST_SEX_CHECK_1,
            f'{self.mock_env.LOADING_DATASETS}/v03/GRCh38/SNV_INDEL/sex_check/78d7998164bbe170d4f5282a66873df2e3b18099175069a32565fb0dc08dc3d4.ht',
        )
        shutil.copytree(
            TEST_RELATEDNESS_CHECK_1,
            f'{self.mock_env.LOADING_DATASETS}/v03/GRCh38/SNV_INDEL/relatedness_check/78d7998164bbe170d4f5282a66873df2e3b18099175069a32565fb0dc08dc3d4.ht',
        )

    def test_write_metadata_for_run_task(self) -> None:
        worker = luigi.worker.Worker()
        write_metadata_for_run_task = WriteMetadataForRunTask(
            reference_genome=ReferenceGenome.GRCh38,
            dataset_type=DatasetType.SNV_INDEL,
            sample_type=SampleType.WGS,
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
            f'{self.mock_env.DATASETS}/v03/GRCh38/SNV_INDEL/runs/run_123456/metadata.json',
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
