import json

import luigi.worker

from v03_pipeline.lib.model import DatasetType, ReferenceGenome, SampleType
from v03_pipeline.lib.tasks.write_metadata_for_run import WriteMetadataForRunTask
from v03_pipeline.lib.test.mocked_dataroot_testcase import MockedDatarootTestCase

TEST_VCF = 'v03_pipeline/var/test/callsets/1kg_30variants.vcf'
TEST_REMAP_2 = 'v03_pipeline/var/test/remaps/test_remap_2.tsv'
TEST_PEDIGREE_3 = 'v03_pipeline/var/test/pedigrees/test_pedigree_3.tsv'
TEST_PEDIGREE_4 = 'v03_pipeline/var/test/pedigrees/test_pedigree_4.tsv'


class WriteMetadataForRunTaskTest(MockedDatarootTestCase):
    def test_write_metadata_for_run_task(self) -> None:
        worker = luigi.worker.Worker()
        write_metadata_for_run_task = WriteMetadataForRunTask(
            reference_genome=ReferenceGenome.GRCh38,
            dataset_type=DatasetType.SNV_INDEL,
            sample_type=SampleType.WGS,
            callset_paths=[TEST_VCF],
            project_guids=['R0113_test_project', 'R0114_project4'],
            project_remap_paths=[TEST_REMAP_2, TEST_REMAP_2],
            project_pedigree_paths=[TEST_PEDIGREE_3, TEST_PEDIGREE_4],
            validate=False,
            check_sex_and_relatedness=False,
            run_id='run_123456',
        )
        worker.add(write_metadata_for_run_task)
        worker.run()
        self.assertTrue(
            'run_123456/metadata.json' in write_metadata_for_run_task.output().path,
        )
        self.assertTrue(write_metadata_for_run_task.complete())
        with write_metadata_for_run_task.output().open('r') as f:
            self.assertDictEqual(
                json.load(f),
                {
                    'callsets': [TEST_VCF],
                    'failed_family_samples': {
                        'missing_samples': {
                            'efg_1': {
                                # This sample is present in the callset, but intentionally
                                # mapped away
                                'samples': ['NA20888_1'],
                                'reasons': ["Missing samples: {'NA20888_1'}"],
                            },
                        },
                        'relatedness_check': {},
                        'sex_check': {},
                    },
                    'family_samples': {
                        'abc_1': [
                            'HG00731_1',
                            'HG00732_1',
                            'HG00733_1',
                        ],
                        '123_1': ['NA19675_1'],
                        '234_1': ['NA19678_1'],
                        '345_1': ['NA19679_1'],
                        '456_1': ['NA20870_1'],
                        '567_1': ['NA20872_1'],
                        '678_1': ['NA20874_1'],
                        '789_1': ['NA20875_1'],
                        '890_1': ['NA20876_1'],
                        '901_1': ['NA20877_1'],
                        'bcd_1': ['NA20878_1'],
                        'cde_1': ['NA20881_1'],
                        'def_1': ['NA20885_1'],
                    },
                    'run_id': 'run_123456',
                    'sample_type': SampleType.WGS.value,
                },
            )
