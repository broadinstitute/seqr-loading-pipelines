import hail as hl
import luigi.worker

from v03_pipeline.lib.model import DatasetType, ReferenceGenome, SampleType
from v03_pipeline.lib.paths import (
    project_table_path,
    remapped_and_subsetted_callset_path,
)
from v03_pipeline.lib.tasks.write_project_family_tables import (
    WriteProjectFamilyTablesTask,
)
from v03_pipeline.lib.test.misc import copy_test_project_pedigree
from v03_pipeline.lib.test.mocked_dataroot_testcase import MockedDatarootTestCase

TEST_SNV_INDEL_VCF = 'v03_pipeline/var/test/callsets/1kg_30variants.vcf'
TEST_PEDIGREE_4_REMAP = 'v03_pipeline/var/test/pedigrees/test_pedigree_4_remap.tsv'
TEST_PEDIGREE_4_SUBSET = 'v03_pipeline/var/test/pedigrees/test_pedigree_4_subset.tsv'

TEST_RUN_ID = 'manual__2024-04-03'


class WriteProjectFamilyTablesTest(MockedDatarootTestCase):
    def test_snv_write_project_family_tables_task(self) -> None:
        copy_test_project_pedigree(
            TEST_PEDIGREE_4_REMAP,
            ReferenceGenome.GRCh38,
            DatasetType.SNV_INDEL,
            SampleType.WGS,
            'R0113_test_project',
        )
        worker = luigi.worker.Worker()
        write_project_family_tables = WriteProjectFamilyTablesTask(
            reference_genome=ReferenceGenome.GRCh38,
            dataset_type=DatasetType.SNV_INDEL,
            run_id=TEST_RUN_ID,
            sample_type=SampleType.WGS,
            callset_path=TEST_SNV_INDEL_VCF,
            project_guids=['R0113_test_project'],
            project_i=0,
            skip_validation=True,
            skip_check_sex_and_relatedness=True,
        )
        worker.add(write_project_family_tables)
        worker.run()
        self.assertTrue(write_project_family_tables.complete())
        hts = [
            hl.read_table(write_family_table_task.output().path)
            for write_family_table_task in write_project_family_tables.dynamic_write_family_table_tasks
        ]
        # Validate remapped and subsetted callset families
        remapped_and_subsetted_callset = hl.read_matrix_table(
            remapped_and_subsetted_callset_path(
                ReferenceGenome.GRCh38,
                DatasetType.SNV_INDEL,
                TEST_SNV_INDEL_VCF,
                'R0113_test_project',
            ),
        )
        self.assertCountEqual(
            hl.eval(remapped_and_subsetted_callset.globals.family_samples.keys()),
            {
                '123_1',
                '234_1',
                '345_1',
                '456_1',
                '567_1',
                '678_1',
                '789_1',
                '890_1',
                '901_1',
                'bcd_1',
                'cde_1',
                'def_1',
                'efg_1',
            },
        )
        self.assertCountEqual(
            [ht.globals.sample_ids.collect() for ht in hts],
            [
                [['NA19675_1']],
                [['NA19678_1']],
                [['NA19679_1']],
                [['NA20870_1']],
                [['NA20872_1']],
                [['NA20874_1']],
                [['NA20875_1']],
                [['NA20876_1']],
                [['NA20877_1']],
                [['NA20878_1']],
                [['NA20881_1']],
                [['NA20885_1']],
                [['NA20888_1']],
            ],
        )

        copy_test_project_pedigree(
            TEST_PEDIGREE_4_SUBSET,
            ReferenceGenome.GRCh38,
            DatasetType.SNV_INDEL,
            SampleType.WGS,
            'R0113_test_project',
        )
        write_project_family_tables_subset = WriteProjectFamilyTablesTask(
            reference_genome=ReferenceGenome.GRCh38,
            dataset_type=DatasetType.SNV_INDEL,
            run_id=TEST_RUN_ID,
            sample_type=SampleType.WGS,
            callset_path=TEST_SNV_INDEL_VCF,
            project_guids=['R0113_test_project'],
            project_i=0,
            skip_validation=True,
            skip_check_sex_and_relatedness=True,
        )
        worker.add(write_project_family_tables_subset)
        worker.run()
        self.assertTrue(write_project_family_tables_subset.complete())
        hts = [
            write_family_table_task.output().path
            for write_family_table_task in write_project_family_tables_subset.dynamic_write_family_table_tasks
        ]
        self.assertTrue(len(hts))
        self.assertTrue(
            '123_1' in hts[0],
        )
        # Validate remapped and subsetted callset families
        # (and that it was re-written)
        remapped_and_subsetted_callset = hl.read_matrix_table(
            remapped_and_subsetted_callset_path(
                ReferenceGenome.GRCh38,
                DatasetType.SNV_INDEL,
                TEST_SNV_INDEL_VCF,
                'R0113_test_project',
            ),
        )
        self.assertCountEqual(
            hl.eval(remapped_and_subsetted_callset.globals.family_samples.keys()),
            {'123_1'},
        )
        self.assertCountEqual(
            hl.eval(remapped_and_subsetted_callset.globals.failed_family_samples),
            hl.Struct(
                missing_samples={
                    '234_1': {
                        'reasons': ["Missing samples: {'NA19678_999'}"],
                        'samples': ['NA19678_1', 'NA19678_999'],
                    },
                },
                relatedness_check={},
                sex_check={},
                ploidy_check={},
            ),
        )
        # Project table still contains all family guids
        self.assertCountEqual(
            hl.read_table(
                project_table_path(
                    ReferenceGenome.GRCh38,
                    DatasetType.SNV_INDEL,
                    SampleType.WGS,
                    'R0113_test_project',
                ),
            ).family_guids.collect()[0],
            [
                '123_1',
                '234_1',
                '345_1',
                '456_1',
                '567_1',
                '678_1',
                '789_1',
                '890_1',
                '901_1',
                'bcd_1',
                'cde_1',
                'def_1',
                'efg_1',
            ],
        )
