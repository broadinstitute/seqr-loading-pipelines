import hail as hl
import luigi.worker

from v03_pipeline.lib.model import DatasetType, ReferenceGenome, SampleType
from v03_pipeline.lib.tasks.write_project_family_tables import (
    WriteProjectFamilyTablesTask,
)
from v03_pipeline.lib.test.mocked_dataroot_testcase import MockedDatarootTestCase

TEST_SNV_INDEL_VCF = 'v03_pipeline/var/test/callsets/1kg_30variants.vcf'
TEST_REMAP = 'v03_pipeline/var/test/remaps/test_remap_1.tsv'
TEST_PEDIGREE_4 = 'v03_pipeline/var/test/pedigrees/test_pedigree_4.tsv'


class WriteProjectFamilyTablesTest(MockedDatarootTestCase):
    def test_snv_write_project_family_tables_task(self) -> None:
        worker = luigi.worker.Worker()
        write_project_family_tables = WriteProjectFamilyTablesTask(
            reference_genome=ReferenceGenome.GRCh38,
            dataset_type=DatasetType.SNV_INDEL,
            sample_type=SampleType.WGS,
            callset_path=TEST_SNV_INDEL_VCF,
            project_guid='R0113_test_project',
            project_remap_path=TEST_REMAP,
            project_pedigree_path=TEST_PEDIGREE_4,
            validate=False,
        )
        worker.add(write_project_family_tables)
        worker.run()
        self.assertTrue(write_project_family_tables.complete())
        hts = [
            hl.read_table(write_family_table_task.output().path)
            for write_family_table_task in write_project_family_tables.dynamic_write_family_table_tasks
        ]
        print('bensadf', len(hts))
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
