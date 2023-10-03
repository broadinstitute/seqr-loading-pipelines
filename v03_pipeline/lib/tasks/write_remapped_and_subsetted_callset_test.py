import hail as hl
import luigi.worker

from v03_pipeline.lib.model import DatasetType, ReferenceGenome
from v03_pipeline.lib.tasks.write_remapped_and_subsetted_callset import (
    WriteRemappedAndSubsettedCallsetTask,
)
from v03_pipeline.lib.test.mocked_dataroot_testcase import MockedDatarootTestCase

TEST_VCF = 'v03_pipeline/var/test/callsets/1kg_30variants.vcf.bgz'
TEST_REMAP = 'v03_pipeline/var/test/remaps/test_remap_1.tsv'
TEST_PEDIGREE_3 = 'v03_pipeline/var/test/pedigrees/test_pedigree_3.tsv'


class WriteRemappedAndSubsettedCallsetTaskTest(MockedDatarootTestCase):
    def test_write_remapped_and_subsetted_callset_task(
        self,
    ) -> None:
        worker = luigi.worker.Worker()
        wrsc_task = WriteRemappedAndSubsettedCallsetTask(
            reference_genome=ReferenceGenome.GRCh38,
            dataset_type=DatasetType.SNV_INDEL,
            callset_path=TEST_VCF,
            project_guid='R0113_test_project',
            project_remap_path=TEST_REMAP,
            project_pedigree_path=TEST_PEDIGREE_3,
            validate=False,
        )
        worker.add(wrsc_task)
        worker.run()
        self.assertEqual(
            wrsc_task.output().path,
            f'{self.mock_env.LOADING_DATASETS}/v03/GRCh38/SNV_INDEL/remapped_and_subsetted_callsets/R0113_test_project/78d7998164bbe170d4f5282a66873df2e3b18099175069a32565fb0dc08dc3d4.mt',
        )
        self.assertTrue(wrsc_task.complete())
        mt = hl.read_matrix_table(wrsc_task.output().path)
        self.assertEqual(mt.count(), (30, 3))
        self.assertEqual(
            mt.globals.collect(),
            [hl.Struct(family_guids=['abc_1'])],
        )
