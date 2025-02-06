import hailtop.fs as hfs
import luigi.worker

from v03_pipeline.lib.model import DatasetType, ReferenceGenome, SampleType
from v03_pipeline.lib.tasks.write_sample_qc_tsv import WriteSampleQCTsvTask
from v03_pipeline.lib.test.mocked_dataroot_testcase import MockedDatarootTestCase

TEST_VCF = 'v03_pipeline/var/test/callsets/1kg_30variants.vcf'
TEST_RUN_ID = 'manual__2024-04-03'


class WriteSampleQCTsvTaskTest(MockedDatarootTestCase):
    def test_call_sample_qc(self):
        worker = luigi.worker.Worker()
        task = WriteSampleQCTsvTask(
            reference_genome=ReferenceGenome.GRCh38,
            dataset_type=DatasetType.SNV_INDEL,
            run_id=TEST_RUN_ID,
            sample_type=SampleType.WGS,
            callset_path=TEST_VCF,
            project_guids=['R0113_test_project'],
            skip_validation=True,
        )
        worker.add(task)
        worker.run()
        self.assertTrue(task)
        self.assertTrue(hfs.exists(task.output().path))

        with task.output().open('r') as f:
            lines = f.readlines()
            expected_first_five_lines = [
                's\tfiltered_callrate\n',
                'HG00731\t1.0000e+00\n',
                'HG00732\t1.0000e+00\n',
                'HG00733\t1.0000e+00\n',
                'NA19675\t1.0000e+00\n',
            ]
            for expected_line, actual_line in zip(
                expected_first_five_lines,
                lines[:5],
                strict=False,
            ):
                self.assertEqual(expected_line, actual_line)
