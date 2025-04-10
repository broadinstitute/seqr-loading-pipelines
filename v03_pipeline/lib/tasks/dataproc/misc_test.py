import unittest
from unittest.mock import Mock, patch

from v03_pipeline.lib.model import DatasetType, ReferenceGenome, SampleType
from v03_pipeline.lib.tasks.dataproc.misc import to_kebab_str_args
from v03_pipeline.lib.tasks.dataproc.rsync_to_seqr_app_dirs import (
    RsyncToSeqrAppDirsTask,
)


@patch(
    'v03_pipeline.lib.tasks.dataproc.base_run_job_on_dataproc.dataproc.JobControllerClient',
)
class MiscTest(unittest.TestCase):
    def test_to_kebab_str_args(self, _: Mock):
        t = RsyncToSeqrAppDirsTask(
            reference_genome=ReferenceGenome.GRCh38,
            dataset_type=DatasetType.SNV_INDEL,
            sample_type=SampleType.WGS,
            callset_path='test_callset',
            project_guids=['R0113_test_project'],
            project_pedigree_paths=['test_pedigree'],
            run_id='a_misc_run',
        )
        self.assertListEqual(
            to_kebab_str_args(t),
            [
                '--reference-genome',
                'GRCh38',
                '--dataset-type',
                'SNV_INDEL',
                '--run-id',
                'a_misc_run',
                '--sample-type',
                'WGS',
                '--callset-path',
                'test_callset',
                '--project-guids',
                '["R0113_test_project"]',
                '--project-pedigree-paths',
                '["test_pedigree"]',
                '--skip-check-sex-and-relatedness',
                'False',
                '--skip-expect-tdr-metrics',
                'False',
                '--skip-validation',
                'False',
                '--is-new-gcnv-joint-call',
                'False',
            ],
        )
