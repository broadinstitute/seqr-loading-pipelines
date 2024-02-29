import os
from pathlib import Path

import hail as hl
import luigi.worker

from v03_pipeline.lib.model import DatasetType, ReferenceGenome, SampleType
from v03_pipeline.lib.paths import runs_path
from v03_pipeline.lib.tasks.delete_old_runs import DeleteOldRunsTask
from v03_pipeline.lib.test.mocked_dataroot_testcase import MockedDatarootTestCase


class DeleteOldRunsTaskTest(MockedDatarootTestCase):
    def test_too_few_successful_runs(self) -> None:
        for run_dir in [
            'manual__2024-01-05',
            'manual__2024-01-06',
            'manual__2024-01-07',
            'manual__2024-01-08',
            'manual__2024-01-09',
            'manual__2024-01-10',
            'manual__2024-01-11',
        ]:
            run_dir_path = os.path.join(
                runs_path(
                    ReferenceGenome.GRCh38,
                    DatasetType.SNV_INDEL,
                ),
                run_dir,
            )
            Path(run_dir_path).mkdir(parents=True, exist_ok=True)
            Path((run_dir_path) / Path('_SUCCESS')).touch()
        worker = luigi.worker.Worker()
        write_metadata_for_run_task = DeleteOldRunsTask(
            reference_genome=ReferenceGenome.GRCh38,
            dataset_type=DatasetType.SNV_INDEL,
            sample_type=SampleType.WGS,
        )
        worker.add(write_metadata_for_run_task)
        worker.run()
        self.assertTrue(write_metadata_for_run_task.complete())
        self.assertEqual(
            len(
                hl.hadoop_ls(
                    runs_path(
                        ReferenceGenome.GRCh38,
                        DatasetType.SNV_INDEL,
                    ),
                ),
            ),
            7,
        )

    def test_leave_incomplete_runs(self) -> None:
        for run_dir in [
            'manual__2024-01-05',
            'manual__2024-01-06',
            'manual__2024-01-07',
            'manual__2024-01-08',
            'manual__2024-01-09',
            'manual__2024-01-10',
            'manual__2024-01-11',
            'manual__2024-01-12',
            'manual__2024-01-13',
            'manual__2024-01-14',
            'manual__2024-01-15',
            'manual__2024-01-16',
            'manual__2024-01-17',
        ]:
            run_dir_path = os.path.join(
                runs_path(
                    ReferenceGenome.GRCh38,
                    DatasetType.SNV_INDEL,
                ),
                run_dir,
            )
            Path(run_dir_path).mkdir(parents=True, exist_ok=True)

            # Force a couple of incomplete runs
            if run_dir not in {'manual__2024-01-13', 'manual__2024-01-16'}:
                Path((run_dir_path) / Path('_SUCCESS')).touch()

        worker = luigi.worker.Worker()
        write_metadata_for_run_task = DeleteOldRunsTask(
            reference_genome=ReferenceGenome.GRCh38,
            dataset_type=DatasetType.SNV_INDEL,
            sample_type=SampleType.WGS,
        )
        worker.add(write_metadata_for_run_task)
        worker.run()
        self.assertTrue(write_metadata_for_run_task.complete())
        self.assertEqual(
            len(
                hl.hadoop_ls(
                    runs_path(
                        ReferenceGenome.GRCh38,
                        DatasetType.SNV_INDEL,
                    ),
                ),
            ),
            12,
        )
        self.assertFalse(
            hl.hadoop_exists(
                os.path.join(
                    runs_path(
                        ReferenceGenome.GRCh38,
                        DatasetType.SNV_INDEL,
                    ),
                    'manual__2024-01-05',
                ),
            ),
        )
