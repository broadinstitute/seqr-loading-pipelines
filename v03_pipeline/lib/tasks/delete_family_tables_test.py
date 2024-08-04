import pathlib

import hail as hl
import luigi.worker

from v03_pipeline.lib.model import DatasetType, ReferenceGenome, SampleType
from v03_pipeline.lib.paths import family_table_path
from v03_pipeline.lib.tasks.delete_family_tables import (
    DeleteFamilyTablesTask,
)
from v03_pipeline.lib.test.mocked_dataroot_testcase import MockedDatarootTestCase


class DeleteFamilyTablesTaskTest(MockedDatarootTestCase):
    def setUp(self) -> None:
        super().setUp()
        for family_guid in ['family_a', 'family_b']:
            family_ht = hl.utils.range_table(100)
            family_ht.write(
                family_table_path(
                    ReferenceGenome.GRCh38,
                    DatasetType.SNV_INDEL,
                    SampleType.WGS,
                    family_guid,
                ),
            )

    def test_delete_project_family_tables_task(self) -> None:
        self.assertTrue(
            pathlib.Path(
                family_table_path(
                    ReferenceGenome.GRCh38,
                    DatasetType.SNV_INDEL,
                    SampleType.WGS,
                    'family_a',
                ),
            ).exists(),
        )
        worker = luigi.worker.Worker()
        task = DeleteFamilyTablesTask(
            reference_genome=ReferenceGenome.GRCh38,
            dataset_type=DatasetType.SNV_INDEL,
            sample_types=[SampleType.WGS],
            family_guids=['family_a', 'family_b'],
        )
        worker.add(task)
        worker.run()
        self.assertTrue(task.complete())
        for family_guid in ['family_a', 'family_b']:
            self.assertFalse(
                pathlib.Path(
                    family_table_path(
                        ReferenceGenome.GRCh38,
                        DatasetType.SNV_INDEL,
                        SampleType.WGS,
                        family_guid,
                    ),
                ).exists(),
            )

    def test_delete_project_family_tables_task_without_sample_type(self) -> None:
        self.assertTrue(
            pathlib.Path(
                family_table_path(
                    ReferenceGenome.GRCh38,
                    DatasetType.SNV_INDEL,
                    SampleType.WGS,
                    'family_a',
                ),
            ).exists(),
        )
        self.assertFalse(
            pathlib.Path(
                family_table_path(
                    ReferenceGenome.GRCh38,
                    DatasetType.SNV_INDEL,
                    SampleType.WES,
                    'family_a',
                ),
            ).exists(),
        )
        worker = luigi.worker.Worker()
        task = DeleteFamilyTablesTask(
            reference_genome=ReferenceGenome.GRCh38,
            dataset_type=DatasetType.SNV_INDEL,
            family_guids=['family_a', 'family_b'],
        )
        worker.add(task)
        worker.run()
        self.assertTrue(task.complete())
