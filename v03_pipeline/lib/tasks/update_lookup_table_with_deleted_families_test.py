from unittest import mock

import hail as hl
import luigi.worker

from v03_pipeline.lib.model import DatasetType, ReferenceGenome
from v03_pipeline.lib.tasks.update_lookup_table_with_deleted_families import (
    UpdateLookupTableWithDeletedFamiliesTask,
)
from v03_pipeline.lib.test.mocked_dataroot_testcase import MockedDatarootTestCase


class UpdateLookupTableWithDeletedProjectTaskTest(MockedDatarootTestCase):
    def test_delete_project_empty_table(
        self,
    ) -> None:
        worker = luigi.worker.Worker()
        task = UpdateLookupTableWithDeletedFamiliesTask(
            dataset_type=DatasetType.SNV_INDEL,
            reference_genome=ReferenceGenome.GRCh38,
            project_guid='R0555_seqr_demo',
            family_guids=['abc'],
        )
        worker.add(task)
        worker.run()
        self.assertTrue(task.output().exists())
        self.assertTrue(task.complete())
        ht = hl.read_table(task.output().path)
        self.assertEqual(
            ht.globals.collect(),
            [
                hl.Struct(
                    project_guids=[],
                    project_families={},
                    updates=set(),
                ),
            ],
        )
        self.assertEqual(ht.collect(), [])

    @mock.patch(
        'v03_pipeline.lib.tasks.update_lookup_table_with_deleted_families.UpdateLookupTableWithDeletedFamiliesTask.initialize_table',
    )
    def test_delete_project(
        self,
        mock_initialize_table: mock.Mock,
    ) -> None:
        mock_initialize_table.return_value = hl.Table.parallelize(
            [
                {
                    'id': 0,
                    'project_stats': [
                        [
                            hl.Struct(
                                ref_samples=0,
                                heteroplasmic_samples=0,
                                homoplasmic_samples=0,
                            ),
                            hl.Struct(
                                ref_samples=1,
                                heteroplasmic_samples=1,
                                homoplasmic_samples=1,
                            ),
                            hl.Struct(
                                ref_samples=2,
                                heteroplasmic_samples=2,
                                homoplasmic_samples=2,
                            ),
                        ],
                        [
                            hl.Struct(
                                ref_samples=3,
                                heteroplasmic_samples=3,
                                homoplasmic_samples=3,
                            ),
                        ],
                    ],
                },
                {
                    'id': 1,
                    'project_stats': [
                        [
                            hl.Struct(
                                ref_samples=0,
                                heteroplasmic_samples=0,
                                homoplasmic_samples=0,
                            ),
                            hl.Struct(
                                ref_samples=1,
                                heteroplasmic_samples=1,
                                homoplasmic_samples=1,
                            ),
                            hl.Struct(
                                ref_samples=2,
                                heteroplasmic_samples=2,
                                homoplasmic_samples=2,
                            ),
                        ],
                        [
                            hl.Struct(
                                ref_samples=3,
                                heteroplasmic_samples=3,
                                homoplasmic_samples=3,
                            ),
                        ],
                    ],
                },
            ],
            hl.tstruct(
                id=hl.tint32,
                project_stats=hl.tarray(
                    hl.tarray(
                        hl.tstruct(
                            ref_samples=hl.tint32,
                            heteroplasmic_samples=hl.tint32,
                            homoplasmic_samples=hl.tint32,
                        ),
                    ),
                ),
            ),
            key='id',
            globals=hl.Struct(
                project_guids=['project_a', 'project_b'],
                project_families={'project_a': ['1', '2', '3'], 'project_b': ['4']},
                updates={
                    hl.Struct(
                        project_guid='project_a',
                        callset='abc',
                        remap_pedigree_hash=123,
                    ),
                    hl.Struct(
                        project_guid='project_b',
                        callset='abc',
                        remap_pedigree_hash=123,
                    ),
                },
            ),
        )
        worker = luigi.worker.Worker()
        task = UpdateLookupTableWithDeletedFamiliesTask(
            dataset_type=DatasetType.SNV_INDEL,
            reference_genome=ReferenceGenome.GRCh38,
            project_guid='project_a',
            family_guids=['1', '3'],
        )
        worker.add(task)
        worker.run()
        self.assertTrue(task.output().exists())
        self.assertTrue(task.complete())
        ht = hl.read_table(task.output().path)
        self.assertEqual(
            ht.globals.collect(),
            [
                hl.Struct(
                    project_guids=['project_a', 'project_b'],
                    project_families={'project_a': ['2'], 'project_b': ['4']},
                    updates={
                        hl.Struct(
                            project_guid='project_a',
                            callset='abc',
                            remap_pedigree_hash=123,
                        ),
                        hl.Struct(
                            project_guid='project_b',
                            callset='abc',
                            remap_pedigree_hash=123,
                        ),
                    },
                ),
            ],
        )
        self.assertEqual(
            ht.collect(),
            [
                hl.Struct(
                    id=0,
                    project_stats=[
                        [
                            hl.Struct(
                                ref_samples=1,
                                heteroplasmic_samples=1,
                                homoplasmic_samples=1,
                            ),
                        ],
                        [
                            hl.Struct(
                                ref_samples=3,
                                heteroplasmic_samples=3,
                                homoplasmic_samples=3,
                            ),
                        ],
                    ],
                ),
                hl.Struct(
                    id=1,
                    project_stats=[
                        [
                            hl.Struct(
                                ref_samples=1,
                                heteroplasmic_samples=1,
                                homoplasmic_samples=1,
                            ),
                        ],
                        [
                            hl.Struct(
                                ref_samples=3,
                                heteroplasmic_samples=3,
                                homoplasmic_samples=3,
                            ),
                        ],
                    ],
                ),
            ],
        )
