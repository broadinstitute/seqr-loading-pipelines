import hail as hl
import luigi.worker

from v03_pipeline.lib.model import DatasetType, ReferenceGenome
from v03_pipeline.lib.paths import (
    lookup_table_path,
    variant_annotations_table_path,
)
from v03_pipeline.lib.tasks.update_variant_annotations_table_with_deleted_families import (
    UpdateVariantAnnotationsTableWithDeletedFamiliesTask,
)
from v03_pipeline.lib.test.mocked_dataroot_testcase import MockedDatarootTestCase


class UpdateVariantAnnotationsTableWithDeletedFamiliesTaskTest(MockedDatarootTestCase):
    def setUp(self) -> None:
        super().setUp()
        ht = hl.Table.parallelize(
            [
                {
                    'id': 0,
                    'project_stats': [
                        [
                            hl.Struct(
                                ref_samples=0,
                                het_samples=0,
                                hom_samples=0,
                            ),
                            hl.Struct(
                                ref_samples=1,
                                het_samples=1,
                                hom_samples=1,
                            ),
                            hl.Struct(
                                ref_samples=2,
                                het_samples=2,
                                hom_samples=2,
                            ),
                        ],
                        [
                            hl.Struct(
                                ref_samples=3,
                                het_samples=3,
                                hom_samples=3,
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
                                het_samples=0,
                                hom_samples=0,
                            ),
                            None,
                            None,
                        ],
                        [
                            hl.Struct(
                                ref_samples=4,
                                het_samples=4,
                                hom_samples=4,
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
                            het_samples=hl.tint32,
                            hom_samples=hl.tint32,
                        ),
                    ),
                ),
            ),
            key='id',
            globals=hl.Struct(
                project_guids=[('project_a', 'WES'), ('project_b', 'WGS')],
                project_families={
                    ('project_a', 'WES'): ['1', '2', '3'],
                    ('project_b', 'WGS'): ['4'],
                },
                updates={
                    hl.Struct(
                        callset='abc',
                        project_guid='project_a',
                        remap_pedigree_hash=123,
                    ),
                    hl.Struct(
                        callset='123',
                        project_guid='project_b',
                        remap_pedigree_hash=123,
                    ),
                },
            ),
        )
        ht.write(
            lookup_table_path(
                ReferenceGenome.GRCh38,
                DatasetType.SNV_INDEL,
            ),
        )
        ht = hl.Table.parallelize(
            [
                {
                    'id': 0,
                    'gt_stats': hl.Struct(AC=0, AN=1, AF=0.25, hom=0),
                },
                {
                    'id': 1,
                    'gt_stats': hl.Struct(AC=0, AN=1, AF=0.25, hom=0),
                },
                {
                    'id': 2,
                    'gt_stats': hl.Struct(AC=0, AN=1, AF=0.25, hom=0),
                },
            ],
            hl.tstruct(
                id=hl.tint32,
                gt_stats=hl.tstruct(
                    AC=hl.tint32,
                    AN=hl.tint32,
                    AF=hl.tfloat32,
                    hom=hl.tint32,
                ),
            ),
            key='id',
            globals=hl.Struct(
                updates={
                    hl.Struct(
                        callset='abc',
                        project_guid='project_a',
                        remap_pedigree_hash=123,
                    ),
                    hl.Struct(
                        callset='123',
                        project_guid='project_b',
                        remap_pedigree_hash=123,
                    ),
                },
            ),
        )
        ht.write(
            variant_annotations_table_path(
                ReferenceGenome.GRCh38,
                DatasetType.SNV_INDEL,
            ),
        )

    def test_update_annotations_with_deleted_project(self) -> None:
        worker = luigi.worker.Worker()
        task = UpdateVariantAnnotationsTableWithDeletedFamiliesTask(
            dataset_type=DatasetType.SNV_INDEL,
            reference_genome=ReferenceGenome.GRCh38,
            project_guid='project_a',
            family_guids=['2', '3'],
        )
        worker.add(task)
        worker.run()
        ht = hl.read_table(task.output().path)
        self.assertEqual(
            ht.globals.collect(),
            [
                hl.Struct(
                    updates={
                        hl.Struct(
                            callset='abc',
                            project_guid='project_a',
                            remap_pedigree_hash=123,
                        ),
                        hl.Struct(
                            callset='123',
                            project_guid='project_b',
                            remap_pedigree_hash=123,
                        ),
                    },
                ),
            ],
        )
        self.assertEqual(
            ht.collect(),
            [
                hl.Struct(id=0, gt_stats=hl.Struct(AC=9, AN=18, AF=0.5, hom=3)),
                hl.Struct(id=1, gt_stats=hl.Struct(AC=12, AN=24, AF=0.5, hom=4)),
            ],
        )
