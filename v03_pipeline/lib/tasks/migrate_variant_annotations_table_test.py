from unittest import mock

import hail as hl
import luigi.worker

from v03_pipeline.lib.migration.base_migration import BaseMigration
from v03_pipeline.lib.model import DatasetType, ReferenceGenome
from v03_pipeline.lib.tasks.migrate_variant_annotations_table import (
    MigrateVariantAnnotationsTableTask,
)
from v03_pipeline.lib.test.mocked_dataroot_testcase import MockedDatarootTestCase


class MockMigration(BaseMigration):
    reference_genome_dataset_types: frozenset[
        tuple[ReferenceGenome, DatasetType]
    ] = frozenset(
        ((ReferenceGenome.GRCh38, DatasetType.GCNV),),
    )

    @staticmethod
    def migrate(ht: hl.Table) -> hl.Table:
        ht = ht.annotate(
            variant_id_id=hl.format('%s_id', ht.variant_id),
        )
        return ht.annotate_globals(mock_migration='a mock migration')


class MigrateVariantAnnotationsTableTaskTest(MockedDatarootTestCase):
    @mock.patch(
        'v03_pipeline.lib.tasks.base.base_migrate.list_migrations',
    )
    def test_mock_migration(
        self,
        mock_list_migrations: mock.Mock,
    ) -> None:
        mock_list_migrations.return_value = [
            ('0012_mock_migration', MockMigration),
        ]
        worker = luigi.worker.Worker()
        task = MigrateVariantAnnotationsTableTask(
            dataset_type=DatasetType.GCNV,
            reference_genome=ReferenceGenome.GRCh38,
            migration_name='0012_mock_migration',
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
                    paths=hl.Struct(),
                    versions=hl.Struct(),
                    enums=hl.Struct(),
                    updates=set(),
                    migrations=['0012_mock_migration'],
                    mock_migration='a mock migration',
                ),
            ],
        )
        self.assertEqual(
            ht.collect(),
            [],
        )

    @mock.patch(
        'v03_pipeline.lib.tasks.base.base_migrate.list_migrations',
    )
    def test_migration_is_noop_for_other_dataset_types(
        self,
        mock_list_migrations: mock.Mock,
    ) -> None:
        mock_list_migrations.return_value = [
            ('0012_mock_migration', MockMigration),
        ]
        worker = luigi.worker.Worker()
        task = MigrateVariantAnnotationsTableTask(
            dataset_type=DatasetType.SV,
            reference_genome=ReferenceGenome.GRCh38,
            migration_name='0012_mock_migration',
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
                    paths=hl.Struct(),
                    versions=hl.Struct(),
                    enums=hl.Struct(),
                    updates=set(),
                    migrations=[],
                ),
            ],
        )
