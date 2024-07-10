import os
import shutil
import tempfile
import unittest
from unittest.mock import ANY

from v03_pipeline.lib.migration.misc import list_migrations


class TestListMigrations(unittest.TestCase):
    def setUp(self):
        self.tmpdir = tempfile.TemporaryDirectory()
        for migration in [
            '__init__.py',
            '1111_a_migration.py',
            '0000_migration.py',
            '001_test.py',
            'abcd_test.py',
            '0000_migration.txt',
        ]:
            with open(os.path.join(self.tmpdir.name, migration), 'w') as f:
                f.write(
                    """
from v03_pipeline.migrations.base_migration import BaseMigration
class ImplementedMigration(BaseMigration):
    pass
                    """,
                )

    def tearDown(self):
        if os.path.isdir(self.tmpdir.name):
            shutil.rmtree(self.tmpdir.name)

    def test_list_migrations(self):
        self.assertEqual(
            list_migrations(self.tmpdir.name),
            [
                ('0000_migration', ANY),
                ('1111_a_migration', ANY),
            ],
        )
        self.assertTrue(
            all(hasattr(x[1], 'migrate') for x in list_migrations(self.tmpdir.name)),
        )
