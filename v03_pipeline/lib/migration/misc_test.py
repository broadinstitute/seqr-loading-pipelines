import os
import shutil
import tempfile
import unittest

from v03_pipeline.lib.migration.base_migration import BaseMigration
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
from v03_pipeline.lib.migration.base_migration import BaseMigration
class ImplementedMigration(BaseMigration):
    pass
                    """,
                )

    def tearDown(self):
        if os.path.isdir(self.tmpdir.name):
            shutil.rmtree(self.tmpdir.name)

    def test_list_migrations(self):
        self.assertEqual(
            [
                (x, issubclass(y, BaseMigration))
                for (x, y) in list_migrations([self.tmpdir.name])
            ],
            [
                ('0000_migration', True),
                ('1111_a_migration', True),
            ],
        )
        self.assertTrue(
            all(hasattr(x[1], 'migrate') for x in list_migrations([self.tmpdir.name])),
        )
