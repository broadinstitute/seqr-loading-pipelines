import os
import shutil
import tempfile
import unittest

from v03_pipeline.lib.misc.migration import list_migrations


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
            with open(os.path.join(self.tmpdir.name, migration), 'w'):
                os.utime(os.path.join(self.tmpdir.name, migration), None)

    def tearDown(self):
        if os.path.isdir(self.tmpdir.name):
            shutil.rmtree(self.tmpdir.name)

    def test_list_migrations(self):
        self.assertEqual(
            list_migrations(
                path=os.path.join(self.tmpdir.name, '*.py'),
            ),
            [
                '0000_migration',
                '1111_a_migration',
            ],
        )