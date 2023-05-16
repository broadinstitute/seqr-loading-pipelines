import os
import tempfile
import unittest

from v03_pipeline.lib.tasks.files import HailTable, RawFile, VCFFile


class FilesTest(unittest.TestCase):
    def test_raw_file(self) -> None:
        with tempfile.NamedTemporaryFile(suffix='.txt') as f:
            self.assertTrue(RawFile(f.name).complete())

    def test_vcf_file(self) -> None:
        with tempfile.NamedTemporaryFile(suffix='.vcf.bgz') as f:
            self.assertTrue(VCFFile(f.name).complete())

        with tempfile.TemporaryDirectory(suffix='.vcf.bgz') as d:
            self.assertFalse(VCFFile(os.path.join(d, '*.bgz')).complete())
            with open(os.path.join(d, '_SUCCESS'), 'w') as f:
                f.write('0')
            self.assertTrue(VCFFile(os.path.join(d, '*.bgz')).complete())

    def test_hail_table(self) -> None:
        with tempfile.TemporaryDirectory(suffix='.ht') as d:
            self.assertFalse(HailTable(d).complete())
            with open(os.path.join(d, '_SUCCESS'), 'w') as f:
                f.write('0')
            self.assertTrue(HailTable(d).complete())
