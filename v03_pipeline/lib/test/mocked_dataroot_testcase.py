import os
import tempfile
import unittest
from unittest.mock import patch

from v03_pipeline.lib.model import DataRoot


class MockedDatarootTestCase(unittest.TestCase):
    def setUp(self) -> None:
        self.patcher = patch('v03_pipeline.lib.paths.DataRoot')
        self.mock_dataroot = self.patcher.start()
        for field_name in DataRoot.__dataclass_fields__:
            setattr(self.mock_dataroot, field_name, tempfile.TemporaryDirectory().name)

    def tearDown(self) -> None:
        for field_name in DataRoot.__dataclass_fields__:
            if os.path.isdir(getattr(self.mock_dataroot, field_name)):
                getattr(self.mock_dataroot, field_name)
        self.patcher.stop()
