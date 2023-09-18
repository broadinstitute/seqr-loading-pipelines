import os
import tempfile
import unittest
from unittest.mock import patch

from v03_pipeline.lib.model import Env


class MockedDatarootTestCase(unittest.TestCase):
    def setUp(self) -> None:
        self.patcher = patch('v03_pipeline.lib.paths.Env')
        self.mock_env = self.patcher.start()
        for field_name in Env.__dataclass_fields__:
            if 'DATASETS' in field_name:
                continue
            setattr(self.mock_env, field_name, tempfile.TemporaryDirectory().name)

    def tearDown(self) -> None:
        for field_name in Env.__dataclass_fields__:
            if os.path.isdir(getattr(self.mock_env, field_name)):
                os.path.rmtree(getattr(self.mock_env, field_name))
        self.patcher.stop()
