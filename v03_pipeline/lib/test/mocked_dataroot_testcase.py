import tempfile
import unittest
from unittest.mock import patch

from v03_pipeline.lib.model import Env


class MockedDatarootTestCase(unittest.TestCase):
    def setUp(self) -> None:
        patcher = patch('v03_pipeline.lib.paths.Env')
        self.mock_env = patcher.start()
        self.addCleanup(patcher.stop)  # https://stackoverflow.com/a/37534051
        for field_name in Env.__dataclass_fields__:
            if 'DATA' in field_name or 'DIR' in field_name:
                setattr(self.mock_env, field_name, tempfile.TemporaryDirectory().name)

    # def tearDown(self) -> None:
    #    for field_name in Env.__dataclass_fields__:
    #        if os.path.isdir(getattr(self.mock_env, field_name)):
    #            shutil.rmtree(getattr(self.mock_env, field_name))
