import os
import shutil

from v03_pipeline.lib.core.definitions import ReferenceGenome
from v03_pipeline.lib.paths import valid_reference_dataset_path
from v03_pipeline.lib.reference_datasets.reference_dataset import ReferenceDataset
from v03_pipeline.lib.test.mocked_dataroot_testcase import MockedDatarootTestCase

REFERENCE_DATASETS_PATH = 'v03_pipeline/var/test/reference_datasets'


class MockedReferenceDatasetsTestCase(MockedDatarootTestCase):
    pass
