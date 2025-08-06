import os
import shutil

import responses

from v03_pipeline.lib.model.definitions import ReferenceGenome
from v03_pipeline.lib.paths import valid_reference_dataset_path
from v03_pipeline.lib.reference_datasets.reference_dataset import ReferenceDataset
from v03_pipeline.lib.test.mock_clinvar_urls import mock_clinvar_urls
from v03_pipeline.lib.test.mocked_dataroot_testcase import MockedDatarootTestCase

REFERENCE_DATASETS_PATH = 'v03_pipeline/var/test/reference_datasets'


class MockedReferenceDatasetsTestCase(MockedDatarootTestCase):
    def setUp(self) -> None:
        super().setUp()
        responses.start()
        for reference_genome in ReferenceGenome:
            with mock_clinvar_urls(reference_genome):
                path = os.path.join(
                    REFERENCE_DATASETS_PATH,
                    reference_genome.value,
                )
                # Use listdir, allowing for missing datasets
                # in the tests.
                for dataset_name in os.listdir(
                    path,
                ):
                    # Copy the entire directory tree under
                    # the dataset name.
                    shutil.copytree(
                        os.path.join(path, dataset_name),
                        os.path.dirname(
                            valid_reference_dataset_path(
                                reference_genome,
                                ReferenceDataset(dataset_name),
                            ),
                        ),
                    )

    def tearDown(self):
        super().tearDown()
        responses.stop()
        responses.reset()
