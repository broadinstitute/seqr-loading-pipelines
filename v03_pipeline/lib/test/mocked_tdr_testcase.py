import logging
from unittest.mock import patch

from v03_pipeline.lib.test.mocked_dataroot_testcase import MockedDatarootTestCase

logger = logging.getLogger()


class MockedTDRTestCase(MockedDatarootTestCase):
    def setUp(self) -> None:
        super().setUp()
        self.mock_bq_metrics_query = patch(
            'v03_pipeline.lib.tasks.write_tdr_metrics_file.bq_metrics_query',
        ).start()
        self.mock_gen_bq_table_names = patch(
            'v03_pipeline.lib.tasks.write_tdr_metrics_files.gen_bq_table_names',
        ).start()
        self.addCleanup(patch.stopall)

    def set_bq_metrics_query_side_effect(self, tables):
        self.mock_bq_metrics_query.side_effect = [iter(rows) for rows in tables]

    def tearDown(self):
        self.mock_bq_metrics_query.side_effect = None
        super().tearDown()
