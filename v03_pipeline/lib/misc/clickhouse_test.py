import unittest

from v03_pipeline.lib.misc.clickhouse import get_clickhouse_client


class ClickhouseTest(unittest.TestCase):
    def test_get_clickhouse_client(self):
        client = get_clickhouse_client()
        result = client.execute('SELECT 1')
        self.assertEqual(result[0][0], 1)
