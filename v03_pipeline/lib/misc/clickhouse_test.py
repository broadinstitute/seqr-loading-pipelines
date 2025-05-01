import unittest

from v03_pipeline.lib.misc.clickhouse import (
    get_clickhouse_client,
    google_xml_native_path,
)


class ClickhouseTest(unittest.TestCase):
    def test_get_clickhouse_client(self):
        client = get_clickhouse_client()
        result = client.execute('SELECT 1')
        self.assertEqual(result[0][0], 1)

    def test_google_xml_native_path(self):
        path = 'gcs://my-bucket/my-file.txt'
        self.assertEqual(
            google_xml_native_path(path),
            'https://storage.googleapis.com/my-bucket/my-file.txt',
        )
        path = '/var/seqr/my-bucket/my-file.txt'
        self.assertEqual(google_xml_native_path(path), path)
