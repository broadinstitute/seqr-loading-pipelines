import unittest
from unittest import mock
import responses

from v03_pipeline.lib.reference_data.gencode.download_utils import (
    download_file,
)

DEFAULT_TEST_DIR = 'default_test/dir'
TEST_DIR = 'test/dir'
GS_TEST_DIR = 'gs://test-bucket/test/dir'
TEST_TXT_FILE = 'test_file.txt'
TEST_GZ_FILE = 'test_file.gz'
TXT_DATA_URL = 'https://mock_url/test_file.txt'
GZ_DATA_URL = 'https://mock_url/test_file.gz'
GZ_DATA = b'test data\nanother line\n'


class DownloadUtilsTest(unittest.TestCase):
    @responses.activate
    @mock.patch(
        'v03_pipeline.lib.reference_data.gencode.download_utils.DEFAULT_TO_DIR',
        DEFAULT_TEST_DIR,
    )
    @mock.patch('v03_pipeline.lib.reference_data.gencode.download_utils.logger')
    @mock.patch('v03_pipeline.lib.reference_data.gencode.download_utils.os.path.isfile')
    @mock.patch(
        'v03_pipeline.lib.reference_data.gencode.download_utils.os.path.getsize',
    )
    @mock.patch('v03_pipeline.lib.reference_data.gencode.download_utils.open')
    @mock.patch(
        'v03_pipeline.lib.reference_data.gencode.download_utils.tempfile.gettempdir',
    )
    @mock.patch(
        'v03_pipeline.lib.reference_data.gencode.download_utils.parse_gs_path_to_bucket',
    )
    def test_download_file(
        self,
        mock_get_bucket,
        mock_gettempdir,
        mock_open,
        mock_getsize,
        mock_isfile,
        mock_logger,
    ):
        responses.add(
            responses.HEAD, GZ_DATA_URL, headers={'Content-Length': '1024'}, status=200,
        )
        responses.add(responses.GET, GZ_DATA_URL, body=GZ_DATA)
        responses.add(
            responses.HEAD, TXT_DATA_URL, headers={'Content-Length': '1024'}, status=200,
        )
        responses.add(responses.GET, TXT_DATA_URL, body='test data\nanother line\n')

        # Test bad url
        with self.assertRaises(ValueError) as ve:
            download_file('bad_url')
        self.assertEqual(str(ve.exception), 'Invalid url: bad_url')

        # Test already downloaded
        mock_isfile.return_value = True
        mock_getsize.return_value = 1024
        result = download_file(GZ_DATA_URL)
        self.assertEqual(result, 'default_test/dir/test_file.gz')
        mock_open.assert_called_with('default_test/dir/test_file.gz', 'wb')
        mock_isfile.assert_called_with('default_test/dir/test_file.gz')
        mock_getsize.assert_called_with('default_test/dir/test_file.gz')
        mock_logger.info.assert_called_with(
            f'Re-using default_test/dir/test_file.gz previously downloaded from {GZ_DATA_URL}',
        )

        # Test download, .gz file format, verbose
        mock_isfile.reset_mock()
        mock_getsize.reset_mock()
        mock_logger.reset_mock()
        mock_open.reset_mock()
        mock_isfile.return_value = False
        result = download_file(GZ_DATA_URL, TEST_DIR)
        self.assertEqual(result, 'test/dir/test_file.gz')
        mock_isfile.assert_called_with('test/dir/test_file.gz')
        mock_getsize.assert_not_called()
        mock_open.assert_called_with('test/dir/test_file.gz', 'wb')
        mock_logger.info.assert_called_with(
            f'Downloading {GZ_DATA_URL} to test/dir/test_file.gz',
        )

        # Test download, non-.gz file format, non-verbose
        mock_isfile.reset_mock()
        mock_logger.reset_mock()
        mock_open.reset_mock()
        mock_isfile.return_value = False
        result = download_file(TXT_DATA_URL, TEST_DIR, verbose=False)
        self.assertEqual(result, 'test/dir/test_file.txt')
        mock_isfile.assert_called_with('test/dir/test_file.txt')
        mock_getsize.assert_not_called()
        mock_open.assert_called_with('test/dir/test_file.txt', 'wb')
        mock_open.return_value.writelines.assert_called_once()
        mock_logger.info.assert_not_called()

        mock_gettempdir.assert_not_called()
        mock_get_bucket.assert_not_called()

        # Test using Google Storage
        mock_isfile.reset_mock()
        mock_logger.reset_mock()
        mock_open.reset_mock()
        mock_gettempdir.return_value = TEST_DIR
        mock_bucket = mock.MagicMock()
        mock_get_bucket.return_value = mock_bucket, 'test/dir/test_file.gz'
        result = download_file(GZ_DATA_URL, GS_TEST_DIR)
        self.assertEqual(result, 'gs://test-bucket/test/dir/test_file.gz')
        mock_gettempdir.assert_called_once()
        mock_isfile.assert_not_called()
        mock_getsize.assert_not_called()
        mock_open.assert_called_with('test/dir/test_file.gz', 'wb')
        mock_logger.info.assert_called_with(
            f'Downloading {GZ_DATA_URL} to gs://test-bucket/test/dir/test_file.gz',
        )
        mock_bucket.get_blob.assert_called_with('test/dir/test_file.gz')
        mock_bucket.blob.assert_called_with('test/dir/test_file.gz')
        mock_bucket.blob.return_value.upload_from_filename.assert_called_with(
            'test/dir/test_file.gz',
        )
