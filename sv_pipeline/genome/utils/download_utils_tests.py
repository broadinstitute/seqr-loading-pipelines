import unittest
import mock
import responses

from sv_pipeline.genome.utils.download_utils import download_file

TEST_DIR = 'test/dir'
TEST_TXT_FILE = 'test_file.txt'
TEST_GZ_FILE = 'test_file.gz'
TXT_DATA_URL = 'https://mock_url/test_file.txt'
GZ_DATA_URL = 'https://mock_url/test_file.gz'


class DownloadUtilsTest(unittest.TestCase):

    @responses.activate
    @mock.patch('sv_pipeline.genome.utils.download_utils.logger')
    @mock.patch('sv_pipeline.genome.utils.download_utils.os.path.isfile')
    @mock.patch('sv_pipeline.genome.utils.download_utils.os.path.getsize')
    @mock.patch('sv_pipeline.genome.utils.download_utils.open')
    @mock.patch('sv_pipeline.genome.utils.download_utils.tempfile')
    def test_download_file(self, mock_tempfile, mock_open, mock_getsize, mock_isfile, mock_logger):
        responses.add(responses.HEAD, GZ_DATA_URL,
                      headers={"Content-Length": "1024"}, status=200)
        responses.add(responses.HEAD, TXT_DATA_URL,
                      headers={"Content-Length": "1024"}, status=200)
        responses.add(responses.GET, TXT_DATA_URL, body='test data\nanother line\n')

        # Test bad url
        with self.assertRaises(ValueError) as ve:
            download_file("bad_url")
        self.assertEqual(str(ve.exception), "Invalid url: bad_url")

        # Test already downloaded
        mock_tempfile.reset_mock()
        mock_isfile.return_value = True
        mock_getsize.return_value = 1024
        mock_tempfile.gettempdir.return_value = TEST_DIR
        result = download_file(GZ_DATA_URL)
        self.assertEqual(result, 'test/dir/test_file.gz')
        mock_open.assert_called_with('test/dir/test_file.gz', 'wb')
        mock_isfile.assert_called_with('test/dir/test_file.gz')
        mock_getsize.assert_called_with('test/dir/test_file.gz')
        mock_tempfile.gettempdir.assert_called_once()
        mock_logger.info.assert_called_with(f'Re-using test/dir/test_file.gz previously downloaded from {GZ_DATA_URL}')

        # Test download
        mock_isfile.reset_mock()
        mock_getsize.reset_mock()
        mock_tempfile.reset_mock()
        mock_logger.reset_mock()
        mock_isfile.return_value = False
        mock_getsize.return_value = 0
        result = download_file(TXT_DATA_URL, TEST_DIR)
        self.assertEqual(result, 'test/dir/test_file.txt')
        mock_open.assert_called_with('test/dir/test_file.txt', 'wb')
        mock_open.return_value.writelines.assert_called_once()
        mock_isfile.assert_called_with('test/dir/test_file.txt')
        mock_getsize.assert_not_called()
        mock_tempfile.gettempdir.assert_not_called()
        mock_logger.info.assert_called_with(f'Downloading {TXT_DATA_URL} to test/dir/test_file.txt')
