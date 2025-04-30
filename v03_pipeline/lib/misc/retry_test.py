import unittest
from unittest.mock import Mock, patch

from v03_pipeline.lib.misc.retry import retry


class TestRetryDecorator(unittest.TestCase):
    @patch('time.sleep', return_value=None)
    def test_retry_success_first_try(self, mock_sleep):
        mock_func = Mock(return_value='success')

        @retry(tries=3, delay=1, backoff=2)
        def func():
            return mock_func()

        result = func()
        self.assertEqual(result, 'success')
        self.assertEqual(mock_func.call_count, 1)
        mock_sleep.assert_not_called()

    @patch('time.sleep', return_value=None)
    def test_retry_eventual_success(self, mock_sleep):
        mock_func = Mock(
            side_effect=[Exception('fail'), Exception('fail again'), 'success'],
        )

        @retry(tries=3, delay=1, backoff=2)
        def func():
            return mock_func()

        result = func()
        self.assertEqual(result, 'success')
        self.assertEqual(mock_func.call_count, 3)
        self.assertEqual(mock_sleep.call_count, 2)
        mock_sleep.assert_any_call(1)
        mock_sleep.assert_any_call(2)

    @patch('time.sleep', return_value=None)
    def test_retry_all_failures(self, mock_sleep):
        mock_func = Mock(side_effect=Exception('always fails'))

        @retry(tries=3, delay=1, backoff=2)
        def func():
            return mock_func()

        with self.assertRaises(Exception) as context:
            func()

        self.assertEqual(str(context.exception), 'always fails')
        self.assertEqual(mock_func.call_count, 3)
        self.assertEqual(mock_sleep.call_count, 2)

    @patch('time.sleep', return_value=None)
    @patch('v03_pipeline.lib.misc.retry.logger.info')
    def test_logs_retry_message(self, mock_log, mock_sleep):
        mock_func = Mock(side_effect=[Exception('fail'), 'success'])

        @retry(tries=2, delay=1, backoff=2)
        def func():
            return mock_func()

        func()
        mock_log.assert_called_once()
        self.assertIn('failed with fail', mock_log.call_args[0][0])
