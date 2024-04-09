import unittest
from unittest.mock import patch, MagicMock
from datetime import datetime, timedelta
from ahd_data_pipelines.integrations.http_processors.fema_http_processor import FEMAHTTPProcessor

class TestFEMAHTTPProcessor(unittest.TestCase):

    @patch('ahd_data_pipelines.integrations.http_processors.http_processor.HTTPProcessor.get_http_response')
    def test_process_pagination(self, mock_get_http_response):
        # Mocking HTTP response
        mock_response = MagicMock()
        mock_response.json.return_value = {
            'metadata': {'entityname': 'records'},
            'records': [{'id': 1}, {'id': 2}]
        }
        mock_get_http_response.return_value = mock_response

        # Constructing test parameters
        params = {
            'endpoint': 'http://example.com/data',
            'query_parameters': 'param1={today:%Y-%m-%d}&param2={yesterday:%Y-%m-%d}',
            'paginate': {'query_parameter': 'page', 'page_size': 10}
        }

        # Initializing the processor and executing the process method
        processor = FEMAHTTPProcessor()
        result = processor.process(params)

        # Assertions
        mock_get_http_response.assert_called()
        self.assertEqual(len(result), 2)

    @patch('ahd_data_pipelines.integrations.http_processors.http_processor.HTTPProcessor.get_http_response')
    def test_process_no_pagination(self, mock_get_http_response):
        # Mocking HTTP response
        mock_response = MagicMock()
        mock_response.json.return_value = {
            'metadata': {'entityname': 'records'},
            'records': [{'id': 1}, {'id': 2}]
        }
        mock_get_http_response.return_value = mock_response

        # Constructing test parameters
        params = {
            'endpoint': 'http://example.com/data',
            'query_parameters': 'param1={today:%Y-%m-%d}&param2={yesterday:%Y-%m-%d}'
        }

        # Initializing the processor and executing the process method
        processor = FEMAHTTPProcessor()

        # Assertions
        with self.assertRaises(NotImplementedError):
            processor.process(params)

        mock_get_http_response.assert_not_called()

    def test_construct_query_params(self):
        # Constructing test parameters
        query_parameters = 'param1={today:%Y-%m-%d}&param2={yesterday:%Y-%m-%d}'
        processor = FEMAHTTPProcessor()

        # Executing the method to test
        constructed_params = processor.construct_query_params(query_parameters)

        # Assertions
        today = datetime.today().strftime('%Y-%m-%d')
        yesterday = (datetime.today() - timedelta(days=1)).strftime('%Y-%m-%d')

        self.assertIn(f'param1={today}', constructed_params)
        self.assertIn(f'param2={yesterday}', constructed_params)


if __name__ == '__main__':
    unittest.main()
