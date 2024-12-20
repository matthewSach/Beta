import unittest
import pandas as pd
from unittest.mock import patch, MagicMock
from io import StringIO
import os
import time


class TestFileLoader(unittest.TestCase):

    @patch('os.path.isfile')
    @patch('pandas.read_csv')
    def test_load_file_success(self, mock_read_csv, mock_isfile):
        # Setup mocks
        mock_isfile.return_value = True
        mock_read_csv.return_value = pd.DataFrame({
            'Country': ['USA', 'Canada'],
            'Total Profit': [1000, 1500],
            'Order Date': ['2021-06-15', '2021-06-16']
        })

        loader = FileLoader()
        data = loader.load_file('test.csv')

        # Test results
        self.assertIsNotNone(data)
        self.assertEqual(data.shape[0], 2)  # Two rows in test data
        self.assertIn('Country', data.columns)
        self.assertIn('Total Profit', data.columns)

    @patch('os.path.isfile')
    def test_load_file_file_not_found(self, mock_isfile):
        # Mock the file check to simulate file not found
        mock_isfile.return_value = False
        loader = FileLoader()
        result = loader.load_file('non_existent_file.csv')

        # Test if None is returned on error
        self.assertIsNone(result)

    @patch('os.path.isfile')
    @patch('pandas.read_csv')
    def test_load_file_missing_column(self, mock_read_csv, mock_isfile):
        # Setup mocks
        mock_isfile.return_value = True
        mock_read_csv.return_value = pd.DataFrame({
            'Country': ['USA'],
            'Order Date': ['2021-06-15']
        })

        loader = FileLoader()
        result = loader.load_file('test.csv')

        # Test if MissingColumnError is raised
        self.assertIsNone(result)


class TestDataProcessor(unittest.TestCase):

    def test_format_currency_valid(self):
        processor = DataProcessor()
        result = processor.format_currency(1000)
        self.assertEqual(result, "1,000.00 USD")

    def test_format_currency_invalid(self):
        processor = DataProcessor()
        with self.assertRaises(Exception):
            processor.format_currency("invalid")

    def test_calculate_profit(self):
        data = pd.DataFrame({
            'Country': ['USA', 'USA', 'Canada'],
            'Total Profit': [1000, 2000, 1500],
            'Order Date': ['2021-06-15', '2021-06-16', '2021-06-16']
        })
        processor = DataProcessor()
        result = processor.calculate_profit(data, 'USA')
        self.assertEqual(result, 3000)

    def test_calculate_profit_no_data(self):
        data = pd.DataFrame({
            'Country': ['Canada'],
            'Total Profit': [1500],
            'Order Date': ['2021-06-16']
        })
        processor = DataProcessor()
        with self.assertRaises(ValueError):
            processor.calculate_profit(data, 'USA')

    def test_analyze_trends_by_year(self):
        data = pd.DataFrame({
            'Country': ['USA', 'USA', 'Canada'],
            'Total Profit': [1000, 2000, 1500],
            'Order Date': ['2021-06-15', '2021-06-16', '2021-06-16']
        })
        processor = DataProcessor()
        trends = processor.analyze_trends_by_year(data)
        self.assertEqual(trends, {2021: 4500})

    def test_validate_and_clean_data(self):
        data = pd.DataFrame({
            'Country': ['USA', None, 'Canada'],
            'Total Profit': [1000, 2000, None],
            'Order Date': ['2021-06-15', '2021-06-16', '2021-06-16'],
            'Units Sold': [1, 2, 0],
            'Unit Price': [100, 200, 150],
            'Unit Cost': [50, 100, 120],
            'Total Revenue': [500, 400, 300]
        })
        processor = DataProcessor()
        cleaned_data, invalid_count = processor.validate_and_clean_data(data)

        # Test if the invalid rows were removed
        self.assertEqual(invalid_count, 1)  # One row with missing data
        self.assertEqual(cleaned_data.shape[0], 2)

    def test_calculate_average_profit_per_order(self):
        data = pd.DataFrame({
            'Country': ['USA', 'USA', 'Canada'],
            'Total Profit': [1000, 2000, 1500],
            'Order Date': ['2021-06-15', '2021-06-16', '2021-06-16']
        })
        processor = DataProcessor()
        avg_profit = processor.calculate_average_profit_per_order(data)
        self.assertEqual(avg_profit, 1500)

    @patch('multiprocessing.Pool.starmap')
    def test_compare_profits(self, mock_starmap):
        data = pd.DataFrame({
            'Country': ['USA', 'Canada'],
            'Total Profit': [1000, 1500],
            'Order Date': ['2021-06-15', '2021-06-16']
        })
        processor = DataProcessor()

        # Mock the result of starmap
        mock_starmap.return_value = [1000, 2000]

        with patch('builtins.print') as mocked_print:
            processor.compare_profits(data, 'USA', 'Canada')
            mocked_print.assert_any_call("Total profit for USA: 1,000.00 USD")
            mocked_print.assert_any_call("Total profit for Canada: 2,000.00 USD")
            mocked_print.assert_any_call("Canada has a higher profit by 1,000.00 USD.")

    @patch('multiprocessing.Pool.starmap')
    def test_analyze_trends(self, mock_starmap):
        data = pd.DataFrame({
            'Country': ['USA', 'USA', 'Canada'],
            'Total Profit': [1000, 2000, 1500],
            'Order Date': ['2021-06-15', '2021-06-16', '2021-06-16'],
            'Item Type': ['A', 'B', 'A']
        })
        processor = DataProcessor()

        # Mock the result of starmap
        mock_starmap.return_value = [data]

        with patch('builtins.print') as mocked_print:
            processor.analyze_trends(data, 'USA')
            mocked_print.assert_any_call("Analyzing trends by year for USA...")


if __name__ == '__main__':
    unittest.main()
