import unittest
from unittest.mock import patch, MagicMock
from main import InstrumentPriceModifierDB, CalculationEngine, InstrumentDataProcessor


class TestInstrumentDataProcessor(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.spark = MagicMock()

    @classmethod
    def tearDownClass(cls):
        pass

    @patch('main.CalculationEngine.calculate_mean')
    def test_calculate_mean(self, mock_calculate_mean):
        mock_df = MagicMock()
        mock_calculate_mean.return_value = 3.3675917318899224

        result = CalculationEngine.calculate_mean(mock_df, "INSTRUMENT1")
        self.assertEqual(result, 3.3675917318899224)
        mock_calculate_mean.assert_called_once_with(mock_df, "INSTRUMENT1")

    @patch('main.CalculationEngine.calculate_mean_for_november')
    def test_calculate_mean_for_november(self, mock_calculate_mean_for_november):
        mock_df = MagicMock()
        mock_calculate_mean_for_november.return_value = 9.413481179393493

        result = CalculationEngine.calculate_mean_for_november(mock_df, "INSTRUMENT2")
        self.assertEqual(result, 9.413481179393493)
        mock_calculate_mean_for_november.assert_called_once_with(mock_df, "INSTRUMENT2")

    @patch('main.CalculationEngine.calculate_statistic')
    def test_calculate_statistic(self, mock_calculate_statistic):
        mock_df = MagicMock()
        mock_calculate_statistic.return_value = 109.365

        result = CalculationEngine.calculate_statistic(mock_df, "INSTRUMENT3", "median_value")
        self.assertEqual(result, 109.365)
        mock_calculate_statistic.assert_called_once_with(mock_df, "INSTRUMENT3", "median_value")


if __name__ == '__main__':
    unittest.main()
