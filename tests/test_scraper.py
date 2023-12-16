import json
import os
import pickle
import sys
import unittest
from unittest.mock import MagicMock, patch

current_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.append(current_dir + "/../")

from scraper import (  # noqa: E402 (Need to import after adding to path)
    get_active_terminals,
)


class TestGetActiveTerminals(unittest.TestCase):
    """Test the get_active_terminals function."""

    def setUp(self: "TestGetActiveTerminals") -> None:
        """Set up the test cases for TestGetActiveTerminals."""
        file_path = os.path.join(
            current_dir, "TestGetActiveTerminals_Assets/AMC_Home_Page_12-16-23.pkl"
        )

        # Load the serialized response
        with open(
            file_path,
            "rb",
        ) as file:
            self.serialized_response = file.read()

    @patch("scraper.scraper_utils.get_with_retry")
    def test_active_terminals(
        self: "TestGetActiveTerminals", mock_get_with_retry: MagicMock
    ) -> None:
        """Test that the function returns a list of active terminals."""
        # Deserialize the response
        response_data = pickle.loads(  # noqa: S301 (Loading test data)
            self.serialized_response
        )

        mock_response = unittest.mock.Mock()
        mock_response.configure_mock(**response_data)

        mock_get_with_retry.return_value = mock_response

        result = get_active_terminals("https://www.amc.af.mil/AMC-Travel-Site/")

        self.assertIsInstance(result, list)
        self.assertEqual(len(result), 40)

        array_path = os.path.join(
            current_dir, "TestGetActiveTerminals_Assets/terminal_names.json"
        )
        # Load the expected terminal_names array
        with open(array_path, "r") as file:
            terminal_names = json.load(file)

        parsed_terminal_names = [terminal.name for terminal in result]

        self.assertListEqual(parsed_terminal_names, terminal_names)

    @patch("scraper.scraper_utils.get_with_retry")
    def test_active_terminals_none_response(
        self: "TestGetActiveTerminals", mock_get_with_retry: MagicMock
    ) -> None:
        """Test that the function returns an empty list when the response is None."""
        mock_response = unittest.mock.Mock()
        mock_response.configure_mock(**{"content": None})

        mock_get_with_retry.return_value = mock_response

        with self.assertRaises(SystemExit):
            get_active_terminals("https://www.amc.af.mil/AMC-Travel-Site/")

    @patch("scraper.scraper_utils.get_with_retry")
    def test_active_terminals_empty_response(
        self: "TestGetActiveTerminals", mock_get_with_retry: MagicMock
    ) -> None:
        """Test that the function returns an empty list when the response is None."""
        mock_response = unittest.mock.Mock()
        mock_response.configure_mock(**{"content": ""})

        mock_get_with_retry.return_value = mock_response

        with self.assertRaises(SystemExit):
            get_active_terminals("https://www.amc.af.mil/AMC-Travel-Site/")


if __name__ == "__main__":
    unittest.main()
