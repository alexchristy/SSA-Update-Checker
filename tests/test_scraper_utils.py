import hashlib
import os
import shutil
import sys
import time
import unittest
import uuid
from unittest.mock import MagicMock, patch
from urllib.parse import quote

import requests

current_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.append(current_dir + "/../")

from scraper_utils import (  # noqa: E402 (Need sys append for proper imports)
    calc_sha256_hash,
    check_env_variables,
    check_local_pdf_dirs,
    ensure_url_encoded,
    extract_relative_path_from_full_path,
    format_pdf_metadata_date,
    gen_pdf_name_uuid,
    get_pdf_name,
    get_with_retry,
    normalize_url,
    timing_decorator,
)


class TestCheckEnvVariables(unittest.TestCase):
    """Test the check_env_variables function."""

    def setUp(self: "TestCheckEnvVariables") -> None:
        """Stage before each test."""
        self.original_env_exists = os.path.exists(".env")
        if self.original_env_exists:
            os.rename(".env", ".env-bak")

    def tearDown(self: "TestCheckEnvVariables") -> None:
        """Cleanup after each test."""
        # Delete the test .env file if it exists
        if os.path.exists(".env"):
            os.remove(".env")

        # Restore the original .env file
        if self.original_env_exists and os.path.exists(".env-bak"):
            os.rename(".env-bak", ".env")

    def create_test_env_file(self: "TestCheckEnvVariables", content: str) -> None:
        """Create a test .env file."""
        with open(".env", "w") as file:
            file.write(content)

    def test_env_variables_success(self: "TestCheckEnvVariables") -> None:
        """Test success case where all environment variables are set."""
        self.create_test_env_file("TEST_VAR=test_value\n")
        result = check_env_variables(["TEST_VAR"])
        self.assertTrue(result)

    def test_env_variables_missing(self: "TestCheckEnvVariables") -> None:
        """Test failure case where a required environment variable is missing."""
        self.create_test_env_file("")  # Empty .env file
        result = check_env_variables(["MISSING_VAR"])
        self.assertFalse(result)

    def test_env_variables_empty(self: "TestCheckEnvVariables") -> None:
        """Test failure case where a required environment variable is empty."""
        self.create_test_env_file("EMPTY_VAR=\n")
        result = check_env_variables(["EMPTY_VAR"])
        self.assertFalse(result)

    def test_multiple_env_variables(self: "TestCheckEnvVariables") -> None:
        """Test with multiple environment variables."""
        self.create_test_env_file("VAR1=value1\nVAR2=value2\n")
        result = check_env_variables(["VAR1", "VAR2"])
        self.assertTrue(result)

    def test_partial_missing_variables(self: "TestCheckEnvVariables") -> None:
        """Test case where some, but not all, required environment variables are missing."""
        self.create_test_env_file("VAR1=value1\n")
        result = check_env_variables(["VAR1", "MISSING_VAR2"])
        self.assertFalse(result)


def mock_function(x: int) -> int:
    """Mock function that takes 0.1 seconds to run."""
    time.sleep(0.1)
    return x * 2


@timing_decorator
def decorated_mock_function(x: int) -> int:
    """Mock function that takes 0.1 seconds to run."""
    return mock_function(x)


class TestError(Exception):
    """A custom exception for testing purposes."""

    pass


class TestTimingDecorator(unittest.TestCase):
    """Test the timing_decorator function."""

    @patch("scraper_utils.logging.info")
    def test_decorated_function_timing(
        self: "TestTimingDecorator", mock_log_info: MagicMock
    ) -> None:
        """Test if the decorator correctly logs the execution time."""
        result = decorated_mock_function(5)
        self.assertEqual(result, 10)

        # Check if logging.info was called at least once
        self.assertTrue(mock_log_info.called)

        # Extract the arguments passed to logging.info
        log_message_format, func_name, elapsed_time = mock_log_info.call_args[0]

        # Assert that the function name is correctly logged
        self.assertEqual(func_name, "decorated_mock_function")

        # Assert that the log message format is correct
        self.assertIn("%s took %d seconds to run", log_message_format)

        # Assert that the elapsed time is a float (since exact time measurement can vary)
        self.assertIsInstance(elapsed_time, float)

    @patch("scraper_utils.logging.error")
    def test_decorated_function_exception(
        self: "TestTimingDecorator", mock_log_error: MagicMock
    ) -> None:
        """Test if the decorator correctly logs an exception."""
        with self.assertRaises(TestError):

            @timing_decorator
            def fail_function() -> None:
                msg = "Test Exception"
                raise TestError(msg)

            fail_function()

        # Check if logging.error was called at least once
        self.assertTrue(mock_log_error.called)

        # Extract the arguments passed to logging.error
        error_message_format, func_name, exception = mock_log_error.call_args[0]

        # Assert that the function name is correctly logged
        self.assertEqual(func_name, "fail_function")

        # Assert the type and message of the exception
        self.assertIsInstance(exception, TestError)
        self.assertEqual(str(exception), "Test Exception")


class TestCheckLocalPdfDirs(unittest.TestCase):
    """Test the check_local_pdf_dirs function."""

    def setUp(self: "TestCheckLocalPdfDirs") -> None:
        """Set up a temporary PDF directory for testing."""
        self.original_pdf_dir = os.getenv("PDF_DIR")
        self.test_pdf_dir = "test_pdf_dir/"
        os.environ["PDF_DIR"] = self.test_pdf_dir

        # Create the test PDF directory if it does not exist
        if not os.path.exists(self.test_pdf_dir):
            os.makedirs(self.test_pdf_dir)

    def tearDown(self: "TestCheckLocalPdfDirs") -> None:
        """Clean up after tests."""
        # Remove the test PDF directory and its contents
        if os.path.exists(self.test_pdf_dir):
            shutil.rmtree(self.test_pdf_dir)

        # Restore the original PDF_DIR environment variable
        if self.original_pdf_dir is not None:
            os.environ["PDF_DIR"] = self.original_pdf_dir
        else:
            del os.environ["PDF_DIR"]

    def test_pdf_dirs_created_successfully(self: "TestCheckLocalPdfDirs") -> None:
        """Test that the function creates the necessary directories."""
        result = check_local_pdf_dirs()
        self.assertTrue(result)

        # Check if the directories were created
        for use_dir in ["tmp/", "current/", "archive/"]:
            for type_dir in ["72_HR/", "30_DAY/", "ROLLCALL"]:
                dir_path = os.path.join(self.test_pdf_dir, use_dir, type_dir)
                self.assertTrue(os.path.exists(dir_path))

    @patch("scraper_utils.logging.error")
    def test_pdf_dir_env_var_not_set(
        self: "TestCheckLocalPdfDirs", mock_log_error: MagicMock
    ) -> None:
        """Test that the function fails when the PDF_DIR environment variable is not set."""
        del os.environ["PDF_DIR"]  # Temporarily unset the PDF_DIR environment variable

        result = check_local_pdf_dirs()
        self.assertFalse(result)

        # Check if the appropriate error message was logged
        mock_log_error.assert_called_once()
        args, kwargs = mock_log_error.call_args
        self.assertIn("PDF_DIR environment variable is not set.", args[0])

        # Reset the PDF_DIR environment variable for other tests
        os.environ["PDF_DIR"] = self.test_pdf_dir


class TestEnsureUrlEncoded(unittest.TestCase):
    """Test the ensure_url_encoded function."""

    def test_url_already_encoded(self: "TestEnsureUrlEncoded") -> None:
        """Test that the function returns the URL unchanged if it's already encoded."""
        encoded_url = "https%3A%2F%2Fwww.example.com%2Fpath%3Fquery%3Dtest"
        result = ensure_url_encoded(encoded_url)
        self.assertEqual(result, encoded_url)

    def test_url_not_encoded(self: "TestEnsureUrlEncoded") -> None:
        """Test that the function correctly encodes a URL that is not already encoded."""
        unencoded_url = "https://www.example.com/path?query=test"
        expected_encoded_url = quote(unencoded_url, safe=":/.-")
        result = ensure_url_encoded(unencoded_url)
        self.assertEqual(result, expected_encoded_url)

    def test_empty_url(self: "TestEnsureUrlEncoded") -> None:
        """Test that the function handles an empty string correctly."""
        empty_url = ""
        result = ensure_url_encoded(empty_url)
        self.assertEqual(result, empty_url)

    def test_url_with_safe_characters(self: "TestEnsureUrlEncoded") -> None:
        """Test that the function does not encode a URL with only safe characters."""
        safe_url = "https://www.example.com/"
        result = ensure_url_encoded(safe_url)
        self.assertEqual(result, safe_url)


class TestExtractRelativePathFromFullPath(unittest.TestCase):
    """Test the extract_relative_path_from_full_path function."""

    def test_base_segment_present(self: "TestExtractRelativePathFromFullPath") -> None:
        """Test that the function returns the correct relative path when the base segment is present."""
        full_path = "/home/user/documents/archive/2023/report.pdf"
        base_segment = "archive"
        expected_relative_path = "archive/2023/report.pdf"
        result = extract_relative_path_from_full_path(base_segment, full_path)
        self.assertEqual(result, expected_relative_path)

    def test_base_segment_not_present(
        self: "TestExtractRelativePathFromFullPath",
    ) -> None:
        """Test that the function returns None when the base segment is not present in the full path."""
        full_path = "/home/user/documents/reports/2023/report.pdf"
        base_segment = "archive"
        result = extract_relative_path_from_full_path(base_segment, full_path)
        self.assertIsNone(result)

    def test_empty_base_segment(self: "TestExtractRelativePathFromFullPath") -> None:
        """Test that the function handles an empty base segment correctly."""
        full_path = "/home/user/documents/archive/2023/report.pdf"
        base_segment = ""
        result = extract_relative_path_from_full_path(base_segment, full_path)
        self.assertIsNone(result)

    def test_empty_full_path(self: "TestExtractRelativePathFromFullPath") -> None:
        """Test that the function handles an empty full path correctly."""
        full_path = ""
        base_segment = "archive"
        result = extract_relative_path_from_full_path(base_segment, full_path)
        self.assertIsNone(result)


class TestGetWithRetry(unittest.TestCase):
    """Test the get_with_retry function."""

    @patch("scraper_utils.requests.get")
    @patch("scraper_utils.logging.debug")
    def test_successful_get_request(
        self: "TestGetWithRetry",
        mock_logging_debug: MagicMock,
        mock_requests_get: MagicMock,
    ) -> None:
        """Test that the function returns the response on a successful GET request."""
        mock_response = requests.Response()  # Create a mock response object
        mock_response.status_code = 200
        mock_requests_get.return_value = mock_response

        result = get_with_retry("https://www.example.com")
        self.assertEqual(result, mock_response)
        mock_requests_get.assert_called_once()

    @patch("scraper_utils.requests.get")
    @patch("scraper_utils.logging.error")
    def test_get_request_timeout(
        self: "TestGetWithRetry",
        mock_logging_error: MagicMock,
        mock_requests_get: MagicMock,
    ) -> None:
        """Test that the function retries after a timeout."""
        mock_requests_get.side_effect = requests.Timeout

        result = get_with_retry("https://www.example.com")
        self.assertIsNone(result)
        self.assertEqual(mock_requests_get.call_count, 3)  # 3 attempts
        mock_logging_error.assert_called_with(
            "Request to %s timed out.", "https://www.example.com"
        )

    @patch("scraper_utils.requests.get")
    @patch("scraper_utils.logging.error")
    def test_get_request_generic_exception(
        self: "TestGetWithRetry",
        mock_logging_error: MagicMock,
        mock_requests_get: MagicMock,
    ) -> None:
        """Test that the function handles a generic exception."""
        mock_requests_get.side_effect = Exception("Generic error")

        result = get_with_retry("https://www.example.com")
        self.assertIsNone(result)
        self.assertEqual(mock_requests_get.call_count, 3)  # 3 attempts
        mock_logging_error.assert_called_with(
            "Request to %s failed in get_with_retry().", "https://www.example.com"
        )

    @patch("scraper_utils.requests.get")
    @patch("scraper_utils.logging.error")
    def test_invalid_url(
        self: "TestGetWithRetry",
        mock_logging_error: MagicMock,
        mock_requests_get: MagicMock,
    ) -> None:
        """Test that the function handles an invalid URL."""
        invalid_url = "https://invalid-url"
        mock_requests_get.side_effect = requests.RequestException

        result = get_with_retry(invalid_url)
        self.assertIsNone(result)
        self.assertEqual(mock_requests_get.call_count, 3)  # 3 attempts
        mock_logging_error.assert_called_with(
            "Request to %s failed in get_with_retry().", invalid_url
        )

    @patch("scraper_utils.requests.get")
    def test_http_error_response(
        self: "TestGetWithRetry", mock_requests_get: MagicMock
    ) -> None:
        """Test that the function handles an HTTP error response."""
        mock_response = requests.Response()  # Create a mock response object
        mock_response.status_code = 404  # HTTP Not Found
        mock_requests_get.return_value = mock_response

        result = get_with_retry("https://www.example.com")
        self.assertEqual(result, mock_response)

    @patch("scraper_utils.requests.get")
    @patch("scraper_utils.time.sleep")
    def test_delay_escalation(
        self: "TestGetWithRetry", mock_sleep: MagicMock, mock_requests_get: MagicMock
    ) -> None:
        """Test if the delay between retries escalates correctly."""
        mock_requests_get.side_effect = requests.Timeout

        get_with_retry("https://www.example.com")
        mock_sleep.assert_has_calls([unittest.mock.call(2), unittest.mock.call(4)])


class TestCalcSha256Hash(unittest.TestCase):
    """Test the calc_sha256_hash function."""

    def test_hash_of_empty_string(self: "TestCalcSha256Hash") -> None:
        """Test that the function correctly calculates the hash of an empty string."""
        expected_hash = hashlib.sha256("".encode("utf-8")).hexdigest()
        result = calc_sha256_hash("")
        self.assertEqual(result, expected_hash)

    def test_hash_of_regular_string(self: "TestCalcSha256Hash") -> None:
        """Test that the function correctly calculates the hash of a regular string."""
        input_string = "Hello, world!"
        expected_hash = hashlib.sha256(input_string.encode("utf-8")).hexdigest()
        result = calc_sha256_hash(input_string)
        self.assertEqual(result, expected_hash)

    def test_hash_of_unicode_string(self: "TestCalcSha256Hash") -> None:
        """Test that the function correctly calculates the hash of a unicode string."""
        unicode_string = "こんにちは世界"  # 'Hello, world' in Japanese
        expected_hash = hashlib.sha256(unicode_string.encode("utf-8")).hexdigest()
        result = calc_sha256_hash(unicode_string)
        self.assertEqual(result, expected_hash)

    def test_hash_is_consistent_for_same_input(self: "TestCalcSha256Hash") -> None:
        """Test that the function returns consistent hashes for the same input."""
        input_string = "consistent input"
        hash1 = calc_sha256_hash(input_string)
        hash2 = calc_sha256_hash(input_string)
        self.assertEqual(hash1, hash2)


class TestNormalizeUrl(unittest.TestCase):
    """Test the normalize_url function."""

    @patch("scraper_utils.logging.debug")
    def test_normalization_of_full_url(
        self: "TestNormalizeUrl", mock_logging_debug: MagicMock
    ) -> None:
        """Test that the function normalizes a URL with path and query string."""
        url = "http://www.example.com/path?query=string"
        expected_normalized_url = "https://www.example.com/"
        result = normalize_url(url)
        self.assertEqual(result, expected_normalized_url)

    @patch("scraper_utils.logging.debug")
    def test_normalization_of_https_url(
        self: "TestNormalizeUrl", mock_logging_debug: MagicMock
    ) -> None:
        """Test that the function normalizes an HTTPS URL."""
        url = "https://www.example.com"
        expected_normalized_url = "https://www.example.com/"
        result = normalize_url(url)
        self.assertEqual(result, expected_normalized_url)

    @patch("scraper_utils.logging.debug")
    def test_normalization_of_url_without_scheme(
        self: "TestNormalizeUrl", mock_logging_debug: MagicMock
    ) -> None:
        """Test that the function normalizes a URL without a scheme."""
        url = "www.example.com/path"
        expected_normalized_url = "https://www.example.com/"
        result = normalize_url(url)
        self.assertEqual(result, expected_normalized_url)

    @patch("scraper_utils.logging.debug")
    def test_normalization_of_empty_url(
        self: "TestNormalizeUrl", mock_logging_debug: MagicMock
    ) -> None:
        """Test that the function returns an empty string for an empty URL."""
        url = ""
        expected_normalized_url = ""
        result = normalize_url(url)
        self.assertEqual(result, expected_normalized_url)

    @patch("scraper_utils.logging.debug")
    def test_normalization_of_malformed_url(
        self: "TestNormalizeUrl", mock_logging_debug: MagicMock
    ) -> None:
        """Test that the function returns an empty string for a malformed URL."""
        url = "http:/malformed.url"
        expected_normalized_url = ""
        result = normalize_url(url)
        self.assertEqual(result, expected_normalized_url)


class TestGetPdfName(unittest.TestCase):
    """Test the get_pdf_name function."""

    def test_pdf_in_url(self: "TestGetPdfName") -> None:
        """Test that the function returns the PDF name when a PDF is in the URL."""
        url = "http://www.example.com/documents/report.pdf"
        expected_pdf_name = "report.pdf"
        result = get_pdf_name(url)
        self.assertEqual(result, expected_pdf_name)

    def test_no_pdf_in_url(self: "TestGetPdfName") -> None:
        """Test that the function returns an empty string when no PDF is in the URL."""
        url = "http://www.example.com/documents/report"
        result = get_pdf_name(url)
        self.assertEqual(result, "")

    def test_pdf_in_url_with_query_string(self: "TestGetPdfName") -> None:
        """Test that the function returns the PDF name when the URL has a query string."""
        url = "http://www.example.com/documents/report.pdf?query=123"
        expected_pdf_name = "report.pdf"
        result = get_pdf_name(url)
        self.assertEqual(result, expected_pdf_name)

    def test_url_with_path_but_no_pdf(self: "TestGetPdfName") -> None:
        """Test that the function returns an empty string when the path does not end with .pdf."""
        url = "http://www.example.com/documents/report.txt"
        result = get_pdf_name(url)
        self.assertEqual(result, "")

    def test_empty_url(self: "TestGetPdfName") -> None:
        """Test that the function returns an empty string for an empty URL."""
        url = ""
        result = get_pdf_name(url)
        self.assertEqual(result, "")


class TestGenPdfNameUuid(unittest.TestCase):
    """Test the gen_pdf_name_uuid function."""

    @patch("scraper_utils.uuid.uuid4")
    def test_pdf_name_generation(
        self: "TestGenPdfNameUuid", mock_uuid4: MagicMock
    ) -> None:
        """Test that the function generates a new name for a PDF file."""
        test_uuid = uuid.UUID("1234567890abcdef1234567890abcdef")
        mock_uuid4.return_value = test_uuid
        file_path = "/path/to/document.pdf"
        expected_file_name = f"document_{test_uuid!s}.pdf"
        result = gen_pdf_name_uuid(file_path)
        self.assertEqual(result, expected_file_name)

    @patch("scraper_utils.logging.error")
    def test_non_pdf_file(
        self: "TestGenPdfNameUuid", mock_logging_error: MagicMock
    ) -> None:
        """Test that the function returns an empty string for non-PDF files."""
        file_path = "/path/to/document.txt"
        result = gen_pdf_name_uuid(file_path)
        self.assertEqual(result, "")
        mock_logging_error.assert_called_with("%s is not a PDF!", file_path)

    def test_empty_file_path(self: "TestGenPdfNameUuid") -> None:
        """Test that the function returns an empty string for an empty file path."""
        file_path = ""
        result = gen_pdf_name_uuid(file_path)
        self.assertEqual(result, "")

    def test_file_path_without_extension(self: "TestGenPdfNameUuid") -> None:
        """Test that the function returns an empty string for a file path without an extension."""
        file_path = "/path/to/document"
        result = gen_pdf_name_uuid(file_path)
        self.assertEqual(result, "")


class TestFormatPdfMetadataDate(unittest.TestCase):
    """Test the format_pdf_metadata_date function."""

    def test_valid_date_string(self: "TestFormatPdfMetadataDate") -> None:
        """Test that the function correctly formats a valid date string."""
        date_str = "D:20210315123456"
        expected_formatted_date = "20210315123456"
        result = format_pdf_metadata_date(date_str)
        self.assertEqual(result, expected_formatted_date)

    def test_invalid_date_string(self: "TestFormatPdfMetadataDate") -> None:
        """Test that the function returns None for an invalid date string."""
        date_str = "D:20210315"  # Missing time components
        result = format_pdf_metadata_date(date_str)
        self.assertIsNone(result)

    def test_none_date_string(self: "TestFormatPdfMetadataDate") -> None:
        """Test that the function returns None when the input is None."""
        date_str = None
        result = format_pdf_metadata_date(date_str)  # type: ignore
        self.assertIsNone(result)

    def test_empty_date_string(self: "TestFormatPdfMetadataDate") -> None:
        """Test that the function returns None for an empty date string."""
        date_str = ""
        result = format_pdf_metadata_date(date_str)
        self.assertIsNone(result)

    def test_malformed_date_string(self: "TestFormatPdfMetadataDate") -> None:
        """Test that the function returns None for a malformed date string."""
        date_str = "D:2021-03-15T12:34:56"  # Incorrect format
        result = format_pdf_metadata_date(date_str)
        self.assertIsNone(result)


if __name__ == "__main__":
    unittest.main()
