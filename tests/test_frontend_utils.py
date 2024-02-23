import unittest
from unittest.mock import patch, MagicMock
from frontend_utils import _get_terminal_image_url
import pickle
import os
from firestoredb import FirestoreClient
from s3_bucket import S3Bucket


class TestGetTerminalImageUrl(unittest.TestCase):
    """Test that we can find the URL for a terminal's image."""

    terminal_coll: str
    pdf_archive_coll: str
    lock_coll: str
    firestore_cert: str
    fs: FirestoreClient
    s3: S3Bucket
    bwi_page: dict
    dover_page_no_pdfs: dict
    andrews_page_no_pdfs: dict
    seattle_page_no_pdfs: dict
    charleston_page_no_pdfs: dict

    @classmethod
    def setUpClass(cls) -> None:
        """Set up the test cases for TestGetTerminalImageUrl."""
        # Create a FirestoreClient object
        # Set collection names
        cls.terminal_coll = "**TestGetTerminalImageUrl**_Terminals"
        cls.pdf_archive_coll = "**TestGetTerminalImageUrl**_PDF_Archive"
        cls.lock_coll = "**TestGetTerminalImageUrl**_Locks"
        cls.firestore_cert = "./creds.json"

        os.environ["TERMINAL_COLL"] = cls.terminal_coll
        os.environ["PDF_ARCHIVE_COLL"] = cls.pdf_archive_coll
        os.environ["LOCK_COLL"] = cls.lock_coll
        os.environ["FS_CRED_PATH"] = cls.firestore_cert

        cls.fs = FirestoreClient()
        cls.s3 = S3Bucket()

        # Load the serialized response
        with open(
            "tests/assets/TestGetTerminalImageUrl/bwi_page_02-17-2024.pkl",
            "rb",
        ) as file:
            cls.bwi_page = pickle.load(file)  # noqa: S301 (Loading test data)

        with open(
            "tests/assets/TestGetTerminalImageUrl/dover_page_02-17-24_NO_PDFS.pkl",
            "rb",
        ) as file:
            cls.dover_page_no_pdfs = pickle.load(file)  # noqa: S301 (Loading test data)

        with open(
            "tests/assets/TestGetTerminalImageUrl/andrews_page_02-17-24_NO_PDFS.pkl",
            "rb",
        ) as file:
            cls.andrews_page_no_pdfs = pickle.load(  # noqa: S301 (Loading test data)
                file
            )

        with open(
            "tests/assets/TestGetTerminalImageUrl/seattle_page_02-17-24_NO_PDFS.pkl",
            "rb",
        ) as file:
            cls.seattle_page_no_pdfs = pickle.load(  # noqa: S301 (Loading test data)
                file
            )

        with open(
            "tests/assets/TestGetTerminalImageUrl/charleston_page_02-17-24_NO_PDFS.pkl",
            "rb",
        ) as file:
            cls.charleston_page_no_pdfs = pickle.load(
                file
            )  # noqa: S301 (Loading test data)

    @patch("frontend_utils.get_with_retry")
    def test_get_terminal_image_url_bwi(
        self: "TestGetTerminalImageUrl", mock_get_with_retry: MagicMock
    ) -> None:
        """Test that we can find the URL for a terminal's image."""
        # Prepare the mock response object
        mock_response = MagicMock()
        mock_response.status_code = self.bwi_page["status_code"]
        mock_response.headers = self.bwi_page["headers"]
        # Assuming the content is pickled as bytes, decode to simulate response.text
        mock_response.text = self.bwi_page["content"].decode("utf-8")

        # Set mock object to return the mock response
        mock_get_with_retry.return_value = mock_response

        # Actual URL used is not critical due to mocking; adjust as needed
        url = "https://example.com"
        expected_image_url = "https://media.defense.gov/2022/Jan/11/2002919982/-1/-1/0/200122-F-F3200-002.JPG"  # Adjust based on your expectations

        # Call the function under test
        found_image_url = _get_terminal_image_url(url)

        # Verify the function returns the expected URL
        self.assertEqual(found_image_url, expected_image_url)

        # Additional assertions can be made here, for example, checking call count
        mock_get_with_retry.assert_called_once_with(url)

    @patch("frontend_utils.get_with_retry")
    def test_get_terminal_image_url_dover(
        self: "TestGetTerminalImageUrl", mock_get_with_retry: MagicMock
    ) -> None:
        """Test that we can find the URL for a terminal's image."""
        # Prepare the mock response object
        mock_response = MagicMock()
        mock_response.status_code = self.dover_page_no_pdfs["status_code"]
        mock_response.headers = self.dover_page_no_pdfs["headers"]
        # Assuming the content is pickled as bytes, decode to simulate response.text
        mock_response.text = self.dover_page_no_pdfs["content"].decode("utf-8")

        # Set mock object to return the mock response
        mock_get_with_retry.return_value = mock_response

        # Actual URL used is not critical due to mocking; adjust as needed
        url = "https://example.com"
        expected_image_url = "https://media.defense.gov/2022/Mar/07/2002951829/-1/-1/0/070322-F-F3200-001.JPG"  # Adjust based on your expectations

        # Call the function under test
        found_image_url = _get_terminal_image_url(url)

        # Verify the function returns the expected URL
        self.assertEqual(found_image_url, expected_image_url)

        # Additional assertions can be made here, for example, checking call count
        mock_get_with_retry.assert_called_once_with(url)

    @patch("frontend_utils.get_with_retry")
    def test_get_terminal_image_url_andrews(
        self: "TestGetTerminalImageUrl", mock_get_with_retry: MagicMock
    ) -> None:
        """Test that we can find the URL for a terminal's image."""
        # Prepare the mock response object
        mock_response = MagicMock()
        mock_response.status_code = self.andrews_page_no_pdfs["status_code"]
        mock_response.headers = self.andrews_page_no_pdfs["headers"]
        # Assuming the content is pickled as bytes, decode to simulate response.text
        mock_response.text = self.andrews_page_no_pdfs["content"].decode("utf-8")

        # Set mock object to return the mock response
        mock_get_with_retry.return_value = mock_response

        # Actual URL used is not critical due to mocking; adjust as needed
        url = "https://example.com"
        expected_image_url = "https://media.defense.gov/2022/Feb/02/2002931758/-1/-1/0/020222-F-F3200-002.JPG"  # Adjust based on your expectations

        # Call the function under test
        found_image_url = _get_terminal_image_url(url)

        # Verify the function returns the expected URL
        self.assertEqual(found_image_url, expected_image_url)

        # Additional assertions can be made here, for example, checking call count
        mock_get_with_retry.assert_called_once_with(url)

    @patch("frontend_utils.get_with_retry")
    def test_get_terminal_image_url_seattle(
        self: "TestGetTerminalImageUrl", mock_get_with_retry: MagicMock
    ) -> None:
        """Test that we can find the URL for a terminal's image."""
        # Prepare the mock response object
        mock_response = MagicMock()
        mock_response.status_code = self.seattle_page_no_pdfs["status_code"]
        mock_response.headers = self.seattle_page_no_pdfs["headers"]
        # Assuming the content is pickled as bytes, decode to simulate response.text
        mock_response.text = self.seattle_page_no_pdfs["content"].decode("utf-8")

        # Set mock object to return the mock response
        mock_get_with_retry.return_value = mock_response

        # Actual URL used is not critical due to mocking; adjust as needed
        url = "https://example.com"
        expected_image_url = "https://media.defense.gov/2023/Aug/29/2003290861/-1/-1/0/230829-F-DJ189-1001.JPG"  # Adjust based on your expectations

        # Call the function under test
        found_image_url = _get_terminal_image_url(url)

        # Verify the function returns the expected URL
        self.assertEqual(found_image_url, expected_image_url)

        # Additional assertions can be made here, for example, checking call count
        mock_get_with_retry.assert_called_once_with(url)

    @patch("frontend_utils.get_with_retry")
    def test_get_terminal_image_url_charleston(
        self: "TestGetTerminalImageUrl", mock_get_with_retry: MagicMock
    ) -> None:
        """Test that we can find the URL for a terminal's image."""
        # Prepare the mock response object
        mock_response = MagicMock()
        mock_response.status_code = self.charleston_page_no_pdfs["status_code"]
        mock_response.headers = self.charleston_page_no_pdfs["headers"]
        # Assuming the content is pickled as bytes, decode to simulate response.text
        mock_response.text = self.charleston_page_no_pdfs["content"].decode("utf-8")

        # Set mock object to return the mock response
        mock_get_with_retry.return_value = mock_response

        # Actual URL used is not critical due to mocking; adjust as needed
        url = "https://example.com"
        expected_image_url = "https://media.defense.gov/2023/Jan/13/2003144784/-1/-1/0/230110-F-XY111-1001.JPG"  # Adjust based on your expectations

        # Call the function under test
        found_image_url = _get_terminal_image_url(url)

        # Verify the function returns the expected URL
        self.assertEqual(found_image_url, expected_image_url)

        # Additional assertions can be made here, for example, checking call count
        mock_get_with_retry.assert_called_once_with(url)
