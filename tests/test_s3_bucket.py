import os
import unittest
from unittest.mock import MagicMock, patch

from s3_bucket import S3Bucket


class DirectoryExistsError(Exception):
    """Custom exception for directory existence check failure in tests."""

    pass


class CreateDirectoryError(Exception):
    """Custom exception for directory creation failure in tests."""

    pass


class MoveObjectError(Exception):
    """Custom exception for object move failure in tests."""

    pass


class TestS3Bucket(unittest.TestCase):
    """Test S3Bucket class."""

    def setUp(self: "TestS3Bucket") -> None:
        """Set up test environment for S3Bucket."""
        patcher = patch("boto3.client")
        self.mock_boto_client = patcher.start()
        self.addCleanup(patcher.stop)  # Ensure the patcher is stopped after tests

        os.environ["AWS_ACCESS_KEY_ID"] = "fake_access_key"
        os.environ["AWS_SECRET_ACCESS_KEY"] = (
            "fake_secret_key"  # noqa: S105 (Fake key for tests)
        )
        os.environ["AWS_BUCKET_NAME"] = "fake_bucket_name"
        self.mock_client = self.mock_boto_client.return_value
        self.s3_bucket = S3Bucket()

    def test_upload_to_s3(self: "TestS3Bucket") -> None:
        """Test upload_to_s3 method."""
        self.s3_bucket.upload_to_s3("local/path", "s3/path")
        self.mock_client.upload_file.assert_called_with(
            "local/path", "fake_bucket_name", "s3/path"
        )

    def test_list_s3_files(self: "TestS3Bucket") -> None:
        """Test list_s3_files method."""
        mock_response = {"Contents": [{"Key": "file1"}, {"Key": "file2"}]}
        self.mock_client.list_objects.return_value = mock_response
        result = self.s3_bucket.list_s3_files("prefix")
        self.assertEqual(result, ["file1", "file2"])

    def test_download_from_s3(self: "TestS3Bucket") -> None:
        """Test download_from_s3 method."""
        self.s3_bucket.download_from_s3("s3/path", "local/path")
        self.mock_client.download_file.assert_called_with(
            "fake_bucket_name", "s3/path", "local/path"
        )

    def test_move_object_success(self: "TestS3Bucket") -> None:
        """Test successful move_object operation."""
        self.mock_client.copy_object = MagicMock()
        self.mock_client.delete_object = MagicMock()
        self.s3_bucket.move_object("source/path", "destination/path")
        self.mock_client.copy_object.assert_called_once()
        self.mock_client.delete_object.assert_called_once()

    def test_move_object_failure(self: "TestS3Bucket") -> None:
        """Test failure in move_object operation with specific exception."""
        self.mock_client.copy_object.side_effect = MoveObjectError("Copy failed")
        with self.assertRaises(MoveObjectError):
            self.s3_bucket.move_object("source/path", "destination/path")
        self.mock_client.copy_object.assert_called_once()
        self.mock_client.delete_object.assert_not_called()

    def test_create_directory_success(self: "TestS3Bucket") -> None:
        """Test successful create_directory operation."""
        self.mock_client.put_object = MagicMock()
        self.s3_bucket.create_directory("new/directory/")
        self.mock_client.put_object.assert_called_once_with(
            Bucket="fake_bucket_name", Key="new/directory/", Body=""
        )

    def test_create_directory_failure(self: "TestS3Bucket") -> None:
        """Test failure in create_directory operation with specific exception."""
        self.mock_client.put_object.side_effect = CreateDirectoryError(
            "Creation failed"
        )
        with self.assertRaises(CreateDirectoryError):
            self.s3_bucket.create_directory("new/directory/")
        self.mock_client.put_object.assert_called_once()

    def test_directory_exists(self: "TestS3Bucket") -> None:
        """Test directory_exists method."""
        self.mock_client.list_objects_v2.return_value = {
            "Contents": [{"Key": "directory/"}]
        }
        result = self.s3_bucket.directory_exists("directory")
        self.assertTrue(result)

    def test_directory_not_exists(self: "TestS3Bucket") -> None:
        """Test directory_exists method when directory does not exist."""
        self.mock_client.list_objects_v2.return_value = {"Contents": []}
        result = self.s3_bucket.directory_exists("directory")
        self.assertFalse(result)

    def test_gen_archive_dir_s3(self: "TestS3Bucket") -> None:
        """Test gen_archive_dir_s3 method."""
        self.mock_client.list_objects_v2.return_value = {"Contents": []}
        self.mock_client.put_object = MagicMock()
        result = self.s3_bucket.gen_archive_dir_s3("Terminal")
        self.assertEqual(result, "archive/Terminal/")

    def test_archive_pdf(self: "TestS3Bucket") -> None:
        """Test archive_pdf method."""
        pdf = MagicMock()
        pdf.terminal = "Terminal"
        pdf.type = "72_HR"
        pdf.filename = "test.pdf"
        pdf.cloud_path = "current/72_HR/test.pdf"
        self.mock_client.copy_object = MagicMock()
        self.mock_client.delete_object = MagicMock()
        self.s3_bucket.archive_pdf(pdf)
        self.mock_client.copy_object.assert_called_once()
        self.mock_client.delete_object.assert_called_once()

    def test_upload_pdf_to_current_s3(self: "TestS3Bucket") -> None:
        """Test upload_pdf_to_current_s3 method."""
        pdf = MagicMock()
        pdf.get_local_path.return_value = "local/test.pdf"
        pdf.type = "72_HR"
        pdf.filename = "test.pdf"
        self.mock_client.upload_file = MagicMock()
        self.s3_bucket.upload_pdf_to_current_s3(pdf)
        self.mock_client.upload_file.assert_called_once_with(
            "local/test.pdf", "fake_bucket_name", "current/72_HR/test.pdf"
        )

    def test_check_s3_pdf_dirs(self: "TestS3Bucket") -> None:
        """Test check_s3_pdf_dirs method."""
        self.mock_client.list_objects_v2.return_value = {"Contents": []}
        self.mock_client.put_object = MagicMock()
        self.s3_bucket.check_s3_pdf_dirs()
        expected_directories = [
            "current/",
            "archive/",
            "current/72_HR/",
            "current/30_DAY/",
            "current/ROLLCALL/",
        ]
        for dir_path in expected_directories:
            self.mock_client.put_object.assert_any_call(
                Bucket="fake_bucket_name", Key=dir_path, Body=""
            )

    def tearDown(self: "TestS3Bucket") -> None:
        """Tear down test environment for S3Bucket."""
        self.mock_client.reset_mock()


if __name__ == "__main__":
    unittest.main()
