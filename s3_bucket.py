import logging
import os
from typing import List

import boto3  # type: ignore
from botocore.exceptions import NoCredentialsError

from pdf import Pdf


class S3Bucket:
    """S3 bucket class for uploading, downloading, and moving files in S3."""

    def __init__(self: "S3Bucket") -> None:
        """Initialize the S3 bucket class.

        Tries to create a boto3 client using IAM role credentials.
        If it fails, it looks for AWS credentials in the environment variables.
        """
        # Try to initialize boto3 client with IAM role credentials
        try:
            self.client = boto3.client("s3")
            self.client.list_buckets()  # Test if client initialization was successful
            logging.info("Initialized boto3 client with IAM role credentials.")
        except NoCredentialsError:
            logging.warning(
                "No IAM role credentials found, looking for environment variables."
            )

            # Get keys from environment variables
            env_access_key_id = os.environ.get("AWS_ACCESS_KEY_ID")
            env_secret_access_key = os.environ.get("AWS_SECRET_ACCESS_KEY")
            env_bucket_name = os.environ.get("AWS_BUCKET_NAME")

            # Check environment variables
            if not all([env_access_key_id, env_secret_access_key, env_bucket_name]):
                logging.error("AWS configuration missing in environment variables!")
                msg = "Missing AWS configuration in environment variables."
                raise EnvironmentError(
                    msg
                ) from None  # Raise EnvironmentError without context to prevent logging credentials

            # Initialize client with environment variable credentials
            self.client = boto3.client(
                "s3",
                aws_access_key_id=env_access_key_id,
                aws_secret_access_key=env_secret_access_key,
            )
            logging.info(
                "Initialized boto3 client with environment variable credentials."
            )

        # Set bucket name
        self.bucket_name = env_bucket_name if env_bucket_name else "default-bucket-name"

    def upload_to_s3(self: "S3Bucket", local_path: str, s3_path: str) -> None:
        """Upload a file to S3.

        Args:
        ----
            local_path (str): The local path of the file to upload.
            s3_path (str): The S3 path to upload the file to.

        Returns:
        -------
            None

        Raises:
        ------
            Exception: If there is an error uploading the file to S3.
        """
        try:
            self.client.upload_file(local_path, self.bucket_name, s3_path)
            logging.info("Uploaded %s to %s", local_path, s3_path)
        except Exception as e:
            logging.error("Error uploading %s to %s: %s", local_path, s3_path, e)

    def list_s3_files(self: "S3Bucket", s3_prefix: str = "") -> List[str]:
        """List files in S3.

        Args:
        ----
            s3_prefix (str): The prefix to filter the files by.

        Returns:
        -------
            List[str]: A list of the files in S3. Empty list if no files found.
        """
        try:
            response = self.client.list_objects(
                Bucket=self.bucket_name, Prefix=s3_prefix
            )

            # Check if 'Contents' key exists in the response
            if "Contents" in response:
                return [item["Key"] for item in response["Contents"]]

            logging.warning("No files found in S3 with prefix: %s", s3_prefix)
            return []

        except Exception as e:
            logging.error("Error listing files in S3: %s", e)
            return []

    def download_from_s3(self: "S3Bucket", s3_path: str, local_path: str) -> None:
        """Download a file from S3.

        Args:
        ----
            s3_path (str): The S3 path of the file to download.
            local_path (str): The local path to download the file to.

        Returns:
        -------
            None
        """
        try:
            self.client.download_file(self.bucket_name, s3_path, local_path)
            logging.info("Downloaded %s to %s", s3_path, local_path)
        except Exception as e:
            logging.error("Error downloading %s to %s: %s", s3_path, local_path, e)

    def move_object(self: "S3Bucket", source_key: str, destination_key: str) -> None:
        """Move an object in S3.

        Args:
        ----
            source_key (str): The S3 path of the file to move.
            destination_key (str): The S3 path to move the file to.

        Returns:
        -------
            None
        """
        try:
            # Copy the object to the new location
            copy_source = {"Bucket": self.bucket_name, "Key": source_key}
            self.client.copy_object(
                CopySource=copy_source, Bucket=self.bucket_name, Key=destination_key
            )
            logging.info(
                "Successfully copied object from %s to %s in bucket %s.",
                source_key,
                destination_key,
                self.bucket_name,
            )

            # Delete the original object
            self.client.delete_object(Bucket=self.bucket_name, Key=source_key)
            logging.info(
                "Successfully deleted object from %s in bucket %s.",
                source_key,
                self.bucket_name,
            )

        except Exception as e:
            logging.error(
                "Failed to move object from %s to %s. Error: %s",
                source_key,
                destination_key,
                e,
            )
            raise

    def create_directory(self: "S3Bucket", path: str) -> None:
        """Create a directory in S3.

        Args:
        ----
            path (str): The path of the directory to create.

        Returns:
        -------
            None
        """
        try:
            # Make sure the path ends with a '/'
            if not path.endswith("/"):
                path += "/"

            # Creating an empty object representing the directory
            self.client.put_object(Bucket=self.bucket_name, Key=path, Body="")
            logging.info(
                "Successfully created directory at %s in bucket %s.",
                path,
                self.bucket_name,
            )

        except Exception as e:
            logging.error("Failed to create directory at %s. Error: %s", path, e)
            raise

    def directory_exists(self: "S3Bucket", path: str) -> bool:
        """Check if a directory exists in S3.

        Args:
        ----
            path (str): The path of the directory to check.

        Returns:
        -------
            bool: True if the directory exists, False otherwise.
        """
        try:
            # Make sure the path ends with a '/'
            if not path.endswith("/"):
                path += "/"

            # List objects with the given prefix (i.e., directory)
            response = self.client.list_objects_v2(
                Bucket=self.bucket_name, Prefix=path, MaxKeys=1
            )

            # Check if any objects are returned with the given prefix
            if "Contents" in response and response["Contents"]:
                logging.info(
                    "Directory %s exists in bucket %s.",
                    path,
                    self.bucket_name,
                )
                return True

            logging.info(
                "Directory %s does not exist in bucket %s.", path, self.bucket_name
            )
            return False

        except Exception as e:
            logging.error(
                "Failed to check if directory %s exists in bucket %s. Error: %s",
                path,
                self.bucket_name,
                e,
            )
            raise

    def gen_archive_dir_s3(self: "S3Bucket", terminal_name: str) -> str:
        """Generate the archive directory for a terminal in S3.

        Args:
        ----
            terminal_name (str): The name of the terminal to generate the archive directory for.

        Returns:
        -------
            str: The path of the archive directory for the terminal.
        """
        logging.info("Creating archive directories in s3 bucket: %s", self.bucket_name)

        archive_dir = "archive/"

        # Sub directories for sorting the different types of
        # PDFs a terminal can generate.
        dir_types = ["72_HR/", "30_DAY/", "ROLLCALL/"]

        # Convert terminal name to snake case
        snake_case_name = terminal_name.replace(" ", "_")
        terminal_archive_dir = archive_dir + snake_case_name + "/"

        try:
            # Create base terminal folder in archive if it doesn't exist.
            if not self.directory_exists(terminal_archive_dir):
                self.create_directory(terminal_archive_dir)
                logging.info("Created directory %s in s3.", terminal_archive_dir)

            for dir_type in dir_types:
                sub_dir = terminal_archive_dir + dir_type

                if not self.directory_exists(sub_dir):
                    self.create_directory(sub_dir)
                    logging.info("Created sub directory %s in s3.", sub_dir)

        except Exception as e:
            logging.error(
                "Error while generating archive directories for %s in bucket %s. Error: %s",
                terminal_name,
                self.bucket_name,
                e,
            )
            raise

        return terminal_archive_dir

    def archive_pdf(self: "S3Bucket", pdf: Pdf) -> None:
        """Archive a PDF in S3.

        This will use the information in the passed in PDF object to archive the corresponding PDF in S3.

        Args:
        ----
            pdf (Pdf): The PDF to archive.

        Returns:
        -------
            None
        """
        # Verify the archive directory for the PDF exists
        # if not we'll make it.
        terminal_archive_dir = self.gen_archive_dir_s3(pdf.terminal)

        # Create destination path
        pdf_type_dir = pdf.type + "/"
        dest_dir = os.path.join(terminal_archive_dir, pdf_type_dir)
        dest_dir = os.path.join(dest_dir, pdf.filename)

        # Move PDF to archive directory
        self.move_object(pdf.cloud_path, dest_dir)
        logging.info("%s was successfully archived.", pdf.filename)

        # Set new cloud path for the pdf
        pdf.cloud_path = dest_dir

    def upload_pdf_to_current_s3(self: "S3Bucket", pdf: Pdf) -> None:
        """Upload a PDF to the current directory of the S3 bucket.

        Args:
        ----
            pdf (Pdf): The PDF to upload.

        Returns:
        -------
            None
        """
        logging.info("Entering upload_pdf_to_current_s3()")

        local_path = pdf.get_local_path()

        dest_path = os.path.join("current/", pdf.type)
        dest_path = os.path.join(dest_path, pdf.filename)

        # Update pdf object
        pdf.cloud_path = dest_path

        self.upload_to_s3(local_path, dest_path)

    def check_s3_pdf_dirs(self: "S3Bucket") -> None:
        """Check if the current and archive directories exist in S3.

        If they don't exist, they will be created.

        Returns
        -------
            None
        """
        current_dir = "current/"
        archive_dir = "archive/"
        pdf_type_dirs = ["72_HR/", "30_DAY/", "ROLLCALL/"]

        if not self.directory_exists(current_dir):
            self.create_directory(current_dir)

        for dir_type in pdf_type_dirs:
            curre_path = os.path.join(current_dir, dir_type)

            if not self.directory_exists(curre_path):
                self.create_directory(curre_path)

        if not self.directory_exists(archive_dir):
            self.create_directory(archive_dir)
