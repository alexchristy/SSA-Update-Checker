import glob
import hashlib
import logging
import os
import re
import time
import uuid
from functools import wraps
from typing import Any, Callable, List, Optional, Tuple
from urllib.parse import quote, unquote, urlparse

import requests
from dotenv import load_dotenv


def timing_decorator(func: Callable[..., Any]) -> Callable[..., Any]:
    """Return a decorator that times the execution of a function.

    Args:
    ----
        func: The function to be decorated.

    Returns:
    -------
        The decorated function.
    """

    @wraps(func)
    def wrapper(  # noqa: ANN202 (No return type annotation because the return type is unknown)
        *args: Tuple, **kwargs: dict[str, Any]
    ):
        try:
            start_time = time.time()
            result = func(*args, **kwargs)
            end_time = time.time()
            elapsed_time = end_time - start_time
            logging.info("%s took %d seconds to run", func.__name__, elapsed_time)
            return result
        except Exception as e:
            logging.error(
                "An exception occurred while running %s: %s", func.__name__, e
            )
            raise

    return wrapper


def check_env_variables(variables: List[str]) -> bool:
    """Verfy that all of the required environment variables are set.

    Args:
    ----
        variables: A list of environment variables to check.

    Returns:
    -------
        True if all of the environment variables are set, False otherwise.
    """
    load_dotenv()

    empty_vars = []
    for var in variables:
        value = os.getenv(var)
        if not value:
            empty_vars.append(var)

    if empty_vars:
        err_msg = f"The following variable(s) are missing or empty in .env: {', '.join(empty_vars)}"
        logging.error(err_msg)
        return False

    return True


def clean_up_tmp_pdfs() -> bool:
    """Remove all PDFs from the tmp/ directory."""
    base_dir = os.getenv("PDF_DIR")

    if not base_dir:
        logging.error("PDF_DIR environment variable is not set.")
        return False

    tmp_dir = os.path.join(base_dir, "tmp")

    # Look for all PDF files in the directory and its subdirectories
    pdf_files = glob.glob(os.path.join(tmp_dir, "**", "*.pdf"), recursive=True)

    for pdf_file in pdf_files:
        try:
            os.remove(pdf_file)
            logging.info("Removed %s", pdf_file)
        except Exception as e:
            logging.error("Error removing %s. Error: %s", pdf_file, str(e))
            return False

    return True


def check_local_pdf_dirs() -> bool:
    """Verify that the local PDF directories exist and create them if they don't.

    The PDFs will be stored in the following structure under the base directory
    specified by the $PDF_DIR environment variable:

    ./{$PDF_DIR}/
    |
    +----tmp/
    |    +--72_HR/
    |    +--30_DAY/
    |    +--ROLLCALL/
    |
    +----current/
    |    +------72_HR/
    |    +------30_DAY/
    |    +------ROLLCALL/
    |
    +----archive/
    |    +------72_HR/
    |    +------30_DAY/
    |    +------ROLLCALL/

    Returns
    -------
        True if the directories are successfully created or already exist, False otherwise.
    """
    base_dir = os.getenv("PDF_DIR")

    if not base_dir:
        logging.error("PDF_DIR environment variable is not set.")
        return False

    use_dirs = ["tmp/", "current/", "archive/"]  # Added 'current/' and 'archive/'
    type_dirs = ["72_HR/", "30_DAY/", "ROLLCALL/"]

    # Ensure base_dir ends with '/'
    if not base_dir.endswith("/"):
        base_dir += "/"

    try:
        # Create base dir if it doesn't exist
        if not os.path.exists(base_dir):
            os.mkdir(base_dir)

        # Create use dirs: tmp/, current/, archive/
        for use_dir in use_dirs:
            use_dir_path = base_dir + use_dir

            # Create use dir if it doesn't exist
            if not os.path.exists(use_dir_path):
                os.mkdir(use_dir_path)

            # Create type dirs: 72_HR, 30_DAY, ROLLCALL
            for type_dir in type_dirs:
                type_dir_path = use_dir_path + type_dir

                # Create type dir if it doesn't exist
                if not os.path.exists(type_dir_path):
                    os.mkdir(type_dir_path)
    except Exception as e:
        logging.error("Error creating PDF directories: %s", str(e))
        return False

    return True


def ensure_url_encoded(url: str) -> str:
    """Ensure that the URL is encoded.

    If the URL is not encoded, it will be encoded. If the URL is already encoded,
    it will be returned unchanged.

    Args:
    ----
        url: The URL to encode.

    Returns:
    -------
        The encoded URL.
    """
    unquoted_url = unquote(url)

    if unquoted_url == url:
        return quote(url, safe=":/.-")

    # If the URLs are different, it was already encoded.
    return url


def extract_relative_path_from_full_path(
    base_segment: str, full_path: str
) -> Optional[str]:
    """Extract the relative path from a given full path starting from a specified base segment.

    Args:
    ----
        base_segment: The segment of the path from which to start the relative path.
        full_path: The complete path that includes the base segment and the subsequent path.

    Returns:
    -------
        A string representing the relative path starting from the base segment, or
        None if the base segment is not found in the full path.

    Example:
    -------
        If full_path is "/home/user/documents/archive/2023/report.pdf" and
        base_segment is "archive", the function will return "archive/2023/report.pdf".
    """
    if not base_segment:
        logging.error("base_segment is empty.")
        return None

    if base_segment in full_path:
        start_index = full_path.index(base_segment)
        return full_path[start_index:]

    return None


@timing_decorator  # Timer for debugging connection issues
def get_with_retry(url: str) -> Optional[requests.Response]:
    """Send a GET request to the given URL and retry if it fails.

    Args:
    ----
        url: The URL to send the GET request to.

    Returns:
    -------
        The response object if the request was successful, None otherwise.
    """
    logging.debug("Entering get_with_retry() requesting: %s", url)

    max_attempts = 3
    delay = 2

    url = ensure_url_encoded(url)

    for attempt in range(max_attempts):
        try:
            logging.debug("Sending GET request.")

            response = requests.get(url, timeout=5)

            logging.debug("GET request successful.")
            return response

        except requests.Timeout:
            logging.error("Request to %s timed out.", url)

        except Exception as e:  # Catch any exceptions
            logging.error("Request to %s failed in get_with_retry().", url)
            logging.debug("Error: %s", e)

        # If it was not the last attempt
        if attempt < max_attempts:
            logging.info("Retrying request to %s in %d seconds...", url, delay)
            time.sleep(delay)  # Wait before next attempt
            delay *= 2

        # It was the last attempt
        else:
            logging.error("All attempts failed.")

    return None


def calc_sha256_hash(input_string: str) -> str:
    """Calculate the SHA-256 hash of a given input string.

    Args:
    ----
        input_string: The string to hash.

    Returns:
    -------
        The SHA-256 hash of the input string.
    """
    # Create a new SHA-256 hash object
    sha256_hash = hashlib.sha256()

    # Update the hash object with the bytes of the input string
    sha256_hash.update(input_string.encode("utf-8"))

    # Get the hexadecimal representation of the hash
    return sha256_hash.hexdigest()


def is_valid_sha256(s: str) -> bool:
    """Check if the given string is a valid SHA256 checksum.

    Args:
    ----
        s: The string to check.

    Returns:
    -------
        True if the string is a valid SHA256 checksum, False otherwise.
    """
    hash_length = 64

    if len(s) != hash_length:
        return False

    # Regular expression pattern for a hexadecimal number
    if re.match("^[a-fA-F0-9]{64}$", s):
        return True

    return False


def normalize_url(url: str) -> str:
    """Normalize a URL by removing the path and query string and adding https://.

    Args:
    ----
        url: The URL to normalize.

    Returns:
    -------
        The normalized URL, or an empty string if the URL is malformed.
    """
    logging.debug("Entering normalize_url()")

    # Prepend 'http://' if the URL does not have a scheme
    if not urlparse(url).scheme:
        url = "http://" + url

    parsed_url = urlparse(url)
    hostname = str(parsed_url.netloc)

    if not hostname:
        return ""

    return "https://" + hostname + "/"


def get_pdf_name(url: str) -> str:
    """Get the name of the PDF file from the URL.

    Args:
    ----
        url: The URL to get the PDF name from.

    Returns:
    -------
        The name of the PDF file, or an empty string if no PDF file is present.
    """
    result = urlparse(url)
    path = unquote(result.path)
    filename = path.split("/")[-1]

    # Return filename only if it's a PDF
    if filename.lower().endswith(".pdf"):
        return filename
    return ""


def gen_pdf_name_uuid(file_path: str) -> str:
    """Generate a new PDF file name with a random UUID.

    The new file name will be in the format: {base_name}_{random_uuid}.pdf

    Args:
    ----
        file_path: The path to the PDF file.

    Returns:
    -------
        The new file name.
    """
    dir_path, file_name = os.path.split(file_path)
    base_name, ext = os.path.splitext(file_name)

    # Ensure the file is a PDF
    if ext.lower() != ".pdf":
        logging.error("%s is not a PDF!", file_path)
        return ""

    random_uuid = str(uuid.uuid4())

    # Construct the new file name
    return f"{base_name}_{random_uuid}.pdf"


def format_pdf_metadata_date(date_str: str) -> Optional[str]:
    """Format a PDF metadata date string.

    Args:
    ----
        date_str: The date string to format.

    Returns:
    -------
        The formatted date string, or None if the date string is invalid.
    """
    if date_str is None:
        return None

    # Regular expression to extract the date components
    match = re.match(r"D:(\d{4})(\d{2})(\d{2})(\d{2})(\d{2})(\d{2})", date_str)
    if match:
        # Format the date to YYYYMMDDHHMMSS
        return "".join(match.groups())

    return None
