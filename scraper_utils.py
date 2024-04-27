import glob
import hashlib
import logging
import os
import re
import time
import uuid
from functools import wraps
from typing import Any, Callable, List, Optional, Sequence, Tuple, TypeVar
from urllib.parse import quote, unquote, urlparse

import requests
from bs4 import BeautifulSoup  # type: ignore


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
    set_env_vars = list(os.environ.keys())

    empty_vars = []
    for var in variables:
        if var not in set_env_vars:
            logging.error("The following variable is missing in .env: %s", var)
            empty_vars.append(var)
        elif not os.getenv(var):
            logging.error("The following variable is empty in .env: %s", var)
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
    timeout = 5

    url = ensure_url_encoded(url)

    for attempt in range(max_attempts):
        try:
            logging.debug("Sending GET request.")

            response = requests.get(url, timeout=timeout)

            logging.debug("GET request successful.")
            return response

        except requests.Timeout:
            logging.warning("Request to %s timed out.", url)

        except Exception as e:  # Catch any exceptions
            logging.warning("Request to %s failed in get_with_retry().", url)
            logging.debug("Error: %s", e)

        # If it was not the last attempt
        if attempt < max_attempts:
            logging.info("Retrying request to %s in %d seconds...", url, delay)
            time.sleep(delay)  # Wait before next attempt
            delay *= 2
            timeout += 5

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
    base_name = base_name.replace(" ", "_")

    # Ensure the file is a PDF
    if ext.lower() != ".pdf":
        logging.error("%s is not a PDF!", file_path)
        return ""

    random_uuid = str(uuid.uuid4())

    # Construct the new file name
    uuid_name = f"{base_name}_{random_uuid}.pdf"

    # Encode it to prevent issues with special characters
    return quote(uuid_name)


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


# Define a type variable that can be any subtype of 'object'
T = TypeVar("T")


def deduplicate_with_attribute(object_list: Sequence[T], attribute: str) -> List[T]:
    """Deduplicate a list of objects based on a specified attribute.

    Args:
    ----
        object_list: The list of objects to deduplicate.
        attribute: The attribute to deduplicate on.

    Returns:
    -------
        A list of unique objects based on the specified attribute.

    """
    unique_attrs = set()
    unique_objects: List[T] = []  # Specify the list to hold objects of type T
    unique_count = 0  # Tracks the number of unique attributes seen

    if not object_list:
        return []

    if not hasattr(object_list[0], attribute):
        logging.error("Attribute %s does not exist in object list.", attribute)
        msg = f"Attribute {attribute} does not exist in object list."
        raise AttributeError(msg)

    for obj in object_list:
        attr_value = getattr(obj, attribute)
        # Attempt to add the attribute value to the set
        unique_attrs.add(attr_value)
        if len(unique_attrs) > unique_count:
            # If the set size increased, update the unique count and add the object to the result list
            unique_count += 1
            unique_objects.append(obj)

    return unique_objects


def get_terminal_name_from_page(terminal_page_url: str) -> Optional[str]:
    """Extract the terminal name from the terminal page URL.

    Args:
    ----
        terminal_page_url: The URL of the terminal page.

    Returns:
    -------
        The terminal name extracted from the terminal page.

    """
    # Download the terminal page
    response = get_with_retry(terminal_page_url)

    if response is None:
        logging.error("Failed to download terminal page.")
        return None

    # Extract the terminal name from the page
    terminal_name = extract_h1_terminal_name(response.text)

    if terminal_name is None:
        logging.error("Failed to extract terminal name.")
        return None

    # Capitalize the terminal name
    return capitilize_words_and_abbreviations(
        terminal_name,
        [
            "afb",  # Air Force Base
            "ab",  # Air Base
            "ns",  # Naval Station
            "nas",  # Naval Air Station
            "nsa",  # Naval Support Activity
            "raf",  # Royal Air Force
            "jb",  # Joint Base
            "mcas",  # Marine Corps Air Station
            "raaf",  # Royal Australian Air Force
            "naf",  # Naval Air Facility
            "usaf",  # United States Air Force
            "usa",  # United States of America
            "sfb",  # Space Force Base
            "angb",  # Air National Guard Base
            "ang",  # Air National Guard
            "arb",  # Air Reserve Base
            "ars",  # Air Reserve Station
            "jrb",  # Joint Reserve Base
        ],
    )


def extract_h1_terminal_name(html: str) -> Optional[str]:
    """Extract the text of the <h1> tag from the HTML content.

    Generally, the <h1> tag contains the main heading of the page and the
    name of the terminal.

    Args:
    ----
        html: The HTML content to parse.

    Returns:
    -------
        The text of the <h1> tag, or None if the tag is not found.

    """
    # Parse the HTML content
    soup = BeautifulSoup(html, "html.parser")

    # Find the <figure> element with class "hero banner"
    figure = soup.find("figure", class_="hero banner")

    # Extract the text from the <h1> tag inside <figcaption>
    if figure:
        h1_tag = figure.find("figcaption").find("h1")
        if h1_tag:
            return h1_tag.text
    return None


def capitilize_words_and_abbreviations(
    input_string: str, abbreviations: List[str]
) -> str:
    """Format a string by capitalizing the first letter of each word.

    If a word is in the list of abbreviations, the entire word will be converted to uppercase.
    Ensure that the abbreviations list is in lowercase. Example: ["pdf", "html", "xml"]

    Example: "this is a pdf file" -> "This Is A PDF File" if "pdf" is in the abbreviations list.

    Args:
    ----
        input_string: The string to format.
        abbreviations: A list of abbreviations to convert to uppercase.

    Returns:
    -------
        The formatted string.

    """
    # Convert the input string to lowercase first
    words = input_string.lower().split()

    # Process each word in the string
    formatted_words = []
    for word in words:
        # Check if the word is in the abbreviations list
        if word in abbreviations:
            # If yes, convert the entire word to uppercase
            formatted_words.append(word.upper())
        else:
            # Otherwise, capitalize the first letter of the word
            formatted_words.append(word.capitalize())

    # Join all the formatted words back into a single string
    return " ".join(formatted_words)
