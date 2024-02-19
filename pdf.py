import datetime
import hashlib
import logging
import os
from typing import Any, Dict, List, Optional, Type

from pdfminer.high_level import extract_pages
from pdfminer.layout import LTTextContainer
from PyPDF2 import PdfReader

import scraper_utils
from pdf_page import PdfPage


def count_words_in_pdf(pdf_path: str) -> Optional[int]:
    """Count the number of words in a PDF.

    Args:
    ----
        pdf_path (str): Path to PDF file

    Returns:
    -------
        Optional[int]: Number of words in the PDF, None if an error occurred

    """
    try:
        word_count = 0

        # Process each page in the PDF
        for page_layout in extract_pages(pdf_path):
            for element in page_layout:
                if isinstance(element, LTTextContainer):
                    # Extract text and count words in each text container
                    text = element.get_text()
                    words = text.split()
                    word_count += len(words)

        return word_count
    except Exception as e:
        logging.error("An error occurred while counting words: %s", e)
        return None


def count_characters_in_pdf(pdf_path: str) -> Optional[int]:
    """Count the number of characters in a PDF.

    Args:
    ----
        pdf_path (str): Path to PDF file

    Returns:
    -------
        Optional[int]: Number of characters in the PDF, None if an error occurred

    """
    try:
        character_count = 0

        # Process each page in the PDF
        for page_layout in extract_pages(pdf_path):
            for element in page_layout:
                if isinstance(element, LTTextContainer):
                    # Count characters in each text container
                    text = element.get_text()
                    character_count += len(text)

        return character_count
    except Exception as e:
        logging.error("An error occurred while counting characters: %s", e)
        return None


def count_pages_in_pdf(pdf_path: str) -> Optional[int]:
    """Count the number of pages in a PDF.

    Args:
    ----
        pdf_path (str): Path to PDF file

    Returns:
    -------
        Optional[int]: Number of pages in the PDF, None if an error occurred

    """
    try:
        # Open the PDF file
        with open(pdf_path, "rb") as file:
            reader = PdfReader(file)
            return len(reader.pages)
    except Exception as e:
        logging.error("An error occurred while counting pages: %s", e)
        return None


class Pdf:
    """Class to represent a PDF file."""

    MAX_PAGES = 15

    def __init__(
        self: "Pdf", link: str, populate: bool = False, hash_only: bool = False
    ) -> None:
        """Initialize a PDF object.

        The PDF is initialized with the link to the PDF. If populate is True, the PDF
        is downloaded, the hash is calculated, and the PDF metadata is read. If populate
        is False, the attributes are left blank.

        Args:
        ----
            link (str): Link to the PDF.
            populate (bool, optional): Whether to populate the PDF attributes. Defaults to False.
            hash_only (bool, optional): Whether to only calculate the hash. Defaults to False.

        Attributes:
        ----------
            filename (str): Filename of the PDF.
            link (str): Link to the PDF.
            hash (str): SHA256 hash of the PDF.
            first_seen_time (str): Time the PDF was first seen in YYYYMMDDHHMMSS format.
            cloud_path (str): Path to the PDF in the cloud storage bucket.
            modify_time (str): Modification time of the PDF in YYYYMMDDHHMMSS format.
            creation_time (str): Creation time of the PDF in YYYYMMDDHHMMSS format.
            type (str): Type of the PDF. Valid types are "72_HR", "30_DAY", "ROLLCALL", and "DISCARD".
            terminal (str): Terminal the PDF belongs to.
            seen_before (bool): Whether the PDF has been seen before.
            num_pages (int): Number of pages in the PDF.
            num_words (int): Number of words in the PDF.
            num_chars (int): Number of characters in the PDF.
            pages (List[PdfPage]): List of PdfPage objects representing the pages in the PDF.


        Returns:
        -------
            None

        """
        self.filename: str = ""
        self.original_filename: str = ""
        self.link: str = link
        self.hash: str = ""
        self.first_seen_time: str = ""
        self.cloud_path: str = ""
        self.modify_time: str = ""
        self.creation_time: str = ""
        self.type: str = ""
        self.terminal: str = ""
        self.seen_before = False
        self.num_pages = -1
        self.num_words = -1
        self.num_chars = -1
        self.pages: List[PdfPage] = []

        if populate:
            self.populate()

        if hash_only:
            self.populate_hash_only()

    def populate_hash_only(self: "Pdf") -> None:
        """Download the PDF and calculate the hash only."""
        was_downloaded = self._download()

        if not was_downloaded:
            self.seen_before = True
            return

        self._calc_hash()

    def populate(self: "Pdf") -> None:
        """Populate the PDF attributes."""
        if not self.seen_before:
            self._gen_first_seen_time()

        if not self.filename and not self.cloud_path:
            was_downloaded = self._download()
            if not was_downloaded:
                self.seen_before = True
                return

        if not self.hash:
            self._calc_hash()
            if self.hash is None:
                self.seen_before = True
                return

        self._get_num_pages()

        if self.num_pages > self.MAX_PAGES:
            logging.warning(
                "PDF %s has %d pages. Skipping word, character, and page details.",
                self.filename,
                self.num_pages,
            )
            self.num_words = -1
            self.num_chars = -1
            self.pages = []
            self.type = "DISCARD"
            return

        self._get_pdf_metadata()
        self._get_num_words()
        self._get_num_chars()
        self._populate_page_details()

    def _download(self: "Pdf") -> bool:
        """Download the PDF from the link."""
        logging.info("Starting download of PDF from %s...", self.link)

        download_dir = os.getenv("PDF_DIR")

        if download_dir is None:
            logging.critical("PDF_DIR environment variable is not set.")
            msg = "PDF_DIR environment variable is not set."
            raise EnvironmentError(msg)

        download_dir = os.path.join(download_dir, "tmp/")

        # Get the filename from the URL
        filename = scraper_utils.get_pdf_name(self.link)
        self.original_filename = filename
        filename = scraper_utils.gen_pdf_name_uuid(filename)

        # Get PDF from link
        response = scraper_utils.get_with_retry(self.link)

        # If download was successful
        success_code = 200
        if response is not None and response.status_code == success_code:
            # Make sure the directory exists, if not, create it.
            os.makedirs(download_dir, exist_ok=True)

            # Combine the directory with the filename
            filepath = os.path.join(download_dir, filename)

            # Write the content of the request to a file
            with open(filepath, "wb") as f:
                f.write(response.content)

            logging.info("Successfully downloaded %s at %s", self.link, filepath)

            # Set filename
            self.filename = filename

            # Store relative path for compatability
            relative_path = scraper_utils.extract_relative_path_from_full_path(
                "tmp/", filepath
            )

            if not relative_path:
                logging.error("Unable to extract relative path from %s.", filepath)
                msg = f"Unable to extract relative path from {filepath}."
                raise ValueError(msg)

            self.cloud_path = relative_path
            return True

        logging.warning("Download failed for link: %s", self.link)
        self.filename = ""
        self.cloud_path = ""
        return False

    def get_local_path(self: "Pdf") -> str:
        """Get the local path of the PDF."""
        if self.cloud_path is None:
            return None

        base_dir = os.getenv("PDF_DIR")

        if base_dir is None:
            logging.critical("PDF_DIR environment variable is not set.")
            msg = "PDF_DIR environment variable is not set."
            raise EnvironmentError(msg)

        return os.path.join(base_dir, self.cloud_path)

    def set_type(self: "Pdf", type_str: str) -> None:
        """Set the type of the PDF."""
        valid_types = ["72_HR", "30_DAY", "ROLLCALL", "DISCARD"]

        if type_str in valid_types:
            self.type = type_str
            logging.info("%s type set to %s.", self.filename, self.type)
            return

        logging.error(
            "Failed to set %s type. Invalid type %s.",
            self.filename,
            type_str,
        )
        return

    def set_terminal(self: "Pdf", terminal_name: str) -> None:
        """Set the terminal of the PDF."""
        if isinstance(terminal_name, str):
            self.terminal = terminal_name
            logging.info(
                "Set %sterminal attribute to %s.",
                self.filename,
                terminal_name,
            )
        else:
            logging.error(
                "Unable to set %s terminal. Invalid terminal string: %s.",
                self.filename,
                terminal_name,
            )

    def _calc_hash(self: "Pdf") -> None:
        logging.info("Calculating hash for %s.", self.get_local_path())

        sha256_hash = hashlib.sha256()

        try:
            with open(self.get_local_path(), "rb") as f:
                for byte_block in iter(lambda: f.read(4096), b""):
                    sha256_hash.update(byte_block)
        except FileNotFoundError as e:
            logging.error("File {self.get_local_path()} not found. Exception: %s", e)
        except Exception as e:
            logging.exception(
                "Unexpected error reading file %s. Error: %s", self.get_local_path(), e
            )
            return

        self.hash = sha256_hash.hexdigest()

    def _gen_first_seen_time(self: "Pdf") -> None:
        """Generate the current time in YYYYMMDDHHMMSS format and set it as firstSeenTime.

        Returns
        -------
            None

        """
        self.first_seen_time = datetime.datetime.now(tz=datetime.timezone.utc).strftime(
            "%Y%m%d%H%M%S"
        )

    def _get_pdf_metadata(self: "Pdf") -> None:
        logging.info("Reading metadata from %s.", self.get_local_path())

        try:
            with open(self.get_local_path(), "rb") as file:
                pdf_reader = PdfReader(file)
                metadata = pdf_reader.metadata

                if not metadata:
                    logging.warning(
                        "PDF metadata is empty for %s.", self.get_local_path()
                    )
                    return

                creation_date = metadata.get("/CreationDate", "")
                modification_date = metadata.get("/ModDate", "")

            returned_create_time = scraper_utils.format_pdf_metadata_date(creation_date)

            if returned_create_time is None:
                logging.error(
                    "Unable to parse creation date from PDF metadata for %s.",
                    self.get_local_path(),
                )
                self.creation_time = ""
            else:
                self.creation_time = returned_create_time

            returned_modify_time = scraper_utils.format_pdf_metadata_date(
                modification_date
            )

            if returned_modify_time is None:
                logging.error(
                    "Unable to parse modification date from PDF metadata for %s.",
                    self.get_local_path(),
                )
                self.modify_time = ""
            else:
                self.modify_time = returned_modify_time

        except FileNotFoundError as e:
            logging.error("File %s not found. Exception: %s", self.get_local_path(), e)
        except KeyError as e:
            logging.warning(
                "PDF metadata does not contain creation or modification dates. Exception: %s",
                e,
            )
        except Exception as e:
            logging.exception(
                "Unexpected error reading PDF metadata for %s. Exception: %s",
                self.get_local_path(),
                e,
            )

    def _get_num_pages(self: "Pdf") -> None:
        """Get the number of pages in the PDF."""
        path = self.get_local_path()

        if not path:
            logging.error(
                "Unable to get number of pages for %s. PDF not at path: %s.",
                self.filename,
                path,
            )
            return

        num_pages = count_pages_in_pdf(path)

        if num_pages is None:
            logging.error("Unable to get number of pages for %s.", self.filename)
            return

        self.num_pages = num_pages

    def _get_num_words(self: "Pdf") -> None:
        """Get the number of words in the PDF."""
        path = self.get_local_path()

        if not path:
            logging.error(
                "Unable to get number of words for %s. PDF not at path: %s.",
                self.filename,
                path,
            )
            return

        num_words = count_words_in_pdf(path)

        if num_words is None:
            logging.error("Unable to get number of words for %s.", self.filename)
            return

        self.num_words = num_words

    def _get_num_chars(self: "Pdf") -> None:
        """Get the number of characters in the PDF."""
        path = self.get_local_path()

        if not path:
            logging.error(
                "Unable to get number of characters for %s. PDF not at path: %s.",
                self.filename,
                path,
            )
            return

        num_chars = count_characters_in_pdf(path)

        if num_chars is None:
            logging.error("Unable to get number of characters for %s.", self.filename)
            return

        self.num_chars = num_chars

    def _count_words_on_page(self: "Pdf", pdf_path: str, page_number: int) -> int:
        """Count the number of words on a specific page of a PDF.

        Args:
        ----
            pdf_path (str): Path to PDF file.
            page_number (int): Page number to count words on.

        Returns:
        -------
            int: Number of words on the specified page.

        """
        word_count = 0
        for page_layout in extract_pages(pdf_path, page_numbers=[page_number]):
            for element in page_layout:
                if isinstance(element, LTTextContainer):
                    text = element.get_text()
                    words = text.split()
                    word_count += len(words)
        return word_count

    def _count_characters_on_page(self: "Pdf", pdf_path: str, page_number: int) -> int:
        """Count the number of characters on a specific page of a PDF.

        Args:
        ----
            pdf_path (str): Path to PDF file.
            page_number (int): Page number to count characters on.

        Returns:
        -------
            int: Number of characters on the specified page.

        """
        character_count = 0
        for page_layout in extract_pages(pdf_path, page_numbers=[page_number]):
            for element in page_layout:
                if isinstance(element, LTTextContainer):
                    text = element.get_text()
                    character_count += len(text)
        return character_count

    def _populate_page_details(self: "Pdf") -> None:
        """Populate details for each page in the PDF, including word and character counts.

        Returns
        -------
            None

        """
        if self.num_pages > self.MAX_PAGES:
            logging.warning(
                "PDF %s has %d pages. Skipping page details.",
                self.filename,
                self.num_pages,
            )
            return

        try:
            path = self.get_local_path()
            if not path:
                logging.error("PDF file path is invalid for %s.", self.filename)
                return

            with open(path, "rb") as file:
                pdf_reader = PdfReader(file)
                for i, page in enumerate(pdf_reader.pages):
                    pdf_page = PdfPage(page_number=i + 1)
                    pdf_page.degrees_of_rotation = page.get("/Rotate", 0)
                    pdf_page.width = int(page.mediabox.width)
                    pdf_page.height = int(page.mediabox.height)

                    # Count words and characters on the page
                    pdf_page.num_words = self._count_words_on_page(path, i)
                    pdf_page.num_chars = self._count_characters_on_page(path, i)

                    self.pages.append(pdf_page)
        except Exception as e:
            logging.error("Error populating page details for %s: %s", self.filename, e)

    def _extract_page_details(self: "Pdf", pdf_reader: PdfReader) -> List[PdfPage]:
        """Extract page details from the PDF reader and return a list of PdfPage objects."""
        pages = []
        for i, page in enumerate(pdf_reader.pages):
            pdf_page = PdfPage(page_number=i + 1)
            pdf_page.degrees_of_rotation = page.get("/Rotate", 0)
            pdf_page.width = int(page.mediabox.width)
            pdf_page.height = int(page.mediabox.height)
            pages.append(pdf_page)
        return pages

    def to_dict(self: "Pdf") -> Dict[str, Any]:
        """Convert PDF object to a dictionary, suitable for storing in Firestore.

        The seen_before attribute is excluded from the returned dictionary.
        """
        return {
            "filename": self.filename,
            "link": self.link,
            "hash": self.hash,
            "firstSeenTime": self.first_seen_time,
            "cloud_path": self.cloud_path,
            "modifyTime": self.modify_time,
            "creationTime": self.creation_time,
            "type": self.type,
            "terminal": self.terminal,
            # 'seenBefore': self.seen_before  # This line is intentionally omitted; Only used in internal logic
            "originalFilename": self.original_filename,
            "numPages": self.num_pages,
            "numWords": self.num_words,
            "numChars": self.num_chars,
            "pages": [page.to_dict() for page in self.pages],
        }

    @classmethod
    def from_dict(cls: Type["Pdf"], data: Dict[str, Any]) -> "Pdf":
        """Create a PDF object from a dictionary (e.g., a Firestore document).

        The seen_before attribute is set to False by default. Populate is set to
        false to prevent overwrite of attributes when reading from the database.

        Args:
        ----
            data (dict): Dictionary containing PDF attributes.

        Returns:
        -------
            Pdf: PDF object.

        """
        pdf = cls(link=data["link"], populate=False)
        pdf.filename = data["filename"]
        pdf.hash = data["hash"]
        pdf.first_seen_time = data["firstSeenTime"]
        pdf.cloud_path = data["cloud_path"]
        pdf.modify_time = data["modifyTime"]
        pdf.creation_time = data["creationTime"]
        pdf.type = data["type"]
        pdf.terminal = data["terminal"]
        pdf.seen_before = False
        pdf.original_filename = data["originalFilename"]
        pdf.num_pages = data["numPages"]
        pdf.num_words = data["numWords"]
        pdf.num_chars = data["numChars"]
        pdf.pages = [PdfPage.from_dict(page) for page in data["pages"]]

        return pdf

    def __eq__(self: "Pdf", other: object) -> bool:
        """Compare two PDF objects."""
        if not isinstance(other, Pdf):
            return NotImplemented

        return (
            self.filename == other.filename
            and self.link == other.link
            and self.hash == other.hash
            and self.first_seen_time == other.first_seen_time
            and self.cloud_path == other.cloud_path
            and self.modify_time == other.modify_time
            and self.creation_time == other.creation_time
            and self.type == other.type
            and self.terminal == other.terminal
            and self.seen_before == other.seen_before
            and self.original_filename == other.original_filename
            and self.num_pages == other.num_pages
            and self.num_words == other.num_words
            and self.num_chars == other.num_chars
            and self.pages == other.pages
        )
