import datetime
import hashlib
import logging
import os
from typing import Any, Dict, List, Type

from PyPDF2 import PdfReader

import scraper_utils
from pdf_page import PdfPage


class Pdf:
    """Class to represent a PDF file."""

    def __init__(self: "Pdf", link: str, populate: bool = True) -> None:
        """Initialize a PDF object.

        The PDF is initialized with the link to the PDF. If populate is True, the PDF
        is downloaded, the hash is calculated, and the PDF metadata is read. If populate
        is False, the attributes are left blank.

        Args:
        ----
            link (str): Link to the PDF.
            populate (bool, optional): Whether to populate the PDF attributes. Defaults to True.

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

        Returns:
        -------
            None
        """
        self.filename = ""
        self.original_filename = ""
        self.link = link
        self.hash = ""
        self.first_seen_time = ""
        self.cloud_path = ""
        self.modify_time = ""
        self.creation_time = ""
        self.type = ""
        self.terminal = ""
        self.seen_before = False
        self.num_pages = -1
        self.num_words = -1
        self.num_chars = -1
        self.page_dimensions: List[PdfPage] = []

        if populate:
            self.populate()

    def populate(self: "Pdf") -> None:
        """Populate the PDF attributes."""
        self._gen_first_seen_time()
        self._download()
        if self.filename is None:
            self.seen_before = True
            return

        self._calc_hash()
        if self.hash is None:
            self.seen_before = True
            return

        self._get_pdf_metadata()

    def _download(self: "Pdf") -> None:
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
            return

        logging.warning("Download failed for link: %s", self.link)
        self.filename = ""
        self.cloud_path = ""
        return

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
        logging.info("Calculating hash for {self.filename}.")

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
        logging.info("Reading metadata from %s}.", self.get_local_path())

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

    def to_dict(self: "Pdf") -> Dict[str, str]:
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
            "terminal": self.terminal
            # 'seenBefore': self.seen_before  # This line is intentionally omitted; Only used in internal logic
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

        return pdf
