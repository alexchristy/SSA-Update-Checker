import logging
import os
from typing import Any, Dict, Optional

from firebase_admin import credentials, firestore, initialize_app  # type: ignore

from pdf import Pdf
from scraper_utils import is_valid_sha256
from terminal import Terminal


class FirestoreClient:
    """Client for interacting with the Firestore database."""

    def __init__(self: "FirestoreClient") -> None:
        """Initialize the Firestore client.

        Uses the FS_CREDS_PATH environment variable to get the path to the
        Firebase Admin SDK service account key JSON file (creds.json).
        """
        # Get the path to the Firebase Admin SDK service account key JSON file from an environment variable
        fs_creds_path = os.getenv("FS_CRED_PATH")

        # Initialize the credentials with the JSON file
        cred = credentials.Certificate(fs_creds_path)

        # Initialize the Firebase application with the credentials
        self.app = initialize_app(cred)

        # Create the Firestore client
        self.db = firestore.client(app=self.app)

    def set_document(
        self: "FirestoreClient",
        collection_name: str,
        document_name: str,
        data: Dict[str, Any],
    ) -> None:
        """Set the data for a document in a collection.

        Args:
        ----
            collection_name (str): The name of the collection
            document_name (str): The name of the document
            data (Dict[str, Any]): The data to set for the document

        Returns:
        -------
            None
        """
        doc_ref = self.db.collection(collection_name).document(document_name)
        doc_ref.set(data)

    def upsert_document(
        self: "FirestoreClient",
        collection_name: str,
        document_name: str,
        data: Dict[str, Any],
    ) -> None:
        """Upsert data for a document in a collection.

        This will update the document with the provided data, or create it if it doesn't exist.

        Args:
        ----
            collection_name (str): The name of the collection
            document_name (str): The name of the document
            data (Dict[str, Any]): The data to set for the document

        Returns:
        -------
            None
        """
        doc_ref = self.db.collection(collection_name).document(document_name)
        doc_ref.set(data, merge=True)

    def upsert_terminal_info(self: "FirestoreClient", terminal: Terminal) -> None:
        """Upsert terminal object in the Terminals collection.

        This will update the fields of the terminal not including the hashes if the document
        already exists. If the terminal does not exist, then the pdf hash fields will also be updated.

        Args:
        ----
            terminal (Terminal): The terminal object to upsert

        Returns:
        -------
            None
        """
        # Get enviroment variable
        terminal_coll = os.getenv("TERMINAL_COLL")

        if not terminal_coll:
            logging.error("Terminal collection name not found in enviroment variables.")
            return

        # Get the terminal document
        doc_ref = self.db.collection(terminal_coll).document(terminal.name)
        doc = doc_ref.get()

        if doc.exists:
            # Specify what fields are terminal info
            updates = {
                "name": terminal.name,
                "link": terminal.link,
                "pagePosition": terminal.page_pos,
                "location": terminal.location,
                "group": terminal.group,
            }

            doc_ref.update(updates)
        else:
            self.upsert_document(terminal_coll, terminal.name, terminal.to_dict())

    def update_terminal_pdf_hash(self: "FirestoreClient", pdf: Pdf) -> None:
        """Update the hash of a PDF for a terminal in the Terminals collection.

        This will update the hash of the PDF for the terminal in the Terminals collection.

        Args:
        ----
            pdf (Pdf): The PDF object to upsert

        Returns:
        -------
            None
        """
        # Get enviroment variable
        terminal_coll = os.getenv("TERMINAL_COLL")

        doc_ref = self.db.collection(terminal_coll).document(pdf.terminal)

        if pdf.type == "72_HR":
            doc_ref.update({"pdf72HourHash": pdf.hash})
            logging.info("Updated %s with new 72 hour hash.", pdf.terminal)
        elif pdf.type == "30_DAY":
            doc_ref.update({"pdf30DayHash": pdf.hash})
            logging.info("Updated %s with new 30 day hash.", pdf.terminal)
        elif pdf.type == "ROLLCALL":
            doc_ref.update({"pdfRollcallHash": pdf.hash})
            logging.info("Updated %s with new rollcall hash.", pdf.terminal)
        else:
            logging.error(
                "Unable to update terminal with %s. Invalid PDF type: %s.",
                pdf.filename,
                pdf.type,
            )

    def upsert_pdf_to_archive(self: "FirestoreClient", pdf: Pdf) -> None:
        """Upsert Pdf object into the PDF Archive/seen before collection.

        This will update the field of the terminal with the provided data or create
        it if it does not exist.

        Args:
        ----
            pdf (Pdf): The PDF object to upsert

        Returns:
        -------
            None
        """
        # Get enviroment variable
        pdf_archive_coll = os.getenv("PDF_ARCHIVE_COLL")

        if not pdf_archive_coll:
            logging.error(
                "PDF Archive collection name not found in enviroment variables."
            )
            return

        self.upsert_document(pdf_archive_coll, pdf.hash, pdf.to_dict())

    def pdf_seen_before(self: "FirestoreClient", pdf: Pdf) -> bool:
        """Check if a PDF file has been seen before in Firestore.

        Args:
        ----
            pdf (Pdf): The PDF object to check

        Returns:
        -------
            bool: True if the PDF has been seen before, False otherwise
        """
        # If hash is not valid return True so that the PDF
        # is discarded.
        if not is_valid_sha256(pdf.hash):
            logging.error("%s is not a valid sha256 hash.", pdf.hash)
            return True

        collection_name = os.getenv("PDF_ARCHIVE_COLL")

        if not collection_name:
            logging.error(
                "PDF Archive collection name not found in enviroment variables."
            )
            # Raise an exception here
            msg = "PDF Archive collection name not found in enviroment variables."
            raise EnvironmentError(msg)

        # Create a reference to the document using the SHA-256 hash as the document ID
        doc_ref = self.db.collection(collection_name).document(pdf.hash)

        # Try to retrieve the document
        doc = doc_ref.get()

        # Check if the document exists
        if doc.exists:
            # The document exists, indicating that the PDF has been seen before
            logging.info(
                "%s with hash %s has been seen before.", pdf.filename, pdf.hash
            )
            return True

        # The document does not exist, indicating that the PDF is new
        logging.info(
            "%s with hash %s has NEVER been seen before.",
            pdf.filename,
            pdf.hash,
        )
        return False

    def archive_pdf(self: "FirestoreClient", pdf: Pdf) -> bool:
        """Archive a PDF file in Firestore.

        Args:
        ----
            pdf (Pdf): The PDF object to archive

        Returns:
        -------
            bool: True if the PDF was archived, False otherwise
        """
        if pdf.seen_before:
            logging.warning(
                "Not archiving %s. Marked for being discarded.",
                pdf.filename,
            )
            return False

        collection_name = os.getenv("PDF_ARCHIVE_COLL")

        if not collection_name:
            logging.error(
                "PDF Archive collection name not found in enviroment variables."
            )
            # Raise an exception here
            msg = "PDF Archive collection name not found in enviroment variables."
            raise EnvironmentError(msg)

        self.set_document(collection_name, pdf.hash, pdf.to_dict())
        logging.info("Inserted PDF into archive at %s", pdf.hash)

        return True

    def get_pdf_by_hash(self: "FirestoreClient", hash_str: str) -> Optional[Pdf]:
        """Get a PDF object from the PDF Archive/seen before collection.

        Args:
        ----
            hash_str (str): The hash of the PDF to retrieve

        Returns:
        -------
            Optional[Pdf]: The retrieved PDF object, or None if the PDF was not found
        """
        logging.info("Entering get_pdf_by_hash().")

        # Verify that the hash is a valid sha256 hash
        if not is_valid_sha256(hash_str):
            logging.error("Invalid hash supplied: %s", hash_str)
            return None

        # Get the name of the collection from an environment variable
        collection_name = os.getenv("PDF_ARCHIVE_COLL")

        if not collection_name:
            logging.error(
                "PDF Archive collection name not found in enviroment variables."
            )
            msg = "PDF Archive collection name not found in enviroment variables."
            raise EnvironmentError(msg)

        # Create a reference to the document using the SHA-256 hash as the document ID
        doc_ref = self.db.collection(collection_name).document(hash_str)

        # Try to retrieve the document
        doc = doc_ref.get()

        # Check if the document exists
        if doc.exists:
            # The document exists, so we retrieve its data and create a Pdf object
            logging.info("PDF with hash %s found in the database.", hash_str)

            # Get the document's data
            pdf_data = doc.to_dict()

            # Create a Pdf object from the retrieved data
            return Pdf.from_dict(pdf_data)

        logging.warning("PDF with hash %s does not exist in the database.", hash_str)
        return None

    def get_all_terminals(self: "FirestoreClient") -> list[Terminal]:
        """Get all terminal objects from the Terminals collection.

        Returns
        -------
            list[Terminal]: A list of all terminal objects
        """
        terminal_coll = os.getenv("TERMINAL_COLL")

        if not terminal_coll:
            logging.error("Terminal collection name not found in enviroment variables.")
            return []

        terminals_ref = self.db.collection(terminal_coll)

        terminals = terminals_ref.stream()

        terminal_objects = []

        for terminal in terminals:
            curr_terminal = Terminal.from_dict(terminal.to_dict())
            terminal_objects.append(curr_terminal)

        return terminal_objects
