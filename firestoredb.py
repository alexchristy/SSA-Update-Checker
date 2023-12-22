import logging
import os
from typing import Any, Dict, List, Optional

from firebase_admin import credentials, firestore, initialize_app  # type: ignore

from location_tz import TerminalTzFinder
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
                "timezone": terminal.timezone,
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

    def update_terminals(
        self: "FirestoreClient", scraped_terminals: List[Terminal]
    ) -> bool:
        """Update the Terminals collection with new terminal objects.

        This function will upsert the terminal objects in the Terminals collection.
        Additionally, it will add timezone attributes to the terminals only when the
        terminal documents do not have it already or the terminal's attribute has
        changed.

        Args:
        ----
            scraped_terminals (List[Terminal]): A list of newly scraped terminal objects to upsert

        Returns:
        -------
            bool: True if the terminals were updated, False otherwise
        """
        if not scraped_terminals:
            logging.error("No terminals provided to update.")
            return False

        # Get the name of the collection from an environment variable
        terminal_coll = os.getenv("TERMINAL_COLL")

        if not terminal_coll:
            logging.error("Terminal collection name not found in enviroment variables.")
            return False

        # Get all the terminals from the Terminals collection
        terminals_from_db = self.get_all_terminals()

        if not terminals_from_db:
            logging.warning("No terminals found in the database.")

        never_seen_terminals = []
        seen_terminals = []

        # Check for discrepancies between the number of terminals in the database
        # and the number of terminals found by the scraper
        if len(terminals_from_db) != len(scraped_terminals):
            scraper_terminal_set = set(scraped_terminals)
            old_terminal_set = set(terminals_from_db)

            # Get the difference between the two sets
            terminal_diff = scraper_terminal_set - old_terminal_set
            diff_names = [terminal.name for terminal in terminal_diff]

            if len(scraped_terminals) > len(terminals_from_db):
                logging.info("New terminals found: %s", diff_names)

                # Get the terminals that have never been seen before
                never_seen_terminals = list(terminal_diff)
                seen_terminals = list(scraper_terminal_set - terminal_diff)

            if len(terminals_from_db) > len(scraped_terminals):
                logging.error("Scraper missed terminals: %s", diff_names)
                return False
        else:
            logging.info("No discrepancies found between the database and scraper.")
            seen_terminals = scraped_terminals

        tz_finder = TerminalTzFinder()

        # Must add timezone to terminals that have never been seen before
        for terminal in never_seen_terminals:
            if not terminal.location:
                logging.error("Terminal %s has no location.", terminal.name)
                continue

            terminal.timezone = tz_finder.get_timezone(terminal.location)
            self.upsert_terminal_info(terminal)

        terminals_to_update = []

        # Sort the terminals by name to ensure that the order is the same
        # and that the terminals are being compared correctly
        terminals_from_db.sort(key=lambda x: x.name)
        seen_terminals.sort(key=lambda x: x.name)

        # Check to see if the terminals have had any changes
        for db_terminal, scraped_terminal in zip(
            terminals_from_db, seen_terminals, strict=True
        ):
            # This needs to be here because scraped terminal objects
            # do not have the timezone attribute set. So, by setting
            # the same timezone as the database terminal, we can compare
            # the two terminals and prevent treating the empty terminal
            # attribute as an update.
            scraped_terminal.timezone = db_terminal.timezone

            # If the terminals are not the same, then we need to update the database
            if db_terminal != scraped_terminal:
                # If the location has changed, then we need to update the timezone
                if db_terminal.location != scraped_terminal.location:
                    logging.info(
                        "Terminal %s has changed location from %s to %s.",
                        db_terminal.name,
                        db_terminal.location,
                        scraped_terminal.location,
                    )

                    if not scraped_terminal.location:
                        logging.error(
                            "Terminal %s has no location.", scraped_terminal.name
                        )
                        continue

                    scraped_terminal.timezone = tz_finder.get_timezone(
                        scraped_terminal.location
                    )

                terminals_to_update.append(scraped_terminal)

        if not terminals_to_update:
            logging.info("No terminals need to be updated.")

        # Upsert the updated terminals
        for terminal in terminals_to_update:
            self.upsert_terminal_info(terminal)

        # Return true if there are terminals that need to be updated
        # or added to the database
        return bool(never_seen_terminals or terminals_to_update)

    def delete_collection(
        self: "FirestoreClient", collection_name: str, batch_size: int = 10
    ) -> Optional[bool]:
        """Delete all documents in a Firestore collection.

        Args:
        ----
            collection_name (str): Name of the Firestore collection to delete.
            batch_size (int): The size of the batch to use for deleting documents.

        Returns:
        -------
            Optional[bool]: Returns True if the operation is successful, None otherwise.
        """
        coll_ref = self.db.collection(collection_name)
        return self._delete_collection_batch(coll_ref, batch_size)

    def _delete_collection_batch(
        self: "FirestoreClient",
        coll_ref: firestore.CollectionReference,
        batch_size: int,
    ) -> Optional[bool]:
        docs = coll_ref.limit(batch_size).stream()
        deleted = 0

        for doc in docs:
            print(f"Deleting doc {doc.id} => {doc.to_dict()}")
            doc.reference.delete()
            deleted += 1

        if deleted >= batch_size:
            return self._delete_collection_batch(coll_ref, batch_size)

        return True
