import logging
import os
import threading
from datetime import datetime
from functools import partial
from typing import Any, Callable, Dict, List, Optional
from uuid import uuid4
from time import sleep

from firebase_admin import credentials, firestore, initialize_app  # type: ignore
from google.cloud.firestore import (  # type: ignore
    DocumentReference,
    DocumentSnapshot,
    Transaction,
)

from location_tz import TerminalTzFinder
from pdf import Pdf
from scraper_utils import is_valid_sha256
from terminal import Terminal

# Global or shared event object
terminal_lock_change_event = threading.Event()


def attribute_update_callback(
    attribute_name: str, event: threading.Event
) -> Callable[[Dict[str, Any]], None]:
    """Create a callback function that signals an event when a specific attribute changes.

    Args:
    ----
        attribute_name (str): The name of the attribute to watch for changes.
        event (threading.Event): The event to signal when the attribute changes.

    Returns:
    -------
        Callable[[Dict[str, Any]], None]: The callback function that signals the event when the attribute changes.

    """

    def callback(changed_attributes: Dict[str, Any]) -> None:
        if attribute_name in changed_attributes:
            new_value = changed_attributes[attribute_name]
            logging.info(
                "%s status changed to: %s.",
                attribute_name,
                new_value,
            )
            event.set()  # Signal that the attribute has changed
        else:
            logging.info("No change detected in %s.", attribute_name)

    return callback


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

    def get_document(
        self: "FirestoreClient", collection_name: str, document_id: str
    ) -> Optional[Dict[str, Any]]:
        """Fetch a document from Firestore.

        Args:
        ----
            collection_name: The name of the collection where the document is stored.
            document_id: The ID of the document to retrieve.

        Returns:
        -------
            The document data as a dictionary if the document exists, otherwise None.

        """
        # Create a reference to the document
        doc_ref = self.db.collection(collection_name).document(document_id)

        # Attempt to fetch the document
        doc = doc_ref.get()

        # Check if the document exists
        if doc.exists:
            print("Document data:", doc.to_dict())
            return doc.to_dict()

        print("No such document!")
        return None

    def _on_snapshot(  # noqa: PLR0913
        self: "FirestoreClient",
        attributes: List[str],
        callback: Callable[[Dict[str, Any]], None],
        doc_snapshot: List[DocumentSnapshot],
        changes: List[Any],
        read_time: datetime,
    ) -> None:
        """Handle real-time updates to documents in a generic manner.

        Args:
        ----
            attributes (List[str]): List of document attributes to track.
            callback (Callable[[Dict[str, any]], None]): Callback function to execute with the changed attributes.
            doc_snapshot (List[DocumentSnapshot]): The snapshot of the document.
            changes (List[DocumentChange]): A list of changes made to the document.
            read_time (datetime): The time of the read.

        """
        for change in changes:
            if change.type.name == "MODIFIED":
                logging.info("Document modified.")
                changed_data = change.document.to_dict()
                changed_attributes = {
                    attr: changed_data[attr]
                    for attr in attributes
                    if attr in changed_data
                }
                if changed_attributes:
                    callback(changed_attributes)
            elif change.type.name == "ADDED":
                logging.info("Document added.")
            elif change.type.name == "REMOVED":
                logging.info("Document removed.")

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
                "contactInfo": terminal.contact_info,
                "contactInfoHash": terminal.contact_info_hash,
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
            scraper_terminal_set = {terminal.name for terminal in scraped_terminals}
            old_terminal_set = {terminal.name for terminal in terminals_from_db}

            # Get the difference between the two sets
            diff_terminal_names = scraper_terminal_set - old_terminal_set

            if len(scraped_terminals) > len(terminals_from_db):
                logging.info("New terminals found: %s", diff_terminal_names)

                # Get the terminals that have never been seen before
                for terminal in scraped_terminals:
                    if terminal.name in diff_terminal_names:
                        never_seen_terminals.append(terminal)

                # Get the terminals that have been seen before
                for terminal in terminals_from_db:
                    if terminal.name not in diff_terminal_names:
                        seen_terminals.append(terminal)

            if len(terminals_from_db) > len(scraped_terminals):
                logging.critical("Scraper missed terminals: %s", diff_terminal_names)
                return False
        else:
            logging.info("No discrepancies found between the database and scraper.")
            seen_terminals = scraped_terminals

        if not never_seen_terminals:
            logging.info("No new terminals found.")

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

    def acquire_terminal_coll_update_lock(self: "FirestoreClient") -> bool:
        """Atomically acquire the terminal update lock.

        Returns
        -------
            bool: True if the lock was successfully acquired, False otherwise.

        """
        lock_coll = os.getenv("LOCK_COLL", "Locks")
        lock_doc_ref = self.db.collection(lock_coll).document("terminal_update_lock")

        @firestore.transactional
        def update_in_transaction(
            transaction: Transaction,
            doc_ref: DocumentReference,
        ) -> bool:
            """Update a document in a Firestore transaction.

            Args:
            ----
                transaction (Transaction): The Firestore transaction object.
                doc_ref (DocumentReference): The reference to the document to update.

            Returns:
            -------
                bool: True if the lock was successfully acquired, False otherwise.

            """
            snapshot = doc_ref.get(transaction=transaction)
            new_lock_state = not snapshot.exists or not snapshot.get("lock")

            if new_lock_state:
                if snapshot.exists:
                    # Update the existing document
                    transaction.update(doc_ref, {"lock": True})
                else:
                    # Create the document if it does not exist
                    transaction.set(doc_ref, {"lock": True})

            return new_lock_state

        # Execute the transaction
        try:
            transaction = self.db.transaction()
            return update_in_transaction(transaction, lock_doc_ref)
        except Exception as e:
            logging.warning("Failed to acquire terminal update lock: %s", e)
            return False

    def acquire_terminal_doc_update_lock(
        self: "FirestoreClient", terminal_name: str
    ) -> bool:
        """Atomically acquire a lock for updating a single terminal document.

        Args:
        ----
            terminal_name (str): The name of the terminal to acquire the lock for.

        Returns:
        -------
            bool: True if the lock was successfully acquired, False otherwise.

        """
        lock_coll = os.getenv("TERMINAL_COLL", "Terminals")
        lock_doc_ref = self.db.collection(lock_coll).document(terminal_name)

        # Define the transactional operation for acquiring the lock
        @firestore.transactional
        def update_in_transaction(transaction, doc_ref) -> bool:  # noqa: ANN001
            snapshot = doc_ref.get(transaction=transaction)
            if snapshot.exists:
                current_lock_state = snapshot.get("pdfUpdateLock")
                if not current_lock_state:
                    transaction.update(doc_ref, {"pdfUpdateLock": True})
                    return True
            return False

        # Execute the transaction
        try:
            transaction = self.db.transaction()
            result = update_in_transaction(transaction, lock_doc_ref)
            if result:
                logging.info(
                    "Successfully acquired update lock for terminal '%s'.",
                    terminal_name,
                )
            else:
                logging.warning(
                    "Failed to acquire update lock for terminal '%s' because it's already locked.",
                    terminal_name,
                )
            return result
        except Exception as e:
            logging.warning(
                "Failed to acquire terminal update lock for '%s': %s", terminal_name, e
            )
            return False

    def set_terminal_update_lock_timestamp(self: "FirestoreClient") -> bool:
        """Add a timestamp to the terminal_update_lock document."""
        lock_coll = os.getenv("LOCK_COLL", "Locks")

        lock_doc_ref = self.db.collection(lock_coll).document("terminal_update_lock")

        # Check if the document exists
        if not lock_doc_ref.get().exists:
            logging.error("Terminal update lock document does not exist.")
            return False

        update_data = {"timestamp": firestore.SERVER_TIMESTAMP}

        try:
            # Perform a non-transactional update to add the timestamp
            lock_doc_ref.set(update_data, merge=True)
            logging.info("Added timestamp to terminal update lock.")
            return True

        except Exception as e:
            logging.error("Failed to add timestamp to terminal update lock: %s", e)
            return False

    def get_terminal_update_lock_timestamp(
        self: "FirestoreClient",
    ) -> Optional[datetime]:
        """Get the timestamp from the terminal update lock document.

        Returns
        -------
            Optional[datetime]: The timestamp, or None if the document does not exist

        """
        lock_coll = os.getenv("LOCK_COLL", "Locks")
        lock_doc_ref = self.db.collection(lock_coll).document("terminal_update_lock")

        doc = lock_doc_ref.get()

        if doc.exists:

            try:
                return doc.get("timestamp")
            except KeyError:
                logging.error("Timestamp not found in terminal update lock document.")
                return None

        return None

    def add_termimal_update_fingerprint(self: "FirestoreClient") -> None:
        """Add a fingerprint to the terminal update lock document.

        This will be used in conjunction with terminal names to sign off pdf update job
        completion. This can be used to check if the terminal update job was completed
        in the current run of the program or a previous run.
        """
        lock_coll = os.getenv("LOCK_COLL", "Locks")  # Use a default value or from env
        lock_doc_ref = self.db.collection(lock_coll).document("terminal_update_lock")

        update_data = {"fingerprint": str(uuid4())}

        try:
            # Perform a non-transactional update to release the lock
            lock_doc_ref.set(update_data, merge=True)
            logging.info("Added fingerprint to terminal update lock.")
        except Exception as e:
            logging.error("Failed to add fingerprint to terminal update lock: %s", e)

    def get_terminal_update_fingerprint(self: "FirestoreClient") -> Optional[str]:
        """Get the fingerprint from the terminal update lock document.

        Returns
        -------
            Optional[str]: The fingerprint, or None if the document does not exist

        """
        lock_coll = os.getenv("LOCK_COLL", "Locks")
        lock_doc_ref = self.db.collection(lock_coll).document("terminal_update_lock")

        doc = lock_doc_ref.get()

        if doc.exists:
            return doc.get("fingerprint")

        return None

    def release_terminal_lock(self: "FirestoreClient") -> None:
        """Release the terminal update lock.

        This function sets the lock attribute to False, effectively releasing the lock.
        """
        lock_coll = os.getenv("LOCK_COLL", "Locks")  # Use a default value or from env
        lock_doc_ref = self.db.collection(lock_coll).document("terminal_update_lock")

        # Update the lock attribute to False
        update_data = {"lock": False}

        try:
            # Perform a non-transactional update to release the lock
            lock_doc_ref.set(update_data, merge=True)
            logging.info("Terminal update lock released.")
        except Exception as e:
            logging.error("Failed to release terminal update lock: %s", e)

    def watch_terminal_update_lock(self: "FirestoreClient") -> None:
        """Watch the terminal update lock for changes in Firestore."""
        lock_coll = os.getenv("LOCK_COLL", "Locks")
        document_id = "terminal_update_lock"
        attribute_to_watch = "lock"

        # Pass the event object to the callback function
        callback = attribute_update_callback(
            attribute_to_watch, terminal_lock_change_event
        )

        # Define a partial function for the _on_snapshot with pre-defined attributes and callback
        on_snapshot_callback = partial(
            self._on_snapshot, [attribute_to_watch], callback
        )

        # Get a reference to the document
        doc_ref: DocumentReference = self.db.collection(lock_coll).document(document_id)

        # Set up the listener
        try:
            doc_watch = doc_ref.on_snapshot(on_snapshot_callback)  # noqa: F841
            logging.info(
                "Started watching '%s' for changes in '%s' collection.",
                document_id,
                lock_coll,
            )
        except Exception as e:
            logging.error("Failed to set up watch on '%s': %s", document_id, e)

    def release_terminal_doc_lock(self: "FirestoreClient", terminal_name: str) -> None:
        """Release the lock for updating a single terminal document.

        Args:
        ----
            terminal_name (str): The name of the terminal to release the lock for.

        """
        lock_coll = os.getenv("TERMINAL_COLL", "Terminals")
        lock_doc_ref = self.db.collection(lock_coll).document(terminal_name)

        # Check if the document exists
        if not lock_doc_ref.get().exists:
            logging.error(
                "Cannot release lock for non-existent terminal '%s'.", terminal_name
            )
            return

        # Update the lock attribute to False
        update_data = {"pdfUpdateLock": False}

        retry = 0
        while retry < 5:
            try:
                # Perform a non-transactional update to release the lock
                lock_doc_ref.set(update_data, merge=True)
                logging.info("Released update lock for terminal '%s'.", terminal_name)
                return
            except Exception as e:
                logging.warning(
                    "Failed to release update lock for terminal '%s': %s",
                    terminal_name,
                    e,
                )
                retry += 1
                sleep(1 * retry)

        logging.critical(
            "Attemped %d retries. Failed to release update lock for terminal '%s'.",
            retry,
            terminal_name,
        )

    def get_terminal_update_signature(
        self: "FirestoreClient", terminal_name: str
    ) -> Optional[str]:
        """Get the fingerprint from the terminal update lock document.

        Args:
        ----
            terminal_name (str): The name of the terminal to get the fingerprint for.

        Returns:
        -------
            Optional[str]: The fingerprint, or None if the document does not exist

        """
        lock_coll = os.getenv("TERMINAL_COLL", "Terminals")
        lock_doc_ref = self.db.collection(lock_coll).document(terminal_name)

        doc = lock_doc_ref.get()

        if doc.exists:
            return doc.get("pdfUpdateSignature")

        return None

    def set_terminal_update_signature(
        self: "FirestoreClient", terminal_name: str, signature: str
    ) -> None:
        """Set the fingerprint for the terminal update lock document.

        Args:
        ----
            terminal_name (str): The name of the terminal to set the fingerprint for.
            signature (str): The fingerprint to set.

        """
        lock_coll = os.getenv("TERMINAL_COLL", "Terminals")
        lock_doc_ref = self.db.collection(lock_coll).document(terminal_name)

        # Check if the document exists
        if not lock_doc_ref.get().exists:
            logging.error(
                "Cannot set fingerprint for non-existent terminal '%s'.", terminal_name
            )
            return

        update_data = {"pdfUpdateSignature": signature}

        retry = 0
        while retry < 5:
            try:
                # Perform a non-transactional update to set the fingerprint
                lock_doc_ref.set(update_data, merge=True)
                logging.info(
                    "Set fingerprint for terminal '%s' to '%s'.",
                    terminal_name,
                    signature,
                )
                return
            except Exception as e:
                logging.warning(
                    "Failed to set fingerprint for terminal '%s': %s", terminal_name, e
                )
                retry += 1
                sleep(1 * retry)

        logging.error(  # Not critical because if the signature is not set, the terminal will be updated again in the worst case.
            "Attemped %d retries. Failed to set fingerprint for terminal '%s'.",
            retry,
            terminal_name,
        )

    def safely_release_terminal_lock(self: "FirestoreClient") -> None:
        """Safely release the terminal update lock by flipping the lock state twice.

        This ensures that other instances see a change.
        """
        try:
            self.release_terminal_lock()

            # Then, acquire the lock to signal a change to other instances.
            self.acquire_terminal_coll_update_lock()

        except Exception as e:
            logging.error("Failed to safely release terminal update lock: %s", e)

        # Finally, release the lock as originally intended.
        self.release_terminal_lock()

    def get_terminal_coll_update_lock_value(self: "FirestoreClient") -> bool:
        """Get the terminal update lock value.

        Returns
        -------
            bool: True if the lock was successfully acquired, False otherwise.

        """
        lock_status = self.acquire_terminal_coll_update_lock()

        if lock_status:
            self.safely_release_terminal_lock()

        # Acquire lock = True means the lock was successfully acquired
        # and the lock is currently was set to False.
        return not lock_status

    def wait_for_terminal_lock_change(self: "FirestoreClient") -> None:
        """Wait for the terminal_update_lock 'lock' attribute to change.

        First, we wait only for 7 seconds and then check if the lock was already false. This
        can happen if the lock was acquired by another instance and then released before we
        could watch for changes. If the lock was already false, we can stop waiting. If not,
        we did not miss the change and can continue waiting.
        """
        logging.info("Waiting for terminal_update_lock 'lock' attribute to change...")

        # Initial wait for 7 seconds to catch quick changes
        event_set = terminal_lock_change_event.wait(timeout=7)
        if not event_set:
            logging.info("Initial 7 seconds wait completed without event being set.")
            # Check if the lock was already released
            if not self.get_terminal_coll_update_lock_value():
                logging.info(
                    "terminal_update_lock 'lock' was already false, no need to wait further."
                )
                return

            logging.info("Lock still acquired, continuing to wait for up to 3 minutes.")

        # Continue waiting for the lock to change for up to 3 minutes
        terminal_lock_change_event.wait(
            timeout=180
        )  # Extend wait to 3 minutes if necessary
        logging.info("terminal_update_lock 'lock' attribute changed!")

        # Reset the event for future waits
        terminal_lock_change_event.clear()

    def set_terminal_last_check_timestamp(
        self: "FirestoreClient", terminal_name: str
    ) -> None:
        """Add a timestamp to the terminal document of the last check.

        Args:
        ----
            terminal_name (str): The name of the terminal to add the timestamp to.

        Returns:
        -------
            None

        """
        terminal_coll = os.getenv("TERMINAL_COLL")

        if not terminal_coll:
            logging.error("Terminal collection name not found in enviroment variables.")
            return

        doc_ref = self.db.collection(terminal_coll).document(terminal_name)

        # Check if the document exists
        if not doc_ref.get().exists:
            logging.error(
                "Cannot add timestamp to non-existent terminal '%s'.", terminal_name
            )
            return

        update_data = {"lastCheckTimestamp": firestore.SERVER_TIMESTAMP}

        retry = 0
        while retry < 5:
            try:
                # Perform a non-transactional update to add the timestamp
                doc_ref.update(update_data)
                logging.info(
                    "Added timestamp to terminal '%s' last check.", terminal_name
                )
                return
            except Exception as e:
                logging.warning(
                    "Failed to add timestamp to terminal '%s' last check: %s",
                    terminal_name,
                    e,
                )
                retry += 1
                sleep(1 * retry)

        logging.error(  # Not critical because if the timestamp is not set, the terminal will be incorrect until the next run (~10 minutes).
            "Attemped %d retries. Failed to add timestamp to terminal '%s' last check.",
            retry,
            terminal_name,
        )

    def set_pdf_last_update_timestamp(
        self: "FirestoreClient", terminal_name: str, pdf_type: str
    ) -> None:
        """Add a timestamp to the terminal document of the last update for a specific PDF type.

        Args:
        ----
            terminal_name (str): The name of the terminal to add the timestamp to.
            pdf_type (str): The type of the PDF to add the timestamp to.

        Returns:
        -------
            None

        """
        terminal_coll = os.getenv("TERMINAL_COLL")

        if not terminal_coll:
            logging.error("Terminal collection name not found in enviroment variables.")
            return

        valid_pdf_types = ["72_HR", "30_DAY", "ROLLCALL"]

        if pdf_type not in valid_pdf_types:
            logging.error("Invalid PDF type: %s", pdf_type)
            return

        doc_ref = self.db.collection(terminal_coll).document(terminal_name)

        # Check if the document exists
        if not doc_ref.get().exists:
            logging.error(
                "Cannot add pdf update timestamp to non-existent terminal '%s'.",
                terminal_name,
            )
            return

        if pdf_type == "72_HR":
            pdf_type = "72Hour"
        elif pdf_type == "30_DAY":
            pdf_type = "30Day"
        elif pdf_type == "ROLLCALL":
            pdf_type = "Rollcall"
        else:
            logging.error(
                "Cannot set pdf update timestamp. Invalid PDF type: %s", pdf_type
            )
            return

        update_data = {f"last{pdf_type}UpdateTimestamp": firestore.SERVER_TIMESTAMP}

        retry = 0
        while retry < 5:
            try:
                # Perform a non-transactional update to add the timestamp
                doc_ref.update(update_data)
                logging.info(
                    "Added timestamp to terminal '%s' last %s update.",
                    terminal_name,
                    pdf_type,
                )
                return
            except Exception as e:
                logging.warning(
                    "Failed to add pdf update timestamp to terminal '%s' last %s update: %s",
                    terminal_name,
                    pdf_type,
                    e,
                )
                retry += 1
                sleep(1 * retry)

        logging.error(  # Not critical because if the timestamp is not set, the terminal will be incorrect until the next run (~10 minutes).
            "Attemped %d retries. Failed to add pdf update timestamp to terminal '%s' last %s update.",
            retry,
            terminal_name,
            pdf_type,
        )

    def set_terminal_update_status(
        self: "FirestoreClient", terminal_name: str, status: str
    ) -> None:
        """Set the update status for a terminal document.

        Valid statuses are: "UPDATING", "SUCCESS", "FAILED".

        Args:
        ----
            terminal_name (str): The name of the terminal to set the update status for.
            status (str): The status to set.

        Returns:
        -------
            None

        """
        status = status.upper()
        valid_statuses = ["UPDATING", "SUCCESS", "FAILED"]

        if status not in valid_statuses:
            logging.error("Invalid status: %s", status)
            return

        terminal_coll = os.getenv("TERMINAL_COLL")

        if not terminal_coll:
            logging.error("Terminal collection name not found in enviroment variables.")
            return

        doc_ref = self.db.collection(terminal_coll).document(terminal_name)

        # Check if the document exists
        if not doc_ref.get().exists:
            logging.error(
                "Cannot set update status for non-existent terminal '%s'.",
                terminal_name,
            )
            return

        update_data = {"updateStatus": status}

        retry = 0
        while retry < 5:
            try:
                # Perform a non-transactional update to set the status
                doc_ref.update(update_data)
                logging.info(
                    "Set update status for terminal '%s' to '%s'.",
                    terminal_name,
                    status,
                )
                return
            except Exception as e:
                logging.warning(
                    "Failed to set update status for terminal '%s': %s",
                    terminal_name,
                    e,
                )
                retry += 1
                sleep(1 * retry)

        logging.error(  # Not critical because if the status is not set, the terminal will be incorrect until the next run (~10 minutes).
            "Attemped %d retries. Failed to set update status for terminal '%s'.",
            retry,
            terminal_name,
        )
