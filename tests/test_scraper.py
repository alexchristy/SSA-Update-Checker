import datetime as dt
import json
import os
import pickle
import sys
import time
import unittest
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta
from random import uniform
from typing import List
from unittest.mock import MagicMock, patch
from zoneinfo import ZoneInfo

from firestoredb import FirestoreClient
from s3_bucket import S3Bucket
from terminal import Terminal

current_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.append(current_dir + "/../")

from scraper import (  # noqa: E402 (Need to import after adding to path)
    get_active_terminals,
    update_db_terminals,
    update_terminal_pdfs,
)


class TestGetActiveTerminals(unittest.TestCase):
    """Test the get_active_terminals function."""

    def setUp(self: "TestGetActiveTerminals") -> None:
        """Set up the test cases for TestGetActiveTerminals."""
        file_path = os.path.join(
            current_dir, "TestGetActiveTerminals_Assets/AMC_Home_Page_12-16-23.pkl"
        )

        # Load the serialized response
        with open(
            file_path,
            "rb",
        ) as file:
            self.serialized_response = file.read()

    @patch("scraper.scraper_utils.get_with_retry")
    def test_active_terminals(
        self: "TestGetActiveTerminals", mock_get_with_retry: MagicMock
    ) -> None:
        """Test that the function returns a list of active terminals."""
        # Deserialize the response
        response_data = pickle.loads(  # noqa: S301 (Loading test data)
            self.serialized_response
        )

        mock_response = unittest.mock.Mock()
        mock_response.configure_mock(**response_data)

        mock_get_with_retry.return_value = mock_response

        result = get_active_terminals("https://www.amc.af.mil/AMC-Travel-Site/")

        self.assertIsInstance(result, list)
        self.assertEqual(len(result), 40)

        array_path = os.path.join(
            current_dir, "TestGetActiveTerminals_Assets/terminal_names.json"
        )
        # Load the expected terminal_names array
        with open(array_path, "r") as file:
            terminal_names = json.load(file)

        parsed_terminal_names = [terminal.name for terminal in result]

        self.assertListEqual(parsed_terminal_names, terminal_names)

    @patch("scraper.scraper_utils.get_with_retry")
    def test_active_terminals_none_response(
        self: "TestGetActiveTerminals", mock_get_with_retry: MagicMock
    ) -> None:
        """Test that the function returns an empty list when the response is None."""
        mock_response = unittest.mock.Mock()
        mock_response.configure_mock(**{"content": None})

        mock_get_with_retry.return_value = mock_response

        with self.assertRaises(SystemExit):
            get_active_terminals("https://www.amc.af.mil/AMC-Travel-Site/")

    @patch("scraper.scraper_utils.get_with_retry")
    def test_active_terminals_empty_response(
        self: "TestGetActiveTerminals", mock_get_with_retry: MagicMock
    ) -> None:
        """Test that the function returns an empty list when the response is None."""
        mock_response = unittest.mock.Mock()
        mock_response.configure_mock(**{"content": ""})

        mock_get_with_retry.return_value = mock_response

        with self.assertRaises(SystemExit):
            get_active_terminals("https://www.amc.af.mil/AMC-Travel-Site/")


class TestUpdateTerminalCollParallel(unittest.TestCase):
    """Test that the update_db_temrinals works in parallel."""

    def setUp(self: "TestUpdateTerminalCollParallel") -> None:
        """Set up the test cases for TestUpdateTerminalCollParallel."""
        file_path = os.path.join(
            current_dir,
            "assets/TestUpdateTerminalCollParallel/AMC_Home_Page_12-16-23.pkl",
        )

        # Load the serialized response
        with open(
            file_path,
            "rb",
        ) as file:
            self.serialized_response = file.read()

        # Create a FirestoreClient object
        # Set collection names
        self.terminal_coll = "**TestUpdateTerminalCollParallel**_Terminals"
        self.pdf_archive_coll = "**TestUpdateTerminalCollParallel**_PDF_Archive"
        self.lock_coll = "**TestUpdateTerminalCollParallel**_Locks"
        self.firestore_cert = "./creds.json"

        os.environ["TERMINAL_COLL"] = self.terminal_coll
        os.environ["PDF_ARCHIVE_COLL"] = self.pdf_archive_coll
        os.environ["LOCK_COLL"] = self.lock_coll
        os.environ["FS_CRED_PATH"] = self.firestore_cert

        self.fs = FirestoreClient()

    @patch("scraper.scraper_utils.get_with_retry")
    def test_update_db_terminals_3_parallel(
        self: "TestUpdateTerminalCollParallel", mock_get_with_retry: MagicMock
    ) -> None:
        """Test that the function only allows one instance of the function to run at a time.

        We will call update_db_terminals with 3 threads and check that one one instance of the
        function updates the database and the other instances wait but do not update the database.

        Additionally, we are checking that the other threads wait and do not update the database after and
        it does not hang indefinitely.
        """
        # Deserialize the response
        response_data = pickle.loads(  # noqa: S301 (Loading test data)
            self.serialized_response
        )

        mock_response = unittest.mock.Mock()
        mock_response.configure_mock(**response_data)

        mock_get_with_retry.return_value = mock_response

        def try_update_db_terminals(fs_client: FirestoreClient) -> bool:
            """Attempt to update the terminals in the database anmd return True if successful."""
            return update_db_terminals(fs_client)

        # Run 2 parallel threads to update the database
        with ThreadPoolExecutor(max_workers=3) as executor:
            futures = [
                executor.submit(try_update_db_terminals, self.fs) for _ in range(3)
            ]
            results = [future.result() for future in as_completed(futures, timeout=150)]

        # Check that only one instance of the function updated the database
        self.assertEqual(results.count(True), 1)
        self.assertEqual(results.count(False), 2)

        result_terminals = self.fs.get_all_terminals()

        self.assertIsInstance(result_terminals, list)
        self.assertEqual(len(result_terminals), 40)

        array_path = os.path.join(
            current_dir,
            "assets/TestUpdateTerminalCollParallel/terminal_names.json",
        )
        # Load the expected terminal_names array
        with open(array_path, "r") as file:
            terminal_names = json.load(file)

        parsed_terminal_names = [terminal.name for terminal in result_terminals]

        self.assertCountEqual(parsed_terminal_names, terminal_names)

        # Ensure that the lock was released
        lock_doc = self.fs.get_document(self.lock_coll, "terminal_update_lock")

        if lock_doc is None:
            self.fail("The lock document should not be exist in database.")

        self.assertFalse(lock_doc.get("lock"), "The lock should have been released.")

    @patch("scraper.scraper_utils.get_with_retry")
    def test_update_db_terminals_parallel_random_starts(
        self: "TestUpdateTerminalCollParallel", mock_get_with_retry: MagicMock
    ) -> None:
        """Test that the function only allows one instance of the function to update the terminals when three workers start randomly.

        We will call update_db_terminals with 3 threads and check that one one instance of the
        function updates the database and the other instances wait but do not update the database.

        Additionally, we are checking that the other threads wait and do not update the database after and
        it does not hang indefinitely.

        Lastly, each worker will start at a random time to test that the function does not hang indefinitely with
        unpredictable start times.
        """
        # Deserialize the response
        response_data = pickle.loads(  # noqa: S301 (Loading test data)
            self.serialized_response
        )

        mock_response = unittest.mock.Mock()
        mock_response.configure_mock(**response_data)

        mock_get_with_retry.return_value = mock_response

        def try_rand_update_db_terminals(fs_client: FirestoreClient) -> bool:
            """Attempt to update the terminals in the database anmd return True if successful.

            Start at a random time between 0 and 5 seconds.
            """
            time.sleep(uniform(0, 5))  # noqa: S311 (not for cryptographic purposes)
            return update_db_terminals(fs_client)

        # Run 3 parallel threads to update the database
        with ThreadPoolExecutor(max_workers=3) as executor:
            futures = [
                executor.submit(try_rand_update_db_terminals, self.fs) for _ in range(3)
            ]
            results = [future.result() for future in as_completed(futures, timeout=150)]

        # Check that only one instance of the function updated the database
        self.assertEqual(results.count(True), 1)
        self.assertEqual(results.count(False), 2)

        result_terminals = self.fs.get_all_terminals()

        self.assertIsInstance(result_terminals, list)
        self.assertEqual(len(result_terminals), 40)

        array_path = os.path.join(
            current_dir,
            "assets/TestUpdateTerminalCollParallel/terminal_names.json",
        )
        # Load the expected terminal_names array
        with open(array_path, "r") as file:
            terminal_names = json.load(file)

        parsed_terminal_names = [terminal.name for terminal in result_terminals]

        self.assertCountEqual(parsed_terminal_names, terminal_names)

        # Ensure that the lock was released
        lock_doc = self.fs.get_document(self.lock_coll, "terminal_update_lock")

        if lock_doc is None:
            self.fail("The lock document should not be exist in database.")

        self.assertFalse(lock_doc.get("lock"), "The lock should have been released.")

    def tearDown(self: "TestUpdateTerminalCollParallel") -> None:
        """Tear down the test cases for TestUpdateTerminalCollParallel."""
        # Delete the test collections
        self.fs.delete_collection(self.terminal_coll)
        self.fs.delete_collection(self.pdf_archive_coll)
        self.fs.delete_collection(self.lock_coll)


class TestUpdateTerminalCollTimingLock(unittest.TestCase):
    """Test that the update_db_terminals will no update terminals if the last update was less than 2 minutes ago."""

    def setUp(self: "TestUpdateTerminalCollTimingLock") -> None:
        """Set up the test cases for TestUpdateTerminalCollTimingLock."""
        file_path = os.path.join(
            current_dir,
            "assets/TestUpdateTerminalCollTimingLock/AMC_Home_Page_12-16-23.pkl",
        )

        # Load the serialized response
        with open(
            file_path,
            "rb",
        ) as file:
            self.serialized_response = file.read()

        # Create a FirestoreClient object
        # Set collection names
        self.terminal_coll = "**TestUpdateTerminalCollTimingLock**_Terminals"
        self.pdf_archive_coll = "**TestUpdateTerminalCollTimingLock**_PDF_Archive"
        self.lock_coll = "**TestUpdateTerminalCollTimingLock**_Locks"
        self.firestore_cert = "./creds.json"

        os.environ["TERMINAL_COLL"] = self.terminal_coll
        os.environ["PDF_ARCHIVE_COLL"] = self.pdf_archive_coll
        os.environ["LOCK_COLL"] = self.lock_coll
        os.environ["FS_CRED_PATH"] = self.firestore_cert

        self.fs = FirestoreClient()

    def insert_terminal_coll_update_lock(
        self: "TestUpdateTerminalCollTimingLock",
        last_update_date: datetime,
        locked: bool = True,
    ) -> bool:
        """Insert a lock into the lock collection to simulate a previous update.

        Args:
        ----
            locked (bool): Whether the lock is locked or not.
            last_update_date (datetime): The date of the last update.

        Returns:
        -------
            bool: True if the lock was inserted successfully.

        """
        terminal_lock_doc = "terminal_update_lock"
        doc_data = {
            "lock": locked,
            "timestamp": last_update_date,
            "fingerprint": "test_fingerprint_TestUpdateTerminalCollTimingLock",
        }

        try:
            self.fs.set_document(self.lock_coll, terminal_lock_doc, doc_data)
        except Exception:
            return False

        return True

    @patch("scraper.scraper_utils.get_with_retry")
    def test_no_update_if_less_than_2_minutes(
        self: "TestUpdateTerminalCollTimingLock", mock_get_with_retry: MagicMock
    ) -> None:
        """Test that the function does not update the terminals if the last update was less than 2 minutes ago."""
        # Deserialize the response
        response_data = pickle.loads(  # noqa: S301 (Loading test data)
            self.serialized_response
        )

        # Set the last update date to 1 minute ago
        last_update_date = datetime.now(tz=dt.UTC) - timedelta(minutes=1)

        # Insert a lock into the lock collection to simulate a previous update
        insert_success = self.insert_terminal_coll_update_lock(
            last_update_date, locked=False
        )

        self.assertTrue(insert_success, "Failed to insert lock into lock collection.")

        # Mock the get_with_retry function
        mock_response = unittest.mock.Mock()
        mock_response.configure_mock(**response_data)

        mock_get_with_retry.return_value = mock_response

        # Run the update_db_terminals function
        result = update_db_terminals(self.fs)

        self.assertFalse(result, "The function should not have updated the terminals.")

        # Ensure that the lock was released
        lock_doc = self.fs.get_document(self.lock_coll, "terminal_update_lock")

        if lock_doc is None:
            self.fail("The lock document should not be exist in database.")

        self.assertFalse(lock_doc.get("lock"), "The lock should have been released.")

    @patch("scraper.scraper_utils.get_with_retry")
    def test_update_if_more_than_2_minutes(
        self: "TestUpdateTerminalCollTimingLock", mock_get_with_retry: MagicMock
    ) -> None:
        """Test that the function updates the terminals if the last update was more than 2 minutes ago."""
        # Deserialize the response
        response_data = pickle.loads(  # noqa: S301 (Loading test data)
            self.serialized_response
        )

        # Set the last update date to 3 minutes ago
        last_update_date = datetime.now(tz=dt.UTC) - timedelta(minutes=3)

        # Insert a lock into the lock collection to simulate a previous update
        insert_success = self.insert_terminal_coll_update_lock(
            last_update_date, locked=False
        )

        self.assertTrue(insert_success, "Failed to insert lock into lock collection.")

        # Mock response with empty list
        mock_response = unittest.mock.Mock()
        mock_response.configure_mock(**response_data)
        mock_get_with_retry.return_value = mock_response

        # Run the update_db_terminals function
        result = update_db_terminals(self.fs)

        self.assertTrue(result, "The function should have updated the terminals.")

        # Ensure that the lock was released
        lock_doc = self.fs.get_document(self.lock_coll, "terminal_update_lock")

        if lock_doc is None:
            self.fail("The lock document should not be exist in database.")

        self.assertFalse(lock_doc.get("lock"), "The lock should have been released.")

        # Ensure that the found terminals are correct
        result_terminals = self.fs.get_all_terminals()

        self.assertIsInstance(result_terminals, list)
        self.assertEqual(len(result_terminals), 40)

        array_path = os.path.join(
            current_dir,
            "assets/TestUpdateTerminalCollParallel/terminal_names.json",
        )
        # Load the expected terminal_names array
        with open(array_path, "r") as file:
            terminal_names = json.load(file)

        parsed_terminal_names = [terminal.name for terminal in result_terminals]

        self.assertCountEqual(parsed_terminal_names, terminal_names)

    @patch("scraper.scraper_utils.get_with_retry")
    def test_update_if_no_timestamp(
        self: "TestUpdateTerminalCollTimingLock", mock_get_with_retry: MagicMock
    ) -> None:
        """Test that the function updates the terminals if the last update timestamp is not present."""
        # Deserialize the response
        response_data = pickle.loads(  # noqa: S301 (Loading test data)
            self.serialized_response
        )

        # Insert a lock into the lock collection that has no timestamp
        self.fs.set_document(
            self.lock_coll,
            "terminal_update_lock",
            {
                "lock": False,
                "fingerprint": "test_fingerprint_TestUpdateTerminalCollTimingLock",
            },
        )

        # Mock response with empty list
        mock_response = unittest.mock.Mock()
        mock_response.configure_mock(**response_data)
        mock_get_with_retry.return_value = mock_response

        # Run the update_db_terminals function
        result = update_db_terminals(self.fs)

        self.assertTrue(result, "The function should have updated the terminals.")

        # Ensure that the lock was released
        lock_doc = self.fs.get_document(self.lock_coll, "terminal_update_lock")

        if lock_doc is None:
            self.fail("The lock document should not be exist in database.")

        self.assertFalse(lock_doc.get("lock"), "The lock should have been released.")

        # Ensure that the found terminals are correct
        result_terminals = self.fs.get_all_terminals()

        self.assertIsInstance(result_terminals, list)
        self.assertEqual(len(result_terminals), 40)

        array_path = os.path.join(
            current_dir,
            "assets/TestUpdateTerminalCollParallel/terminal_names.json",
        )
        # Load the expected terminal_names array
        with open(array_path, "r") as file:
            terminal_names = json.load(file)

        parsed_terminal_names = [terminal.name for terminal in result_terminals]

        self.assertCountEqual(parsed_terminal_names, terminal_names)

    def tearDown(self: "TestUpdateTerminalCollTimingLock") -> None:
        """Tear down the test cases for TestUpdateTerminalCollTimingLock."""
        # Delete the test collections
        self.fs.delete_collection(self.terminal_coll)
        self.fs.delete_collection(self.pdf_archive_coll)
        self.fs.delete_collection(self.lock_coll)


class TestUpdateTerminalCollErrors(unittest.TestCase):
    """Test that the update_db_terminals handles errors correctly by releasing the lock."""

    def setUp(self: "TestUpdateTerminalCollErrors") -> None:
        """Set up the test cases for TestUpdateTerminalCollErrors."""
        # Create a FirestoreClient object
        # Set collection names
        self.terminal_coll = "**TestUpdateTerminalCollErrors**_Terminals"
        self.pdf_archive_coll = "**TestUpdateTerminalCollErrors**_PDF_Archive"
        self.lock_coll = "**TestUpdateTerminalCollErrors**_Locks"
        self.firestore_cert = "./creds.json"

        os.environ["TERMINAL_COLL"] = self.terminal_coll
        os.environ["PDF_ARCHIVE_COLL"] = self.pdf_archive_coll
        os.environ["LOCK_COLL"] = self.lock_coll
        os.environ["FS_CRED_PATH"] = self.firestore_cert

        self.fs = FirestoreClient()

    @patch("scraper.scraper_utils.get_with_retry")
    def test_no_terminals_found(
        self: "TestUpdateTerminalCollErrors", mock_get_with_retry: MagicMock
    ) -> None:
        """Test that the function releases the lock if no terminals are found."""
        # Mock response with empty list
        mock_response = unittest.mock.Mock()
        mock_response.configure_mock(**{"content": "No terminals found"})
        mock_get_with_retry.return_value = mock_response

        # Run the update_db_terminals function
        result = update_db_terminals(self.fs)

        self.assertFalse(result, "The function should not have updated the terminals.")

        # Ensure that the lock was released
        lock_doc = self.fs.get_document(self.lock_coll, "terminal_update_lock")

        if lock_doc is None:
            self.fail("The lock document should not be exist in database.")

        self.assertFalse(lock_doc.get("lock"), "The lock should have been released.")

    def tearDown(self: "TestUpdateTerminalCollErrors") -> None:
        """Tear down the test cases for TestUpdateTerminalCollErrors."""
        # Delete the test collections
        self.fs.delete_collection(self.terminal_coll)
        self.fs.delete_collection(self.pdf_archive_coll)
        self.fs.delete_collection(self.lock_coll)


class TestUpdateTerminalPdfs(unittest.TestCase):
    """Test the update_terminal_pdfs function."""

    def setUp(self: "TestUpdateTerminalPdfs") -> None:
        """Set up the test cases for TestUpdateTerminalPdfs."""
        # Create a FirestoreClient object
        # Set collection names
        self.terminal_coll = "**TestUpdateTerminalPdfs**_Terminals"
        self.pdf_archive_coll = "**TestUpdateTerminalPdfs**_PDF_Archive"
        self.lock_coll = "**TestUpdateTerminalPdfs**_Locks"
        self.firestore_cert = "./creds.json"

        os.environ["TERMINAL_COLL"] = self.terminal_coll
        os.environ["PDF_ARCHIVE_COLL"] = self.pdf_archive_coll
        os.environ["LOCK_COLL"] = self.lock_coll
        os.environ["FS_CRED_PATH"] = self.firestore_cert

        self.fs = FirestoreClient()
        self.s3 = S3Bucket()

        # Load the serialized response
        with open(
            "tests/assets/TestUpdateTerminalPdfs/bwi_page_02-17-2024.pkl",
            "rb",
        ) as file:
            self.bwi_page = pickle.load(file)  # noqa: S301 (Loading test data)

        with open(
            "tests/assets/TestUpdateTerminalPdfs/dover_page_02-17-24_NO_PDFS.pkl",
            "rb",
        ) as file:
            self.dover_page_no_pdfs = pickle.load(  # noqa: S301 (Loading test data)
                file
            )

        with open(
            "tests/assets/TestUpdateTerminalPdfs/andrews_page_02-17-24_NO_PDFS.pkl",
            "rb",
        ) as file:
            self.andrews_page_no_pdfs = pickle.load(  # noqa: S301 (Loading test data)
                file
            )

        with open(
            "tests/assets/TestUpdateTerminalPdfs/seattle_page_02-17-24_NO_PDFS.pkl",
            "rb",
        ) as file:
            self.seattle_page_no_pdfs = pickle.load(  # noqa: S301 (Loading test data)
                file
            )

        with open(
            "tests/assets/TestUpdateTerminalPdfs/charleston_page_02-17-24_NO_PDFS.pkl",
            "rb",
        ) as file:
            self.charleston_page_no_pdfs = pickle.load(
                file
            )  # noqa: S301 (Loading test data)

    @patch("scraper.scraper_utils.get_with_retry")
    def test_update_terminal_fail_unlocked(
        self: "TestUpdateTerminalPdfs", mock_get_with_retry: MagicMock
    ) -> None:
        """Test that the function unlocks the terminal if it fails to update the pdfs."""
        # Mock response with empty list
        mock_response = unittest.mock.Mock()
        mock_response.configure_mock(**self.bwi_page)
        mock_get_with_retry.return_value = mock_response

        terminal_name = "Travis AFB Passenger Terminal"

        # Insert a terminal into the terminal collection
        terminal_data = {
            "name": terminal_name,
            "link": "https://www.amc.af.mil/AMC-Travel-Site/Terminals/CONUS-Terminals/Travis-AFB-Passenger-Terminal/",
            "group": "AMC CONUS TERMINALS",
            "location": "Travis AFB, CA",
            "pagePosition": 0,
            "pdf30DayHash": "TestUpdateTerminalPdfs_test_hash",
            "pdf72HourHash": "TestUpdateTerminalPdfs_test_hash",
            "pdfRollcallHash": "TestUpdateTerminalPdfs_test_hash",
            "pdfUpdateLock": False,
            "pdfUpdateSignature": "test_signature",
            "timezone": "America/Los_Angeles",
            "updateStatus": "test_status",
        }

        test_terminal = Terminal.from_dict(terminal_data)

        # Insert the terminal into the terminal collection
        self.fs.set_document(
            self.terminal_coll, "Travis AFB Passenger Terminal", terminal_data
        )

        # Run the update_terminal_pdfs function
        result = update_terminal_pdfs(
            fs=self.fs,
            s3=self.s3,
            terminal=test_terminal,
            update_fingerprint="diff_string_than_test_terminal",
            num_pdfs_updated=0,
            terminals_updated=[],
            terminals_checked=[],
        )

        self.assertFalse(
            result,
            "The function should fail since it can't find a pdf with the given hash.",
        )

        # Ensure that the lock was released
        terminal_doc = self.fs.get_document(self.terminal_coll, terminal_name)

        if terminal_doc is None:
            self.fail("The terminal document should not be exist in database.")

        # Ensure that the lock was released
        self.assertFalse(
            terminal_doc.get("pdfUpdateLock"), "The lock should have been released."
        )

        # Ensure that the update signature was not updated
        self.assertNotEqual(
            terminal_doc.get("pdfUpdateSignature"),
            "diff_string_than_test_terminal",
            "The update signature should have been updated.",
        )

        # Ensure that the update status was updated to FAILED
        self.assertEqual(
            terminal_doc.get("updateStatus"),
            "FAILED",
            "The update status should be failed.",
        )

    @patch("scraper.scraper_utils.get_with_retry")
    def test_update_terminal_unlock_after_completion(
        self: "TestUpdateTerminalPdfs", mock_get_with_retry: MagicMock
    ) -> None:
        """Test that the function unlocks the terminal after it completes successfully."""
        # Mock response with empty list
        mock_response = unittest.mock.Mock()
        mock_response.configure_mock(**self.dover_page_no_pdfs)
        mock_get_with_retry.return_value = mock_response

        terminal_name = "Dover AFB Passenger Terminal"

        # Insert a terminal into the terminal collection
        terminal_data = {
            "name": terminal_name,
            "link": "https://www.amc.af.mil/AMC-Travel-Site/Terminals/CONUS-Terminals/Dover-AFB-Passenger-Terminal/",
            "group": "AMC CONUS TERMINALS",
            "location": "The Moon, Moon Base Alpha",
            "pagePosition": 0,
            "pdf30DayHash": "TestUpdateTerminalPdfs_test_hash",
            "pdf72HourHash": "TestUpdateTerminalPdfs_test_hash",
            "pdfRollcallHash": "TestUpdateTerminalPdfs_test_hash",
            "pdfUpdateLock": False,
            "pdfUpdateSignature": "test_signature",
            "timezone": "America/Moon_Base_Alpha",
            "updateStatus": "test_status",
        }

        test_terminal = Terminal.from_dict(terminal_data)

        # Insert the terminal into the terminal collection
        self.fs.set_document(self.terminal_coll, terminal_name, terminal_data)

        # Run the update_terminal_pdfs function
        result = update_terminal_pdfs(
            fs=self.fs,
            s3=self.s3,
            terminal=test_terminal,
            update_fingerprint="diff_string_than_test_terminal",
            num_pdfs_updated=0,
            terminals_updated=[],
            terminals_checked=[],
        )

        self.assertTrue(
            result,
            "The function should fail since it can't find a pdf with the given hash.",
        )

        # Ensure that the lock was released
        terminal_doc = self.fs.get_document(self.terminal_coll, terminal_name)

        if terminal_doc is None:
            self.fail("The terminal document should not be exist in database.")

        # Ensure that the lock was released
        self.assertFalse(
            terminal_doc.get("pdfUpdateLock"), "The lock should have been released."
        )

        # Ensure that the update signature was not updated
        self.assertEqual(
            terminal_doc.get("pdfUpdateSignature"),
            "diff_string_than_test_terminal",
            "The update signature should have been updated.",
        )

        # Ensure that the update status was updated to SUCCESS
        self.assertEqual(
            terminal_doc.get("updateStatus"),
            "SUCCESS",
            "The update status should be SUCCESS.",
        )

        # Check that the lastCheckTimestamp was
        timestamp = terminal_doc.get("lastCheckTimestamp")

        if not isinstance(timestamp, datetime):
            self.fail("The timestamp should exist.")

        self.assertIsInstance(
            timestamp,
            datetime,
            "The timestamp should exist.",
        )

        # Timestamp should be within the last 2 minutes
        self.assertLess(
            datetime.now(ZoneInfo("UTC")) - timestamp,
            timedelta(minutes=2),
            "The timestamp should be within the last 2 minutes.",
        )

    def run_update_with_mock_response(  # noqa: PLR0913 (For testing purposes, we need to pass in a lot of parameters)
        self: "TestUpdateTerminalPdfs",
        mock_get_with_retry: MagicMock,
        test_terminal: Terminal,
        mock_response: dict,
        update_fingerprint: str,
        num_pdfs_updated: int,
        terminals_updated: List[str],
        checked_terminals: List[str],
    ) -> bool:
        """Execute update_terminal_pdfs with a specific mock response.

        Args:
        ----
            mock_get_with_retry (MagicMock): The mock get_with_retry function.
            test_terminal (Terminal): The terminal to update.
            mock_response (dict): The mock response to return.
            update_fingerprint (str): The update fingerprint.
            num_pdfs_updated (int): The number of pdfs updated.
            terminals_updated (List[str]): The list of terminals updated.
            checked_terminals (List[str]): The list of terminals checked.

        Returns:
        -------
            bool: True if the function was successful.

        """
        mock_get_with_retry.return_value = unittest.mock.Mock(**mock_response)

        results: List[bool] = []

        db_terminals = self.fs.get_all_terminals()

        if not db_terminals:
            self.fail("No terminals found in the database.")

        # Run the update_terminal_pdfs function with parameters adjusted as needed
        try:
            for terminal in db_terminals:
                result = update_terminal_pdfs(
                    fs=self.fs,
                    s3=self.s3,
                    terminal=terminal,
                    update_fingerprint=update_fingerprint,
                    num_pdfs_updated=num_pdfs_updated,
                    terminals_updated=terminals_updated,
                    terminals_checked=checked_terminals,
                )

                results.append(result)
        except Exception:
            return False

        return True

    @patch("scraper.scraper_utils.get_with_retry")
    def test_parallel_update_terminal_pdfs(
        self: "TestUpdateTerminalPdfs", mock_get_with_retry: MagicMock
    ) -> None:
        """Test that the update_terminal_pdfs function works in parallel.

        Insert 8 fake terminal documents into the terminal collection and then run 3 threads of the update_terminal_pdfs function.
        Mocked page responses are reused but since there are no pdfs to update, the function should return True for each terminal.
        Effectively, it's a dry run of the function to test that it works in parallel.
        """
        # Insert all 8 terminals into the terminal collection
        terminals: List[Terminal] = []

        for terminal_name in [
            "Dover",
            "Andrews",
            "Seattle",
            "Charleston",
        ]:
            terminal_data = {
                "name": f"{terminal_name} AFB Passenger Terminal",
                "link": f"https://www.amc.af.mil/AMC-Travel-Site/Terminals/{terminal_name}-AFB-Passenger-Terminal/",
                "group": "AMC CONUS TERMINALS",
                "location": "The Moon, Moon Base Alpha",
                "pagePosition": 0,
                "pdf30DayHash": "TestUpdateTerminalPdfs_test_hash",
                "pdf72HourHash": "TestUpdateTerminalPdfs_test_hash",
                "pdfRollcallHash": "TestUpdateTerminalPdfs_test_hash",
                "pdfUpdateLock": False,
                "pdfUpdateSignature": "test_signature",
                "timezone": "America/Moon_Base_Alpha",
                "updateStatus": "test_status",
            }

            self.fs.set_document(
                self.terminal_coll,
                f"{terminal_name} AFB Passenger Terminal",
                terminal_data,
            )

            terminals.append(Terminal.from_dict(terminal_data))

        terminal_responses = {
            "Dover AFB Passenger Terminal": self.dover_page_no_pdfs,
            "Andrews AFB Passenger Terminal": self.andrews_page_no_pdfs,
            "Seattle AFB Passenger Terminal": self.seattle_page_no_pdfs,
            "Charleston AFB Passenger Terminal": self.charleston_page_no_pdfs,
        }

        results = []
        checked_terminals: List[List[str]] = [
            [],
            [],
            [],
            [],
        ]  # To store the updated terminals for each thread
        index = 0

        with ThreadPoolExecutor(max_workers=3) as executor:
            futures = []
            for _, mock_response in terminal_responses.items():
                # Note: Directly passing the mock_response to the function
                future = executor.submit(
                    self.run_update_with_mock_response,
                    mock_get_with_retry,
                    terminals[index],
                    mock_response,
                    "diff_string_than_test_terminal_BLAH",
                    0,
                    [],
                    checked_terminals[index],
                )
                futures.append(future)
                index += 1
            for future in futures:
                results.append(future.result())

        # Assertions about the results or side effects here
        for result in results:
            self.assertTrue(result, "Expected successful update for each terminal.")

        # Prune empty lists from checked_terminals
        checked_terminals = [terminals for terminals in checked_terminals if terminals]

        # There should only be three workers updating the terminals
        # Technically, there is a 4th but the first 3 workers will
        # keep updating the same terminals until they are all updated.
        self.assertEqual(
            len(checked_terminals), 3, "Expected 3 workers to update terminals."
        )

        combined_list = [
            terminal for sublist in checked_terminals for terminal in sublist
        ]

        self.assertEqual(
            len(combined_list),
            4,
            "Expected 4 terminals to be updated.",
        )

    def tearDown(self: "TestUpdateTerminalPdfs") -> None:
        """Tear down the test cases for TestUpdateTerminalPdfs."""
        # Delete the test collections
        self.fs.delete_collection(self.terminal_coll)
        self.fs.delete_collection(self.pdf_archive_coll)
        self.fs.delete_collection(self.lock_coll)


if __name__ == "__main__":
    unittest.main()
