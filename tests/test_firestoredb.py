import os
import sys
import unittest
from typing import Optional, Type

current_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.append(current_dir + "/../")

from firestoredb import FirestoreClient  # noqa: E402 (Relative import)
from terminal import Terminal  # noqa: E402 (Relative import)


class TestFirestoreClient(unittest.TestCase):
    """Test the FirestoreClient class."""

    firestore_client: FirestoreClient
    orig_terminal_coll: Optional[str]
    test_terminal_coll: str

    @classmethod
    def setUpClass(cls: Type["TestFirestoreClient"]) -> None:
        """Set up the test class."""
        # Save the original TERMINAL_COLL value and set it to the test collection
        cls.orig_terminal_coll = os.environ.get("TERMINAL_COLL")
        cls.test_terminal_coll = "**UNIT_TEST**_Terminals"
        os.environ["TERMINAL_COLL"] = cls.test_terminal_coll

        # Initialize Firestore client once for all tests
        cls.firestore_client = FirestoreClient()

    def setUp(self: "TestFirestoreClient") -> None:
        """Set up the test."""
        # Clear the test collection before each test
        self.firestore_client.delete_collection(self.test_terminal_coll)

    def test_no_terminals_in_db(self: "TestFirestoreClient") -> None:
        """Test when there are no terminals in the database."""
        # Scenario: The database is empty
        terminal = Terminal()
        terminal.name = "test_no_terminals_in_db"
        terminal.location = "Naval Station Rota"

        updated = self.firestore_client.update_terminals([terminal])
        self.assertTrue(updated, "Terminals should be updated when DB is empty")

        terminals = self.firestore_client.get_all_terminals()

        self.assertEqual(
            len(terminals),
            1,
            "Should be one terminal in the DB since we only added one.",
        )

        db_test_terminal = terminals[0]

        self.assertEqual(db_test_terminal.name, terminal.name)
        self.assertEqual(db_test_terminal.location, terminal.location)
        self.assertEqual(db_test_terminal.timezone, "Europe/Madrid")

    def test_update_terminals_no_updates(self: "TestFirestoreClient") -> None:
        """Test updating terminals with no updates."""
        # Scenario: No updates needed
        terminal = Terminal()
        terminal.name = "test_update_terminals_no_updates"
        terminal.location = "Naval Station Rota"
        terminal.timezone = "Europe/Madrid"

        # Prepopulate the database
        self.firestore_client.set_document(
            self.test_terminal_coll, terminal.name, terminal.to_dict()
        )

        updated = self.firestore_client.update_terminals([terminal])
        self.assertFalse(updated, "No updates should occur when data is unchanged")

    def test_update_terminal_excluding_timezone(self: "TestFirestoreClient") -> None:
        """Test updating a terminal excluding timezone."""
        # Scenario: Update terminal without changing location/timezone
        terminal = Terminal()
        terminal.name = "test_update_terminal_excluding_timezone"
        terminal.location = "Naval Station Rota"
        terminal.timezone = "Europe/Madrid"

        # Prepopulate the database with slightly different data
        terminal_in_db = Terminal()
        terminal_in_db.name = terminal.name
        terminal_in_db.location = terminal.location
        terminal_in_db.timezone = terminal.timezone
        terminal_in_db.group = "Test Group"

        self.firestore_client.set_document(
            self.test_terminal_coll, terminal.name, terminal_in_db.to_dict()
        )

        updated = self.firestore_client.update_terminals([terminal])
        self.assertTrue(
            updated, "Should update terminal when non-location attributes change"
        )

        terminals = self.firestore_client.get_all_terminals()

        self.assertEqual(
            len(terminals),
            1,
            "Should be one terminal in the DB since we only added one.",
        )

        db_test_terminal = terminals[0]

        # Make sure only the group attribute was updated
        self.assertDictEqual(db_test_terminal.to_dict(), terminal.to_dict())

    def test_update_terminal_including_timezone(self: "TestFirestoreClient") -> None:
        """Test updating a terminal including timezone."""
        # Scenario: Update terminal including location and timezone
        terminal = Terminal()
        terminal.name = "test_update_terminal_including_timezone"
        terminal.location = "Ramstein Air Base"
        terminal.timezone = "Europe/Berlin"
        terminal.group = "Test Group"

        # Prepopulate the database with different location/timezone
        terminal_in_db = Terminal()
        terminal_in_db.name = terminal.name
        terminal_in_db.location = "Naval Station Rota"
        terminal_in_db.timezone = "Europe/Madrid"
        terminal_in_db.group = "Test Group 2"

        self.firestore_client.set_document(
            self.test_terminal_coll, terminal.name, terminal_in_db.to_dict()
        )

        updated = self.firestore_client.update_terminals([terminal])
        self.assertTrue(updated, "Should update terminal when location changes")

        terminals = self.firestore_client.get_all_terminals()

        self.assertEqual(
            len(terminals),
            1,
            "Should be one terminal in the DB since we only added one.",
        )

        db_test_terminal = terminals[0]

        # Make sure group, location, and timezone were updated
        self.assertDictEqual(db_test_terminal.to_dict(), terminal.to_dict())

    def test_no_empty_terminal_update(self: "TestFirestoreClient") -> None:
        """Test updating an empty terminal."""
        # Scenario: Terminal already exists in DB with all up to date info.
        # This test checks that we don't update the terminal in the DB with
        # an terminal object that has an empty timezone attribute. This can
        # happen because the scraped terminal object has an empty timezone
        # since it is not calculated until it is aboslutely needed. As a result,
        # if the code is broken, everytime the scraper runs, if a terminal
        # in the DB has a timezone, it will be overwritten with an empty
        # timezone.

        # Prepopulate the database with a terminal that has a timezone
        terminal = Terminal()
        terminal.name = "test_no_empty_terminal_update"
        terminal.location = "Naval Station Rota"
        terminal.timezone = "Europe/Madrid"

        self.firestore_client.set_document(
            self.test_terminal_coll, terminal.name, terminal.to_dict()
        )

        # Create a new terminal object with an empty timezone
        terminal = Terminal()
        terminal.name = "test_no_empty_terminal_update"
        terminal.location = "Naval Station Rota"
        terminal.timezone = ""

        updated = self.firestore_client.update_terminals([terminal])
        self.assertFalse(updated, "Should not update terminal when timezone is empty")

        # Make sure the timezone is still in the DB
        terminals = self.firestore_client.get_all_terminals()

        self.assertEqual(
            len(terminals),
            1,
            "Should be one terminal in the DB since we only added one.",
        )

        db_test_terminal = terminals[0]

        self.assertEqual(db_test_terminal.timezone, "Europe/Madrid")

    @classmethod
    def tearDownClass(cls: Type["TestFirestoreClient"]) -> None:
        """Tear down the test class."""
        # Clean up the test collection after all tests
        cls.firestore_client.delete_collection(cls.test_terminal_coll)

        # Restore the original TERMINAL_COLL value
        if cls.orig_terminal_coll is not None:
            os.environ["TERMINAL_COLL"] = cls.orig_terminal_coll
        else:
            del os.environ["TERMINAL_COLL"]


if __name__ == "__main__":
    unittest.main()
