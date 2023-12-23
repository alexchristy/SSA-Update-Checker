import os
import sys
import unittest
from typing import Type

import dotenv

current_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.append(current_dir + "/../")

from location_tz import (  # noqa: E402 (Relative import)
    BadLocationError,
    TerminalTzFinder,
)


class TestLocationTz(unittest.TestCase):
    """Test the location_tz module."""

    ttf: TerminalTzFinder

    @classmethod
    def setUpClass(cls: Type["TestLocationTz"]) -> None:
        """Set up the test class."""
        dotenv.load_dotenv()
        cls.ttf = TerminalTzFinder()

    def test_get_timezone(self: "TestLocationTz") -> None:
        """Test the get_timezone method."""
        returned_tz = self.ttf.get_timezone("Naval Station Rota")
        self.assertEqual(returned_tz, "Europe/Madrid")

        returned_tz = self.ttf.get_timezone("Naval Station Rota, Spain")
        self.assertEqual(returned_tz, "Europe/Madrid")

        returned_tz = self.ttf.get_timezone("Norfolk, VA")
        self.assertEqual(returned_tz, "America/New_York")

        returned_tz = self.ttf.get_timezone("Ramstein Air Base")
        self.assertEqual(returned_tz, "Europe/Berlin")

    def test_get_timezone_fail_bad_location(self: "TestLocationTz") -> None:
        """Test that the get_timezone method fails with bad locations and throws a BadLocationError exception."""
        with self.assertRaises(BadLocationError):
            returned_tz = self.ttf.get_timezone("sdasdadwadwaw")
            print(returned_tz)
