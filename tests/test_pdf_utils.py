import os
import pickle
import sys
import unittest
from typing import Optional, Type

from dotenv import load_dotenv

current_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.append(current_dir + "/../")

from pdf import Pdf  # noqa: E402 (Relative import)
from pdf_utils import (  # noqa: E402 (Relative import)
    sort_terminal_pdfs,
    type_pdfs_by_content,
)
from terminal import Terminal  # noqa: E402 (Relative import)

load_dotenv()
terminal = Terminal()
terminal.link = "https://www.amc.af.mil/AMC-Travel-Site/Terminals/CONUS-Terminals/NS-Norfolk-Passenger-Terminal/"


class TestTypePdfsByContent(unittest.TestCase):
    """Test the type_pdfs_by_content function in pdf_utils."""

    old_pdf_dir: Optional[str]

    @classmethod
    def setUpClass(cls: Type["TestTypePdfsByContent"]) -> None:
        """Set the PDF_DIR environment variable to an empty string."""
        cls.old_pdf_dir = os.getenv("PDF_DIR")
        os.environ["PDF_DIR"] = ""

    def test_norfolk_with_aef_as_discard(self: "TestTypePdfsByContent") -> None:
        """Test with norfolk terminal PDF scrape.

        Should mark the 72_HR AEF and AMC GRAM PDFs as discard since we are not supporting it for now.
        """
        with open(
            "tests/assets/TestTypePdfsByContent/72hr_pdf.pkl",
            "rb",
        ) as f:
            pdf_72hr: Pdf = pickle.load(f)  # noqa: S301 (Only for testing)

        if not pdf_72hr:
            self.fail(
                "Failed to load 72hr pdf object: tests/assets/TestTypePdfsByContent/72hr_pdf.pkl"
            )

        with open(
            "tests/assets/TestTypePdfsByContent/30day_pdf.pkl",
            "rb",
        ) as f:
            pdf_30day: Pdf = pickle.load(f)  # noqa: S301 (Only for testing)

        if not pdf_30day:
            self.fail(
                "Failed to load 30day pdf object: tests/assets/TestTypePdfsByContent/30day_pdf.pkl"
            )

        with open(
            "tests/assets/TestTypePdfsByContent/Rollcall_pdf.pkl",
            "rb",
        ) as f:
            pdf_rollcall: Pdf = pickle.load(f)  # noqa: S301 (Only for testing)

        if not pdf_rollcall:
            self.fail(
                "Failed to load rollcall pdf object: tests/assets/TestTypePdfsByContent/Rollcall_pdf.pkl"
            )

        with open(
            "tests/assets/TestTypePdfsByContent/AEF_72hr_pdf.pkl",
            "rb",
        ) as f:
            pdf_aef_72hr: Pdf = pickle.load(f)  # noqa: S301 (Only for testing)

        if not pdf_aef_72hr:
            self.fail(
                "Failed to load AEF 72hr pdf object: tests/assets/TestTypePdfsByContent/AEF_72hr_pdf.pkl"
            )

        with open(
            "tests/assets/TestTypePdfsByContent/AMC_GRAM_pdf.pkl",
            "rb",
        ) as f:
            pdf_gram: Pdf = pickle.load(f)  # noqa: S301 (Only for testing)

        if not pdf_gram:
            self.fail(
                "Failed to load AMC GRAM pdf object: tests/assets/TestTypePdfsByContent/AMC_GRAM_pdf.pkl"
            )

        pdfs = [pdf_72hr, pdf_rollcall, pdf_30day, pdf_aef_72hr, pdf_gram]

        found = {
            "72_HR": False,
            "30_DAY": False,
            "ROLLCALL": False,
        }

        type_pdfs_by_content(pdfs, found)

        self.assertTrue(found["72_HR"])
        self.assertTrue(found["30_DAY"])
        self.assertTrue(found["ROLLCALL"])

        self.assertEqual(pdf_72hr.type, "72_HR")
        self.assertEqual(pdf_30day.type, "30_DAY")
        self.assertEqual(pdf_rollcall.type, "ROLLCALL")
        self.assertEqual(pdf_aef_72hr.type, "DISCARD")
        self.assertEqual(pdf_gram.type, "DISCARD")

    def tearDown(self: "TestTypePdfsByContent") -> None:
        """Reset the PDF_DIR environment variable."""
        if self.old_pdf_dir:
            os.environ["PDF_DIR"] = self.old_pdf_dir
        else:
            os.environ.pop("PDF_DIR", None)


class TestSortTerminalPdfs(unittest.TestCase):
    """Test the sort_terminal_pdfs function in pdf_utils."""

    old_pdf_dir: Optional[str]

    @classmethod
    def setUpClass(cls: Type["TestSortTerminalPdfs"]) -> None:
        """Set the PDF_DIR environment variable to an empty string."""
        cls.old_pdf_dir = os.getenv("PDF_DIR")
        os.environ["PDF_DIR"] = ""

    def test_norfolk_with_aef(self: "TestSortTerminalPdfs") -> None:
        """Test with norfolk terminal PDF scrape.

        Should mark the 72_HR AEF and AMC GRAM PDFs as discard since we are not supporting it for now.
        """
        with open(
            "tests/assets/TestSortTerminalPdfs/test_norfolk_with_aef/72hr_pdf.pkl",
            "rb",
        ) as f:
            pdf_72hr: Pdf = pickle.load(f)  # noqa: S301 (Only for testing)

        if not pdf_72hr:
            self.fail(
                "Failed to load 72hr pdf object: tests/assets/TestSortTerminalPdfs/test_norfolk_with_aef/72hr_pdf.pkl"
            )

        with open(
            "tests/assets/TestSortTerminalPdfs/test_norfolk_with_aef/30day_pdf.pkl",
            "rb",
        ) as f:
            pdf_30day: Pdf = pickle.load(f)  # noqa: S301 (Only for testing)

        if not pdf_30day:
            self.fail(
                "Failed to load 30day pdf object: tests/assets/TestSortTerminalPdfs/test_norfolk_with_aef/30day_pdf.pkl"
            )

        with open(
            "tests/assets/TestSortTerminalPdfs/test_norfolk_with_aef/Rollcall_pdf.pkl",
            "rb",
        ) as f:
            pdf_rollcall: Pdf = pickle.load(f)  # noqa: S301 (Only for testing)

        if not pdf_rollcall:
            self.fail(
                "Failed to load rollcall pdf object: tests/assets/TestSortTerminalPdfs/test_norfolk_with_aef/Rollcall_pdf.pkl"
            )

        with open(
            "tests/assets/TestSortTerminalPdfs/test_norfolk_with_aef/AEF_72hr_pdf.pkl",
            "rb",
        ) as f:
            pdf_aef_72hr: Pdf = pickle.load(f)  # noqa: S301 (Only for testing)

        if not pdf_aef_72hr:
            self.fail(
                "Failed to load AEF 72hr pdf object: tests/assets/TestSortTerminalPdfs/test_norfolk_with_aef/AEF_72hr_pdf.pkl"
            )

        with open(
            "tests/assets/TestSortTerminalPdfs/test_norfolk_with_aef/AMC_GRAM_pdf.pkl",
            "rb",
        ) as f:
            pdf_gram: Pdf = pickle.load(f)  # noqa: S301 (Only for testing)

        if not pdf_gram:
            self.fail(
                "Failed to load AMC GRAM pdf object: tests/assets/TestSortTerminalPdfs/test_norfolk_with_aef/AMC_GRAM_pdf.pkl"
            )

        pdfs = [pdf_72hr, pdf_rollcall, pdf_30day, pdf_aef_72hr, pdf_gram]

        ret_pdf_72hr, ret_pdf_30day, ret_pdf_rollcall = sort_terminal_pdfs(pdfs)

        # Ensure all three types were found
        self.assertIsNotNone(ret_pdf_72hr)
        self.assertIsNotNone(ret_pdf_30day)
        self.assertIsNotNone(ret_pdf_rollcall)

        # Set types of the loaded PDFs to make a comparison
        pdf_72hr.type = "72_HR"
        pdf_30day.type = "30_DAY"
        pdf_rollcall.type = "ROLLCALL"
        pdf_aef_72hr.type = "DISCARD"
        pdf_gram.type = "DISCARD"

        # Ensure the correct PDFs were found
        self.assertEqual(ret_pdf_72hr, pdf_72hr)
        self.assertEqual(ret_pdf_30day, pdf_30day)
        self.assertEqual(ret_pdf_rollcall, pdf_rollcall)
        self.assertEqual(pdf_aef_72hr.type, "DISCARD")
        self.assertEqual(pdf_gram.type, "DISCARD")
