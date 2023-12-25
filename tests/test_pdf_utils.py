import os
import pickle
import sys
import unittest
from typing import Optional, Type

current_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.append(current_dir + "/../")

from pdf import Pdf  # noqa: E402 (Relative import)
from pdf_utils import (  # noqa: E402 (Relative import)
    sort_terminal_pdfs,
    type_pdfs_by_content,
)


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

    def test_bwi(self: "TestSortTerminalPdfs") -> None:
        """Test with BWI terminal PDF scrape."""
        with open(
            "tests/assets/TestSortTerminalPdfs/test_bwi/BWI_72HR_24-26DEC23_840971f8-f3c1-48a2-97a1-f3b47871481a.pdf.pkl",
            "rb",
        ) as f:
            pdf_72hr: Pdf = pickle.load(f)  # noqa: S301 (Only for testing)

        if not pdf_72hr:
            self.fail(
                "Failed to load 72hr pdf object: tests/assets/TestSortTerminalPdfs/test_bwi/BWI_72HR_24-26DEC23_840971f8-f3c1-48a2-97a1-f3b47871481a.pdf.pkl"
            )

        with open(
            "tests/assets/TestSortTerminalPdfs/test_bwi/BWI_30DAY_DEC23JAN24_4503c09a-e469-4798-bbf0-2dfea582720c.pdf.pkl",
            "rb",
        ) as f:
            pdf_30day: Pdf = pickle.load(f)  # noqa: S301 (Only for testing)

        if not pdf_30day:
            self.fail(
                "Failed to load 30day pdf object: tests/assets/TestSortTerminalPdfs/test_bwi/BWI_30DAY_DEC23JAN24_4503c09a-e469-4798-bbf0-2dfea582720c.pdf.pkl"
            )

        with open(
            "tests/assets/TestSortTerminalPdfs/test_bwi/BWI_ROLLCALL_21DEC23_d1d0ad96-f5b8-46ee-8ed3-04714f122c60.pdf.pkl",
            "rb",
        ) as f:
            pdf_rollcall: Pdf = pickle.load(f)  # noqa: S301 (Only for testing)

        if not pdf_rollcall:
            self.fail(
                "Failed to load rollcall pdf object: tests/assets/TestSortTerminalPdfs/test_bwi/BWI_ROLLCALL_21DEC23_d1d0ad96-f5b8-46ee-8ed3-04714f122c60.pdf.pkl"
            )

        pdfs = [pdf_72hr, pdf_rollcall, pdf_30day]

        ret_pdf_72hr, ret_pdf_30day, ret_pdf_rollcall = sort_terminal_pdfs(pdfs)

        # Ensure all three types were found
        self.assertIsNotNone(ret_pdf_72hr)
        self.assertIsNotNone(ret_pdf_30day)
        self.assertIsNotNone(ret_pdf_rollcall)

        # Set types of the loaded PDFs to make a comparison
        pdf_72hr.type = "72_HR"
        pdf_30day.type = "30_DAY"
        pdf_rollcall.type = "ROLLCALL"

        self.assertEqual(ret_pdf_72hr, pdf_72hr)
        self.assertEqual(ret_pdf_30day, pdf_30day)
        self.assertEqual(ret_pdf_rollcall, pdf_rollcall)

    def test_dover(self: "TestSortTerminalPdfs") -> None:
        """Test with Dover terminal PDF scrape."""
        with open(
            "tests/assets/TestSortTerminalPdfs/test_dover/DOV_72HR_24DEC23_7919fe85-48a7-48db-8f22-c2db2c1268d5.pdf.pkl",
            "rb",
        ) as f:
            pdf_72hr: Pdf = pickle.load(f)  # noqa: S301 (Only for testing)

        if not pdf_72hr:
            self.fail(
                "Failed to load 72hr pdf object: tests/assets/TestSortTerminalPdfs/test_dover/DOV_72HR_24DEC23_7919fe85-48a7-48db-8f22-c2db2c1268d5.pdf.pkl"
            )

        with open(
            "tests/assets/TestSortTerminalPdfs/test_dover/DOVER_ROLLCALL_23DEC23_1bdbc13b-454d-42f6-a9f5-05532c983890.pdf.pkl",
            "rb",
        ) as f:
            pdf_rollcall: Pdf = pickle.load(f)  # noqa: S301 (Only for testing)

        if not pdf_rollcall:
            self.fail(
                "Failed to load 30day pdf object: tests/assets/TestSortTerminalPdfs/test_dover/DOVER_ROLLCALL_23DEC23_1bdbc13b-454d-42f6-a9f5-05532c983890.pdf.pkl"
            )

        with open(
            "tests/assets/TestSortTerminalPdfs/test_dover/AMC_Gram-_KDOV_%28FEB_2023%29_6b25f449-91e5-4e23-8e48-c2bba15d1008.pdf.pkl",
            "rb",
        ) as f:
            pdf_amc_gram: Pdf = pickle.load(f)  # noqa: S301 (Only for testing)

        if not pdf_amc_gram:
            self.fail(
                "Failed to load AMC Gram pdf object: tests/assets/TestSortTerminalPdfs/test_dover/AMC_Gram-_KDOV_%28FEB_2023%29_6b25f449-91e5-4e23-8e48-c2bba15d1008.pdf.pkl"
            )

        pdfs = [pdf_amc_gram, pdf_72hr, pdf_rollcall]

        ret_pdf_72hr, ret_pdf_30day, ret_pdf_rollcall = sort_terminal_pdfs(pdfs)

        # Ensure only 72hr and rollcall were found
        self.assertIsNotNone(ret_pdf_72hr)
        self.assertIsNone(ret_pdf_30day)
        self.assertIsNotNone(ret_pdf_rollcall)

        # Set types of the loaded PDFs to make a comparison
        pdf_72hr.type = "72_HR"
        pdf_rollcall.type = "ROLLCALL"
        pdf_amc_gram.type = "DISCARD"

        self.assertEqual(ret_pdf_72hr, pdf_72hr)
        self.assertEqual(ret_pdf_rollcall, pdf_rollcall)
        self.assertEqual(pdf_amc_gram.type, "DISCARD")

    def test_naples(self: "TestSortTerminalPdfs") -> None:
        """Test with Naples terminal PDF scrape."""
        with open(
            "tests/assets/TestSortTerminalPdfs/test_naples/Naples_72HR_24DEC2023P_3d30efde-51e0-4de6-9e4c-521d901be711.pdf.pkl",
            "rb",
        ) as f:
            pdf_72hr: Pdf = pickle.load(f)  # noqa: S301 (Only for testing)

        if not pdf_72hr:
            self.fail(
                "Failed to load 72hr pdf object: tests/assets/TestSortTerminalPdfs/test_naples/Naples_72HR_24DEC2023P_3d30efde-51e0-4de6-9e4c-521d901be711.pdf.pkl"
            )

        with open(
            "tests/assets/TestSortTerminalPdfs/test_naples/Naples_30DAY_28NOV23_b7301cac-152e-4a10-9dbd-9f8f1ffa7c76.pdf.pkl",
            "rb",
        ) as f:
            pdf_30day: Pdf = pickle.load(f)  # noqa: S301 (Only for testing)

        if not pdf_30day:
            self.fail(
                "Failed to load 30day pdf object: tests/assets/TestSortTerminalPdfs/test_naples/Naples_30DAY_28NOV23_b7301cac-152e-4a10-9dbd-9f8f1ffa7c76.pdf.pkl"
            )

        with open(
            "tests/assets/TestSortTerminalPdfs/test_naples/ROLL_CALL_TEMPLATE12dec_4efe52fd-9647-46bc-b7fb-23050820dcd7.pdf.pkl",
            "rb",
        ) as f:
            pdf_rollcall: Pdf = pickle.load(f)  # noqa: S301 (Only for testing)

        if not pdf_rollcall:
            self.fail(
                "Failed to load rollcall pdf object: tests/assets/TestSortTerminalPdfs/test_naples/ROLL_CALL_TEMPLATE12dec_4efe52fd-9647-46bc-b7fb-23050820dcd7.pdf.pkl"
            )

        with open(
            "tests/assets/TestSortTerminalPdfs/test_naples/AMC_GRAM_01JUN23_9d87b34d-71e2-401d-97fe-cd531b2b830d.pdf.pkl",
            "rb",
        ) as f:
            pdf_gram: Pdf = pickle.load(f)  # noqa: S301 (Only for testing)

        if not pdf_gram:
            self.fail(
                "Failed to load AMC Gram pdf object: tests/assets/TestSortTerminalPdfs/test_naples/AMC_GRAM_01JUN23_9d87b34d-71e2-401d-97fe-cd531b2b830d.pdf.pkl"
            )

        pdfs = [pdf_gram, pdf_rollcall, pdf_72hr, pdf_30day]

        ret_pdf_72hr, ret_pdf_30day, ret_pdf_rollcall = sort_terminal_pdfs(pdfs)

        # Ensure all three types were found
        self.assertIsNotNone(ret_pdf_72hr)
        self.assertIsNotNone(ret_pdf_30day)
        self.assertIsNotNone(ret_pdf_rollcall)

        # Set types of the loaded PDFs to make a comparison
        pdf_72hr.type = "72_HR"
        pdf_30day.type = "30_DAY"
        pdf_rollcall.type = "ROLLCALL"
        pdf_gram.type = "DISCARD"

        self.assertEqual(ret_pdf_72hr, pdf_72hr)
        self.assertEqual(ret_pdf_30day, pdf_30day)
        self.assertEqual(ret_pdf_rollcall, pdf_rollcall)
        self.assertEqual(pdf_gram.type, "DISCARD")

    @classmethod
    def tearDownClass(cls: Type["TestSortTerminalPdfs"]) -> None:
        """Reset the PDF_DIR environment variable to its original value."""
        if cls.old_pdf_dir is not None:
            os.environ["PDF_DIR"] = cls.old_pdf_dir
        else:
            os.environ.pop("PDF_DIR", None)
