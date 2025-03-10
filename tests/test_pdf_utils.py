import os
import pickle
import shutil
import sys
import unittest
from random import shuffle
from typing import Optional, Type

current_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.append(current_dir + "/../")

from pdf import Pdf  # noqa: E402 (Relative import)
from pdf_utils import (  # noqa: E402 (Relative import)
    local_sort_pdf_to_current,
    sort_pdfs_by_creation_time,
    sort_pdfs_by_modify_time,
    sort_terminal_pdfs,
    type_pdfs_by_content,
    type_pdfs_by_filename,
)
from scraper_utils import check_local_pdf_dirs  # noqa: E402 (Relative import)


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

    def test_andersen_20240129(self: "TestSortTerminalPdfs") -> None:
        """Test with Andersen terminal PDF scrape.

        This test is for the 20240129 scrape.
        """
        pdf_72hr = Pdf(
            link="tests/assets/TestSortTerminalPdfs/test_andersen_20240129/72 hr AFPIMS SLIDES.pdf",
            populate=False,
        )
        pdf_72hr.cloud_path = pdf_72hr.link
        pdf_72hr.original_filename = "72 hr AFPIMS SLIDES.pdf"

        pdf_30day = Pdf(
            "tests/assets/TestSortTerminalPdfs/test_andersen_20240129/NEXT 3 MONTHS PE SCHEDULE.pdf",
            populate=False,
        )

        pdf_30day.cloud_path = pdf_30day.link
        pdf_30day.original_filename = "NEXT 3 MONTHS PE SCHEDULE.pdf"

        pdf_rollcall = Pdf(
            "tests/assets/TestSortTerminalPdfs/test_andersen_20240129/24 Hour Roll Call.pdf",
            populate=False,
        )

        pdf_rollcall.cloud_path = pdf_rollcall.link
        pdf_rollcall.original_filename = "24 Hour Roll Call.pdf"

        pdf_terminal_info = Pdf(
            "tests/assets/TestSortTerminalPdfs/test_andersen_20240129/Andersen Passenger Terminal INFO.pdf",
            populate=False,
        )

        pdf_terminal_info.cloud_path = pdf_terminal_info.link
        pdf_terminal_info.original_filename = "Andersen Passenger Terminal INFO.pdf"

        pdfs = [pdf_72hr, pdf_30day, pdf_rollcall, pdf_terminal_info]

        # Shuffle order to test the regex
        shuffle(pdfs)

        ret_pdf_72hr, ret_pdf_30day, ret_pdf_rollcall = sort_terminal_pdfs(pdfs)

        if not ret_pdf_72hr:
            self.fail("Failed to find 72hr PDF")

        if not ret_pdf_30day:
            self.fail("Failed to find 30day PDF")

        if not ret_pdf_rollcall:
            self.fail("Failed to find rollcall PDF")

        # Set types of the loaded PDFs to make a comparison
        pdf_72hr.type = "72_HR"
        pdf_30day.type = "30_DAY"
        pdf_rollcall.type = "ROLLCALL"
        pdf_terminal_info.type = "DISCARD"

        self.assertEqual(ret_pdf_72hr, pdf_72hr)
        self.assertEqual(ret_pdf_30day, pdf_30day)
        self.assertEqual(ret_pdf_rollcall, pdf_rollcall)
        self.assertEqual(pdf_terminal_info.type, "DISCARD")

    @classmethod
    def tearDownClass(cls: Type["TestSortTerminalPdfs"]) -> None:
        """Reset the PDF_DIR environment variable to its original value."""
        if cls.old_pdf_dir is not None:
            os.environ["PDF_DIR"] = cls.old_pdf_dir
        else:
            os.environ.pop("PDF_DIR", None)


class TestSortPdfsByModifyTime(unittest.TestCase):
    """Test the sort_pdfs_by_modify_time function in pdf_utils."""

    def test_sort_pdfs_by_modify_time(self: "TestSortPdfsByModifyTime") -> None:
        """Test with a list of PDFs."""
        with open(
            "tests/assets/TestSortPdfsByModifyTime/test_sort_pdfs_by_modify_time/pdf1.pkl",
            "rb",
        ) as f:
            pdf1: Pdf = pickle.load(f)  # noqa: S301 (Only for testing)

        if not pdf1:
            self.fail(
                "Failed to load pdf1 object: tests/assets/TestSortPdfsByModifyTime/test_sort_pdfs_by_modify_time/pdf1.pkl"
            )

        with open(
            "tests/assets/TestSortPdfsByModifyTime/test_sort_pdfs_by_modify_time/pdf2.pkl",
            "rb",
        ) as f:
            pdf2: Pdf = pickle.load(f)  # noqa: S301 (Only for testing)

        if not pdf2:
            self.fail(
                "Failed to load pdf2 object: tests/assets/TestSortPdfsByModifyTime/test_sort_pdfs_by_modify_time/pdf2.pkl"
            )

        with open(
            "tests/assets/TestSortPdfsByModifyTime/test_sort_pdfs_by_modify_time/pdf3.pkl",
            "rb",
        ) as f:
            pdf3: Pdf = pickle.load(f)  # noqa: S301 (Only for testing)

        if not pdf3:
            self.fail(
                "Failed to load pdf3 object: tests/assets/TestSortPdfsByModifyTime/test_sort_pdfs_by_modify_time/pdf3.pkl"
            )

        with open(
            "tests/assets/TestSortPdfsByModifyTime/test_sort_pdfs_by_modify_time/pdf4.pkl",
            "rb",
        ) as f:
            pdf4: Pdf = pickle.load(f)  # noqa: S301 (Only for testing)

        if not pdf4:
            self.fail(
                "Failed to load pdf4 object: tests/assets/TestSortPdfsByModifyTime/test_sort_pdfs_by_modify_time/pdf4.pkl"
            )

        with open(
            "tests/assets/TestSortPdfsByModifyTime/test_sort_pdfs_by_modify_time/pdf5.pkl",
            "rb",
        ) as f:
            pdf5: Pdf = pickle.load(f)  # noqa: S301 (Only for testing)

        if not pdf5:
            self.fail(
                "Failed to load pdf5 object: tests/assets/TestSortPdfsByModifyTime/test_sort_pdfs_by_modify_time/pdf5.pkl"
            )

        pdf_list = [pdf1, pdf2, pdf3, pdf4, pdf5]

        sorted_pdfs = sort_pdfs_by_modify_time(pdf_list)

        self.assertListEqual(pdf_list, sorted_pdfs)


class TestSortPdfsByCreationTime(unittest.TestCase):
    """Test the sort_pdfs_by_creation_time function in pdf_utils."""

    def test_sort_pdfs_by_creation_time(self: "TestSortPdfsByCreationTime") -> None:
        """Test with a list of PDFs."""
        with open(
            "tests/assets/TestSortPdfsByCreationTime/test_sort_pdfs_by_creation_time/pdf1.pkl",
            "rb",
        ) as f:
            pdf1: Pdf = pickle.load(f)  # noqa: S301 (Only for testing)

        if not pdf1:
            self.fail(
                "Failed to load pdf1 object: tests/assets/TestSortPdfsByCreationTime/test_sort_pdfs_by_creation_time/pdf1.pkl"
            )

        with open(
            "tests/assets/TestSortPdfsByCreationTime/test_sort_pdfs_by_creation_time/pdf2.pkl",
            "rb",
        ) as f:
            pdf2: Pdf = pickle.load(f)  # noqa: S301 (Only for testing)

        if not pdf2:
            self.fail(
                "Failed to load pdf2 object: tests/assets/TestSortPdfsByCreationTime/test_sort_pdfs_by_creation_time/pdf2.pkl"
            )

        with open(
            "tests/assets/TestSortPdfsByCreationTime/test_sort_pdfs_by_creation_time/pdf3.pkl",
            "rb",
        ) as f:
            pdf3: Pdf = pickle.load(f)  # noqa: S301 (Only for testing)

        if not pdf3:
            self.fail(
                "Failed to load pdf3 object: tests/assets/TestSortPdfsByCreationTime/test_sort_pdfs_by_creation_time/pdf3.pkl"
            )

        with open(
            "tests/assets/TestSortPdfsByCreationTime/test_sort_pdfs_by_creation_time/pdf4.pkl",
            "rb",
        ) as f:
            pdf4: Pdf = pickle.load(f)  # noqa: S301 (Only for testing)

        if not pdf4:
            self.fail(
                "Failed to load pdf4 object: tests/assets/TestSortPdfsByCreationTime/test_sort_pdfs_by_creation_time/pdf4.pkl"
            )

        with open(
            "tests/assets/TestSortPdfsByCreationTime/test_sort_pdfs_by_creation_time/pdf5.pkl",
            "rb",
        ) as f:
            pdf5: Pdf = pickle.load(f)  # noqa: S301 (Only for testing)

        if not pdf5:
            self.fail(
                "Failed to load pdf5 object: tests/assets/TestSortPdfsByCreationTime/test_sort_pdfs_by_creation_time/pdf5.pkl"
            )

        pdf_list = [pdf1, pdf2, pdf3, pdf4, pdf5]

        sorted_pdfs = sort_pdfs_by_creation_time(pdf_list)

        self.assertListEqual(pdf_list, sorted_pdfs)


class TestTypePdfsByFilename(unittest.TestCase):
    """Test the type_pdfs_by_filename function in pdf_utils."""

    def setUp(self: "TestTypePdfsByFilename") -> None:
        """Set up test environment for type_pdfs_by_filename."""
        self.old_pdf_dir = os.getenv("PDF_DIR")
        os.environ["PDF_DIR"] = ""

    def test_type_pdfs_by_filename(self: "TestTypePdfsByFilename") -> None:
        """Test with a list of PDFs."""
        with open(
            "tests/assets/TestTypePdfsByFilename/type_pdfs_by_filename/72_Hour_Flight_Schedule_dde0f3d2-a2d5-4485-9d90-adf36fa39213.pdf.pkl",
            "rb",
        ) as f:
            pdf_72hr: Pdf = pickle.load(f)  # noqa: S301 (Only for testing)

        if not pdf_72hr:
            self.fail(
                "Failed to load pdf1 object: tests/assets/TestTypePdfsByFilename/type_pdfs_by_filename/72_Hour_Flight_Schedule_dde0f3d2-a2d5-4485-9d90-adf36fa39213.pdf.pkl"
            )

        pdf_72hr.cloud_path = "tests/assets/TestTypePdfsByFilename/type_pdfs_by_filename/72_Hour_Flight_Schedule_f7aee836-f3b3-4c46-85fd-d07fb2904289.pdf"

        with open(
            "tests/assets/TestTypePdfsByFilename/type_pdfs_by_filename/24-Hour_Space-A_Roll_Call_Report__fa1207be-1bfc-4cba-a247-576beb4f8676.pdf.pkl",
            "rb",
        ) as f:
            pdf_rollcall: Pdf = pickle.load(f)  # noqa: S301 (Only for testing)

        if not pdf_rollcall:
            self.fail(
                "Failed to load pdf2 object: tests/assets/TestTypePdfsByFilename/type_pdfs_by_filename/24-Hour_Space-A_Roll_Call_Report__fa1207be-1bfc-4cba-a247-576beb4f8676.pdf.pkl"
            )

        pdf_rollcall.cloud_path = "tests/assets/TestTypePdfsByFilename/type_pdfs_by_filename/24-Hour_Space-A_Roll_Call_Report__ede640c9-f463-4497-a809-d41512ae926c.pdf"

        with open(
            "tests/assets/TestTypePdfsByFilename/type_pdfs_by_filename/DECEMBER_PE_SCHEDULE_53d9e289-048d-49a7-bb53-5ea6e197a92d.pdf.pkl",
            "rb",
        ) as f:
            pdf_30day: Pdf = pickle.load(f)  # noqa: S301 (Only for testing)

        if not pdf_30day:
            self.fail(
                "Failed to load pdf3 object: tests/assets/TestTypePdfsByFilename/type_pdfs_by_filename/DECEMBER_PE_SCHEDULE_53d9e289-048d-49a7-bb53-5ea6e197a92d.pdf.pkl"
            )

        pdf_30day.cloud_path = "tests/assets/TestTypePdfsByFilename/type_pdfs_by_filename/DECEMBER_PE_SCHEDULE_1d65a5ad-6b0f-42ac-a2b6-b5467df6c27d.pdf"

        with open(
            "tests/assets/TestTypePdfsByFilename/type_pdfs_by_filename/AEF_72_HRS_9a5709cd-9505-40ed-abb7-6a5967f1e1f9.pdf.pkl",
            "rb",
        ) as f:
            pdf_aef_72hr: Pdf = pickle.load(f)  # noqa: S301 (Only for testing)

        if not pdf_aef_72hr:
            self.fail(
                "Failed to load pdf4 object: tests/assets/TestTypePdfsByFilename/type_pdfs_by_filename/AEF_72_HRS_9a5709cd-9505-40ed-abb7-6a5967f1e1f9.pdf.pkl"
            )

        pdf_aef_72hr.cloud_path = "tests/assets/TestTypePdfsByFilename/type_pdfs_by_filename/AEF_72_HRS_2e2549ca-6161-4735-8700-31abecc87eca.pdf"

        with open(
            "tests/assets/TestTypePdfsByFilename/type_pdfs_by_filename/AMC_GRAM_NORFOLK_VA_NOV_23_f21a2e19-cf45-43fa-a4f9-c7802d2a49d6.pdf.pkl",
            "rb",
        ) as f:
            pdf_gram: Pdf = pickle.load(f)  # noqa: S301 (Only for testing)

        if not pdf_gram:
            self.fail(
                "Failed to load pdf5 object: tests/assets/TestTypePdfsByFilename/type_pdfs_by_filename/AMC_GRAM_NORFOLK_VA_NOV_23_f21a2e19-cf45-43fa-a4f9-c7802d2a49d6.pdf.pkl"
            )

        pdf_gram.cloud_path = "tests/assets/TestTypePdfsByFilename/type_pdfs_by_filename/AMC_GRAM_NORFOLK_VA_NOV_23_3a6651c3-ee98-4b03-b112-e89709825c25.pdf"

        pdfs = [pdf_72hr, pdf_rollcall, pdf_30day, pdf_aef_72hr, pdf_gram]

        found = {
            "72_HR": False,
            "30_DAY": False,
            "ROLLCALL": False,
        }

        no_match_pdfs = type_pdfs_by_filename(pdfs, found)

        self.assertTrue(found["72_HR"])
        self.assertTrue(found["30_DAY"])
        self.assertTrue(found["ROLLCALL"])

        self.assertEqual(pdf_72hr.type, "72_HR")
        self.assertEqual(pdf_30day.type, "30_DAY")
        self.assertEqual(pdf_rollcall.type, "ROLLCALL")
        self.assertEqual(pdf_aef_72hr.type, "DISCARD")
        self.assertEqual(pdf_gram.type, "DISCARD")

        self.assertCountEqual(no_match_pdfs, [])

    def tearDown(self: "TestTypePdfsByFilename") -> None:
        """Reset the PDF_DIR environment variable."""
        if self.old_pdf_dir:
            os.environ["PDF_DIR"] = self.old_pdf_dir
        else:
            os.environ.pop("PDF_DIR", None)


class TestLocalSortPdfToCurrent(unittest.TestCase):
    """Test the local_sort_pdf_to_current function in pdf_utils."""

    old_pdf_dir: Optional[str]

    @classmethod
    def setUpClass(cls: Type["TestLocalSortPdfToCurrent"]) -> None:
        """Set the PDF_DIR environment variable to an empty string."""
        cls.old_pdf_dir = os.getenv("PDF_DIR")
        os.environ["PDF_DIR"] = "./test_local_sort_pdf_to_current"

        check_local_pdf_dirs()

    def test_local_sort_pdf_to_current(self: "TestLocalSortPdfToCurrent") -> None:
        """Test with a list of PDFs."""
        local_pdf_dir = os.getenv("PDF_DIR")

        if not local_pdf_dir:
            self.fail("Failed to load local_pdf_dir")

        with open(
            "tests/assets/TestLocalSortPdfToCurrent/test_local_sort_pdf_to_current/pdf1.pkl",
            "rb",
        ) as f:
            pdf1: Pdf = pickle.load(f)  # noqa: S301 (Only for testing)

        if not pdf1:
            self.fail(
                "Failed to load pdf1 object: tests/assets/TestLocalSortPdfToCurrent/test_local_sort_pdf_to_current/pdf1.pkl"
            )

        with open(
            "tests/assets/TestLocalSortPdfToCurrent/test_local_sort_pdf_to_current/pdf2.pkl",
            "rb",
        ) as f:
            pdf2: Pdf = pickle.load(f)  # noqa: S301 (Only for testing)

        if not pdf2:
            self.fail(
                "Failed to load pdf2 object: tests/assets/TestLocalSortPdfToCurrent/test_local_sort_pdf_to_current/pdf2.pkl"
            )

        with open(
            "tests/assets/TestLocalSortPdfToCurrent/test_local_sort_pdf_to_current/pdf3.pkl",
            "rb",
        ) as f:
            pdf3: Pdf = pickle.load(f)  # noqa: S301 (Only for testing)

        if not pdf3:
            self.fail(
                "Failed to load pdf3 object: tests/assets/TestLocalSortPdfToCurrent/test_local_sort_pdf_to_current/pdf3.pkl"
            )

        pdf1.set_type("72_HR")
        pdf2.set_type("30_DAY")
        pdf3.set_type("ROLLCALL")

        shutil.copy(
            pdf1.cloud_path,
            f"{local_pdf_dir}/tmp/pdf1.pdf",  # noqa: S108 (Only for testing)
        )
        shutil.copy(
            pdf2.cloud_path,
            f"{local_pdf_dir}/tmp/pdf2.pdf",  # noqa: S108 (Only for testing)
        )
        shutil.copy(
            pdf3.cloud_path,
            f"{local_pdf_dir}/tmp/pdf3.pdf",  # noqa: S108 (Only for testing)
        )

        pdf1.cloud_path = "tmp/pdf1.pdf"
        pdf2.cloud_path = "tmp/pdf2.pdf"
        pdf3.cloud_path = "tmp/pdf3.pdf"

        local_sort_pdf_to_current(pdf1)
        local_sort_pdf_to_current(pdf2)
        local_sort_pdf_to_current(pdf3)

        # Ensure that the PDFs paths were updated correctly
        self.assertTrue(os.path.isfile(pdf1.get_local_path()))
        self.assertTrue(os.path.isfile(pdf2.get_local_path()))
        self.assertTrue(os.path.isfile(pdf3.get_local_path()))

        # Ensure that the PDFs were moved to the correct directories
        self.assertEqual(
            pdf1.get_local_path(),
            "./test_local_sort_pdf_to_current/current/72_HR/pdf1.pdf",
        )
        self.assertEqual(
            pdf2.get_local_path(),
            "./test_local_sort_pdf_to_current/current/30_DAY/pdf2.pdf",
        )
        self.assertEqual(
            pdf3.get_local_path(),
            "./test_local_sort_pdf_to_current/current/ROLLCALL/pdf3.pdf",
        )

    @classmethod
    def tearDownClass(cls: Type["TestLocalSortPdfToCurrent"]) -> None:
        """Reset the PDF_DIR environment variable to its original value."""
        local_pdf_dir = os.getenv("PDF_DIR")

        if not local_pdf_dir:
            print("Failed to load local_pdf_dir in tearDownClass")
            sys.exit(1)

        shutil.rmtree(local_pdf_dir)

        if cls.old_pdf_dir is not None:
            os.environ["PDF_DIR"] = cls.old_pdf_dir
        else:
            os.environ.pop("PDF_DIR", None)
