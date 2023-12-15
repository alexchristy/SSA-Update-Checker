import glob
import logging
import os
import re
from typing import Dict, List, Optional, Tuple

import PyPDF2
from pdfminer.high_level import extract_pages, extract_text
from pdfminer.layout import LTChar, LTTextContainer
from pdfminer.pdfdocument import PDFDocument, PDFNoValidXRef
from pdfminer.pdfpage import PDFPage
from pdfminer.pdfparser import PDFParser
from pdfminer.psparser import PSEOF

from pdf import Pdf


def check_downloaded_pdfs(directory_path: str) -> bool:
    """Check if at least one PDF was downloaded and log the number of PDFs in the directory.

    Args:
    ----
        directory_path (str): Path to directory containing downloaded PDFs.

    Returns:
    -------
        bool: True if at least one PDF was downloaded, False otherwise.
    """
    num_pdf_files = len(glob.glob(os.path.join(directory_path, "*.pdf")))
    if num_pdf_files == 0:
        logging.warning("No PDFs were downloaded in the directory: %s", directory_path)
    else:
        logging.info(
            "%d PDFs were downloaded in the directory: %s",
            num_pdf_files,
            directory_path,
        )
    return num_pdf_files > 0


def type_pdfs_by_content(list_of_pdfs: List[Pdf], found: Dict[str, bool]) -> None:
    """Sort a list of PDFs from ONE TERMINAL by their text content.

    Args:
    ----
        list_of_pdfs (List[Pdf]): List of Pdf objects
        found (Dict[str, bool]): Dictionary of boolean flags indicating whether a PDF of a given type has been found

    Returns:
    -------
        None
    """
    logging.debug("Entering type_pdfs_by_content()")

    # Disable excessive PDF logging
    logging.getLogger("pdfminer").setLevel(logging.INFO)

    # Define keys for searching the PDF text content
    sch_72hr_keys = ["roll call", "destination", "seats"]
    sch_30day_keys = ["30-day", "monthly"]
    rollcall_keys = ["pax", "seats released"]

    max_num_pages = 15

    for pdf in list_of_pdfs:
        # Skip link if the PDF is not downloaded successfully
        if pdf.get_local_path() is None:
            logging.error(
                "Unabled to complete full text search for pdf at %s",
                pdf.get_local_path(),
            )
            continue

        try:
            # Open the PDF file
            with open(pdf.get_local_path(), "rb") as file:
                # Create a PDF parser object associated with the file object
                parser = PDFParser(file)

                # Create a PDF document object that stores the document structure
                document = PDFDocument(parser)

                # Check number of pages
                if len(list(PDFPage.create_pages(document))) > max_num_pages:
                    # Skip this PDF if it's longer than 15 pages
                    logging.info(
                        "Skipping pdf: %s has more than 15 pages.", pdf.filename
                    )
                    pdf.seen_before = True
                    continue
        except PDFNoValidXRef:
            logging.error("PDFNoValidXRef error for pdf: %s", pdf.get_local_path())
            pdf.set_type("DISCARD")
            continue
        except PSEOF:
            logging.error("PSEOF error for pdf: %s", pdf.get_local_path())
            pdf.set_type("DISCARD")
            continue

        text = extract_text(pdf.get_local_path())
        text = text.lower().strip()

        # Roll calls
        if not found["ROLLCALL"]:
            if any(key in text for key in rollcall_keys):
                pdf.set_type("ROLLCALL")
                continue

            # Additional regex seach to match roll call pdfs
            if re.search(r"seats\s*released", text, re.DOTALL):
                pdf.set_type("ROLLCALL")
                continue

        # 30 Day schedules
        if not found["30_DAY"] and any(key in text for key in sch_30day_keys):
            pdf.set_type("30_DAY")
            continue

        # 72 Hour schedules
        # Check that all three strings are in the text.
        if not found["72_HR"] and any(key in text for key in sch_72hr_keys):
            pdf.set_type("72_HR")
            continue

        # No match found
        pdf.set_type("DISCARD")


def sort_terminal_pdfs(
    list_of_pdfs: List[Pdf],
) -> Tuple[Optional[Pdf], Optional[Pdf], Optional[Pdf]]:
    """Sort all the PDFs from a SINGLE terminal into different buckets.

    The type of PDFs are 72 hour schedules, 30 day schedules, and rollcalls. Only
    use this function with a list of PDFs from one terminal. Otherwise the sorting will
    not work as the sort relies on the context of the other types of pdfs.

    Args:
    ----
        list_of_pdfs (List[Pdf]): List of Pdf objects

    Returns:
    -------
        Tuple[Pdf, Pdf, Pdf]: Tuple of Pdf objects where the first element is the most recent 72 hour schedule,
        the second element is the most recent 30 day schedule, and the third element is the most recent rollcall.
    """
    # Booleans to filter with context
    found = {"72_HR": False, "30_DAY": False, "ROLLCALL": False}

    # Variables to hold the most up to date PDF
    # of each type for the terminal.
    pdf_72hr = None
    pdf_30day = None
    pdf_rollcall = None

    # Sort PDFs based on filename
    no_match_pdfs = type_pdfs_by_filename(list_of_pdfs, found)

    # If there is PDF that was not typed by filename
    if len(no_match_pdfs) > 0:
        # Sort PDFs that did not match any filename filters
        type_pdfs_by_content(no_match_pdfs, found)

    # Sort each pdf by modify date metadata where the
    # most recent pdf is at index 0.
    modify_sorted = sort_pdfs_by_modify_time(list_of_pdfs)

    for pdf in modify_sorted:
        if pdf_72hr is None and pdf.type == "72_HR":
            pdf_72hr = pdf
            continue

        if pdf_30day is None and pdf.type == "30_DAY":
            pdf_30day = pdf
            continue

        if pdf_rollcall is None and pdf.type == "ROLLCALL":
            pdf_rollcall = pdf
            continue

    # Sort each pdf by creation date metadata
    creation_sorted = sort_pdfs_by_creation_time(list_of_pdfs)

    for pdf in creation_sorted:
        if pdf_72hr is None and pdf.type == "72_HR":
            pdf_72hr = pdf
            continue

        if pdf_30day is None and pdf.type == "30_DAY":
            pdf_30day = pdf
            continue

        if pdf_rollcall is None and pdf.type == "ROLLCALL":
            pdf_rollcall = pdf
            continue

    # For PDFs who did not have a type determined
    # set their type to discard.
    for pdf in list_of_pdfs:
        if pdf.type == "":
            pdf.set_type("DISCARD")

    return pdf_72hr, pdf_30day, pdf_rollcall


def metadata_sorting_key(pdf: Pdf, attribute: str) -> str:
    """Generate a sorting key for Pdf metadata.

    Places Pdf objects with empty or irregular strings at the end.

    Args:
    ----
        pdf (Pdf): Pdf object
        attribute (str): Name of the Pdf attribute to use for sorting

    Returns:
    -------
        str: Sorting key for Pdf metadata
    """
    valid_date_length = 14

    value = getattr(pdf, attribute, "0")

    # If PDF does not have that metadata just set it
    # to '0'.
    if value is None:
        value = "0"
        setattr(pdf, attribute, "0")

    if len(value) == valid_date_length and value.isdigit():
        return value

    # Default to 0 if the string is not a valid
    return "0"


def sort_pdfs_by_modify_time(pdfs: List[Pdf]) -> List[Pdf]:
    """Sort a list of Pdf objects based on their modification date in descending order.

    Places Pdf objects with empty or irregular modify_time strings at the end.

    Args:
    ----
        pdfs (List[Pdf]): List of Pdf objects

    Returns:
    -------
        List[Pdf]: Sorted list of Pdf objects with most recent modification date first
    """
    return sorted(
        pdfs, key=lambda pdf: metadata_sorting_key(pdf, "modify_time"), reverse=True
    )


def sort_pdfs_by_creation_time(pdfs: List[Pdf]) -> List[Pdf]:
    """Sort a list of Pdf objects based on their creation date in descending order.

    Places Pdf objects with empty or irregular creation_time strings at the end.

    Args:
    ----
        pdfs (List[Pdf]): List of Pdf objects

    Returns:
    -------
        List[Pdf]: Sorted list of Pdf objects with most recent creation date first
    """
    return sorted(
        pdfs, key=lambda pdf: metadata_sorting_key(pdf, "creation_time"), reverse=True
    )


def type_pdfs_by_filename(list_of_pdfs: List[Pdf], found: Dict[str, bool]) -> List[Pdf]:
    """Sort a list of PDFs from ONE TERMINAL by filename using regex filters.

    Returns four different buckets of PDFs: 72 hour schedules, 30 day schedules,
    rollcalls, and no match PDFs that did not get picked up by any of
    the regexes.

    Args:
    ----
        list_of_pdfs (List[Pdf]): List of Pdf objects
        found (Dict[str, bool]): Dictionary of boolean flags indicating whether a PDF of a given type has been found

    Returns:
    -------
        List[Pdf]: List of Pdf objects that did not get matched by the regex filters
    """
    logging.info("Entering type_pdfs_by_filename()")

    # Inclusion regex filters
    regex_72_hr_name_filter = r"(?i)72[- _%20]{0,1}hr|72[- _%20]{0,1}hour"
    regex_30_day_name_filter = r"(?i)30[-_ ]?day"
    regex_rollcall_name_filter = r"(?i)(roll[-_ ]?call)|roll"

    # Exclusion regex filters
    exclusion_regex_filters = [
        r"(?i)amc[-_ ]?(pe[-_ ])?gram",  # AMC Gram
        r"(?i)(^|\W)pet(\W|$)|pet\w+",  # Pet
        r"(?i)(^|\W)brochure(\W|$)|brochure\w+",  # Brochure
        r"(?i)(^|\W)advice(\W|$)|advice\w+",  # Advice
        r"(?i)(^|\W)guidance(\W|$)|guidance\w+",  # Guidance
        r"(?i)(^|\W)question(\W|$)|question\w+",  # Question
        r"(?i)(^|\W)map(\W|$)|map\w+",  # Map
        r"(?i)(^|\W)flyer(\W|$)|flyer\w+",  # Flyer
    ]

    # Buckets for sorting PDFs into
    no_match_pdfs = []

    for pdf in list_of_pdfs:
        discard = False

        # Exclude PDFs that aren't of interest
        for regex in exclusion_regex_filters:
            if re.search(regex, pdf.filename):
                pdf.set_type("DISCARD")
                discard = True
                break

        if discard:
            continue

        # Check if the PDF is a 72 hour schedule
        if re.search(regex_72_hr_name_filter, pdf.filename):
            found["72_HR"] = True
            pdf.set_type("72_HR")
            continue

        # Check if the PDF is a 30 day schedule
        if re.search(regex_30_day_name_filter, pdf.filename):
            found["30_DAY"] = True
            pdf.set_type("30_DAY")
            continue

        # Check if the PDF is a rollcall
        if re.search(regex_rollcall_name_filter, pdf.filename):
            found["ROLLCALL"] = True
            pdf.set_type("ROLLCALL")
            continue

        # If the PDF filename did not get matched by the
        # regex filters.
        no_match_pdfs.append(pdf)

    return no_match_pdfs


def count_words_in_pdf(pdf_path: str) -> Optional[int]:
    """Count the number of words in a PDF.

    Args:
    ----
        pdf_path (str): Path to PDF file

    Returns:
    -------
        Optional[int]: Number of words in the PDF, None if an error occurred
    """
    try:
        word_count = 0

        # Process each page in the PDF
        for page_layout in extract_pages(pdf_path):
            for element in page_layout:
                if isinstance(element, LTTextContainer):
                    # Extract text and count words in each text container
                    text = element.get_text()
                    words = text.split()
                    word_count += len(words)

        return word_count
    except Exception as e:
        logging.error("An error occurred while counting words: %s", e)
        return None


def count_characters_in_pdf(pdf_path: str) -> Optional[int]:
    """Count the number of characters in a PDF.

    Args:
    ----
        pdf_path (str): Path to PDF file

    Returns:
    -------
        Optional[int]: Number of characters in the PDF, None if an error occurred
    """
    try:
        character_count = 0

        # Process each page in the PDF
        for page_layout in extract_pages(pdf_path):
            for element in page_layout:
                if isinstance(element, LTTextContainer):
                    # Count characters in each text container
                    for text_element in element:
                        if isinstance(text_element, LTChar):
                            character_count += len(text_element.get_text())

        return character_count
    except Exception as e:
        logging.error("An error occurred while counting characters: %s", e)
        return None


def count_pages_in_pdf(pdf_path: str) -> Optional[int]:
    """Count the number of pages in a PDF.

    Args:
    ----
        pdf_path (str): Path to PDF file

    Returns:
    -------
        Optional[int]: Number of pages in the PDF, None if an error occurred
    """
    try:
        # Open the PDF file
        with open(pdf_path, "rb") as file:
            reader = PyPDF2.PdfReader(file)
            return len(reader.pages)
    except Exception as e:
        logging.error("An error occurred while counting pages: %s", e)
        return None
