import datetime
import glob
import logging
import os
import re
from typing import Dict, List, Tuple
from pdfminer.pdfdocument import PDFDocument
from pdfminer.pdfparser import PDFParser
from pdfminer.pdfpage import PDFPage
from pdfminer.pdfdocument import PDFNoValidXRef
from pdfminer.psparser import PSEOF
from pdfminer.high_level import extract_text
from pdf import Pdf
import utils

def check_downloaded_pdfs(directory_path):
    """Check if at least one PDF was downloaded and log the number of PDFs in the directory."""
    num_pdf_files = len(glob.glob(os.path.join(directory_path, "*.pdf")))
    if num_pdf_files == 0:
        logging.warning("No PDFs were downloaded in the directory: %s", directory_path)
    else:
        logging.info('%d PDFs were downloaded in the directory: %s', num_pdf_files, directory_path)
    return num_pdf_files > 0

def type_pdfs_by_content(list_of_pdfs: List[Pdf], found: Dict[str, bool]):

    """
    Sort PDFs based on their text content. Give a list of
    PDF objects and it will set their type based on 
    """

    logging.debug('Entering type_pdfs_by_content()')

    # Disable excessive PDF logging
    logging.getLogger('pdfminer').setLevel(logging.INFO)

    # Define keys for searching the PDF text content
    sch72HourKeys = ['roll call', 'destination', 'seats']
    sch30DayKeys = ['30-day', 'monthly']
    rollcallKeys = ['pax', 'seats released']


    for pdf in list_of_pdfs:

        # Skip link if the PDF is not downloaded successfully
        if pdf.get_local_path() is None:
            logging.error(f'Unabled to complete full text search for pdf at {pdf.get_local_path()}')
            continue

        try:
            # Open the PDF file
            with open(pdf.get_local_path(), 'rb') as file:
                # Create a PDF parser object associated with the file object
                parser = PDFParser(file)

                # Create a PDF document object that stores the document structure
                document = PDFDocument(parser)

                # Check number of pages
                if len(list(PDFPage.create_pages(document))) > 15:
                    # Skip this PDF if it's longer than 15 pages
                    logging.info(f'Skipping pdf: {pdf.filename} has more than 15 pages.')
                    pdf.seen_before = True
                    continue
        except PDFNoValidXRef:
            logging.error(f'PDFNoValidXRef error for pdf: {pdf.get_local_path}')
            pdf.set_type('DISCARD')
            continue
        except PSEOF:
            logging.error(f'PSEOF error for pdf: {pdf.get_local_path}')
            pdf.set_type('DISCARD')
            continue
                
        text = extract_text(pdf.get_local_path())
        text = text.lower().strip()

        # Roll calls
        if not found['ROLLCALL']:
            if any(key in text for key in rollcallKeys):
                pdf.set_type('ROLLCALL')
                continue

            # Additional regex seach to match roll call pdfs
            if re.search(r'seats\s*released', text, re.DOTALL):
                pdf.set_type('ROLLCALL')
                continue

        # 30 Day schedules
        if not found['30_DAY']:
            if any(key in text for key in sch30DayKeys):
                pdf.set_type('30_DAY')
                continue

        # 72 Hour schedules
        # Check that all three strings are in the text.
        if not found['72_HR']:
            if any(key in text for key in sch72HourKeys):
                pdf.set_type('72_HR')
                continue

        # No match found
        pdf.set_type('DISCARD')
        
def sort_terminal_pdfs(list_of_pdfs: List[Pdf]) -> Tuple[Pdf, Pdf, Pdf]:

    """
    Sort all the PDFs from a SINGLE terminal into different buckets. The
    type of PDFs are 72 hour schedules, 30 day schedules, and rollcalls. Only
    use this function with a list of one terminal. Otherwise the sorting will
    not work as the sort relies on the context of the other types of pdfs.
    """

    # Booleans to filter with context
    found = {
        '72_HR': False,
        '30_DAY': False,
        'ROLLCALL': False
    }

    # Variables to hold the most up to date PDF
    # of each type for the terminal.
    pdf72Hour = None
    pdf30Day = None
    pdfRollcall = None

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

        if pdf72Hour is None:
            if pdf.type == '72_HR':
                pdf72Hour = pdf
                continue
        
        if pdf30Day is None:
            if pdf.type == '30_DAY':
                pdf30Day = pdf
                continue
        
        if pdfRollcall is None:
            if pdf.type == 'ROLLCALL':
                pdfRollcall = pdf
                continue

    # Sort each pdf by creation date metadata
    creation_sorted = sort_pdfs_by_creation_time(list_of_pdfs)

    for pdf in creation_sorted:

        if pdf72Hour is None:
            if pdf.type == '72_HR':
                pdf72Hour = pdf
                continue
        
        if pdf30Day is None:
            if pdf.type == '30_DAY':
                pdf30Day = pdf
                continue
        
        if pdfRollcall is None:
            if pdf.type == 'ROLLCALL':
                pdfRollcall = pdf
                continue

    # For PDFs who did not have a type determined
    # set their type to discard.
    for pdf in list_of_pdfs:
        if pdf.type == "":
            pdf.set_type('DISCARD')
    
    return pdf72Hour, pdf30Day, pdfRollcall

def metadata_sorting_key(pdf: Pdf, attribute: str) -> str:
    """
    Generate a sorting key for Pdf metadata.
    Places Pdf objects with empty or irregular strings at the end.
    
    :param pdf: Pdf object
    :param attribute: Attribute name of Pdf object used for sorting (e.g., 'modify_time', 'creation_time')
    :return: Sorting key for the Pdf object
    """
    value = getattr(pdf, attribute, '0')

    # If PDF does not have that metadata just set it 
    # to '0'.
    if value is None:
        value = '0'
        setattr(pdf, attribute, '0')

    if len(value) == 14 and value.isdigit():
        return value
    else:
        return '0'  # Default value for empty or irregular strings

def sort_pdfs_by_modify_time(pdfs: List[Pdf]) -> List[Pdf]:
    """
    Sorts a list of Pdf objects based on their modification date in descending order.
    Places Pdf objects with empty or irregular modify_time strings at the end.
    
    :param pdfs: List of Pdf objects
    :return: Sorted list of Pdf objects with most recent modification date first
    """
    return sorted(pdfs, key=lambda pdf: metadata_sorting_key(pdf, 'modify_time'), reverse=True)

def sort_pdfs_by_creation_time(pdfs: List[Pdf]) -> List[Pdf]:
    """
    Sorts a list of Pdf objects based on their creation date in descending order.
    Places Pdf objects with empty or irregular creation_time strings at the end.
    
    :param pdfs: List of Pdf objects
    :return: Sorted list of Pdf objects with most recent creation date first
    """
    return sorted(pdfs, key=lambda pdf: metadata_sorting_key(pdf, 'creation_time'), reverse=True)

def type_pdfs_by_filename(list_of_pdfs: List[Pdf], found: Dict[str, bool]) -> List[Pdf]:

    """
    Sort a list of PDFs from ONE TERMINAL by filename using regex filters.
    Returns four different buckets of PDFs: 72 hour schedules, 30 day schedules,
    rollcalls, and no match PDFs that did not get picked up by any of
    the regexes. 
    """

    logging.info(f'Entering type_pdfs_by_filename()')

    # Inclusion regex filters
    regex_72_hr_name_filter = r"(?i)72[- _%20]{0,1}hr|72[- _%20]{0,1}hour"
    regex_30_day_name_filter = r"(?i)30[-_ ]?day"
    regex_rollcall_name_filter = r"(?i)(roll[-_ ]?call)|roll"

    # Exclusion regex filters
    exclusion_regex_filters = [
        r'(?i)amc[-_ ]?(pe[-_ ])?gram', # AMC Gram
        r'(?i)(^|\W)pet(\W|$)|pet\w+', # Pet
        r'(?i)(^|\W)brochure(\W|$)|brochure\w+', # Brochure
        r'(?i)(^|\W)advice(\W|$)|advice\w+', # Advice
        r'(?i)(^|\W)guidance(\W|$)|guidance\w+', # Guidance
        r'(?i)(^|\W)question(\W|$)|question\w+', # Question
        r'(?i)(^|\W)map(\W|$)|map\w+', # Map
        r'(?i)(^|\W)flyer(\W|$)|flyer\w+'
    ]

    # Buckets for sorting PDFs into
    no_match_pdfs = []

    for pdf in list_of_pdfs:
        discard = False
        
        # Exclude PDFs that aren't of interest
        for regex in exclusion_regex_filters:
            if re.search(regex, pdf.filename):
                pdf.set_type('DISCARD')
                discard = True
                break
                
        if discard:
            continue
        
        # Check if the PDF is a 72 hour schedule
        if re.search(regex_72_hr_name_filter, pdf.filename):
            found['72_HR'] = True
            pdf.set_type('72_HR')
            continue
    
        # Check if the PDF is a 30 day schedule
        if re.search(regex_30_day_name_filter, pdf.filename):
            found['30_DAY'] = True
            pdf.set_type('30_DAY')
            continue
    
        # Check if the PDF is a rollcall
        if re.search(regex_rollcall_name_filter, pdf.filename):
            found['ROLLCALL'] = True
            pdf.set_type('ROLLCALL')
            continue
        
        # If the PDF filename did not get matched by the 
        # regex filters.
        no_match_pdfs.append(pdf)

    return no_match_pdfs

