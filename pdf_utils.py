import datetime
import glob
import logging
import os
import re
from typing import List, Tuple
from urllib.parse import unquote, urlparse
import uuid
from PyPDF2 import PdfReader
from pdfminer.pdfdocument import PDFDocument
from pdfminer.pdfparser import PDFParser
from pdfminer.pdfpage import PDFPage
from pdfminer.high_level import extract_text
from pdf import Pdf

def check_downloaded_pdfs(directory_path):
    """Check if at least one PDF was downloaded and log the number of PDFs in the directory."""
    num_pdf_files = len(glob.glob(os.path.join(directory_path, "*.pdf")))
    if num_pdf_files == 0:
        logging.warning("No PDFs were downloaded in the directory: %s", directory_path)
    else:
        logging.info('%d PDFs were downloaded in the directory: %s', num_pdf_files, directory_path)
    return num_pdf_files > 0


def get_pdf_name(url) -> str:
    try:
        result = urlparse(url)
        path = unquote(result.path)
        return path.split('/')[-1]
    except Exception as e:
        return str(e)
          
def gen_pdf_archive_name(terminalName, nameModifier):
    # Replace spaces with underscore in terminalName
    terminalName = terminalName.replace(' ', '_')
    
    # Get current date and time
    now = datetime.datetime.now()
    
    # Format date and time as per your requirement
    timestamp = now.strftime('%d-%b-%Y_%H%M')

    # Generate new name
    new_name = f"{terminalName}_{nameModifier}_{timestamp}.pdf"

    # Add uuid
    new_name = gen_pdf_name_uuid10(new_name)
    
    return new_name

def gen_pdf_name_uuid10(file_path):
    # Extract the directory, base name, and extension
    dir_path, file_name = os.path.split(file_path)
    base_name, ext = os.path.splitext(file_name)
    
    # Ensure the file is a PDF
    if ext.lower() != '.pdf':
        logging.error("%s is not a PDF!", file_path)
    
    # Generate a random UUID and take the first few characters for brevity
    random_uuid = str(uuid.uuid4())[:10]
    
    # Construct the new file name
    new_name = f"{base_name}_{random_uuid}.pdf"
    
    return new_name

def format_pdf_metadata_date(date_str):
    if date_str is None:
        return None
    
    # Regular expression to extract the date components
    match = re.match(r"D:(\d{4})(\d{2})(\d{2})(\d{2})(\d{2})(\d{2})", date_str)
    if match:
        # Format the date to YYYYMMDDHHMMSS
        return ''.join(match.groups())
    else:
        return None

def sort_pdfs_by_content(dir:str, pdfLinks: List[str]) -> List[Tuple[str, str]]:
    logging.debug('Entering sort_pdfs_by_content()')

    # Disable excessive PDF logging
    logging.getLogger('pdfminer').setLevel(logging.INFO)

    pdf72HourOpts = []
    pdf30DayOpts = []
    pdfRollcallOpts = []

    sch72HourKeys = ['roll call', 'destination', 'seats']
    sch30DayKeys = ['30-day', 'monthly']
    rollcallKeys = ['pax', 'seats released']

    for link in pdfLinks:
        
        pdfPath = download_pdf(dir, link)

        # Skip link if the PDF is not downloaded successfully
        if pdfPath is None:
            continue

        # Open the PDF file
        with open(pdfPath, 'rb') as file:
            # Create a PDF parser object associated with the file object
            parser = PDFParser(file)

            # Create a PDF document object that stores the document structure
            document = PDFDocument(parser)

            # Check number of pages
            if len(list(PDFPage.create_pages(document))) > 15:
                # Skip this PDF if it's longer than 15 pages
                logging.info('Skipping pdf: %s has more than 15 pages.', pdfPath)
                os.remove(pdfPath)
                continue

        text = extract_text(pdfPath)
        text = text.lower().strip()

        # Roll calls
        if any(key in text for key in rollcallKeys):
            pdfRollcallOpts.append((link, pdfPath))
            continue

        # Additional regex seach to match roll call pdfs
        if re.search(r'seats\s*released', text, re.DOTALL):
            pdfRollcallOpts.append((link, pdfPath))
            continue
        
        # 30 Day schedules
        if any(key in text for key in sch30DayKeys):
            pdf30DayOpts.append((link, pdfPath))
            continue

        # 72 Hour schedules
        # Check that all three strings are in the text.
        if all(key in text for key in sch72HourKeys):
            pdf72HourOpts.append((link, pdfPath))
            continue
        
        # If the PDF did not match any condition, delete it
        os.remove(pdfPath)

    return pdf72HourOpts, pdf30DayOpts, pdfRollcallOpts

def get_newest_pdf(pdfs: List[Tuple[str, str]]) -> str:
    logging.debug('Entering get_newest_pdf().')

    # Create a list to store tuples of (link, path, date)
    pdfs_with_dates = []
    pdfs_no_dates = []
    
    # If list is empty return None
    if len(pdfs) < 1:
        return None
    
    # If there is only one item in the list return the only item
    if len(pdfs) == 1:
        return pdfs[0][0]

    for link, path in pdfs:
        if not os.path.isfile(path):
            logging.warning('PDF: %s does not exist in get_newest_pdf().', path)
            continue

        with open(path, 'rb') as f:
            pdf = PdfReader(f)
            date = None

            # Replace getDocumentInfo().modifiedDate and getDocumentInfo().createdDate with metadata
            if 'ModDate' in pdf.metadata:
                date = pdf.metadata['ModDate']
            elif 'CreationDate' in pdf.metadata:
                date = pdf.metadata['CreationDate']

            if date:
                # Extract the date
                date = str(date)
                date = datetime.strptime(date[2:16], "%Y%m%d%H%M%S")

                # Add the tuple to the list
                pdfs_with_dates.append((link, path, date))
            else:
                pdfs_no_dates.append((link, path))

    # Sort the list by the datetime objects
    pdfs_with_dates.sort(key=lambda x: x[2], reverse=True)

    # Return only the newest PDF, if one exists.
    # Returning only the link and file path.
    if pdfs_with_dates:
        return pdfs_with_dates[0][0]
    
    # No PDFs with date metadata exist
    if pdfs_no_dates:
        return pdfs_no_dates[0][0]
    
def sort_terminal_pdfs(list_of_pdfs: List[Pdf]):

    """
    Sort all the PDFs from a SINGLE terminal into different buckets. The
    type of PDFs are 72 hour schedules, 30 day schedules, and rollcalls. Only
    use this function with a list of one terminal. Otherwise the sorting will
    not work as the sort relies on the context of the other types of pdfs.
    """
    for pdf in list_of_pdfs:

        continue

def sort_pdfs_by_filename(list_of_pdfs: List[Pdf]):

    """
    Sort a list of PDFs from ONE TERMINAL by filename using regex filters.
    Returns four different buckets of PDFs: 72 hour schedules, 30 day schedules,
    rollcalls, and no match PDFs that did not get picked up by any of
    the regexes. 
    """
        
    for pdf in list_of_pdfs:
        # Create bools to shorten sorting time
        pdf_72_hr_found = False
        pdf_30_day_found = False
        pdf_rollcall_found = False

        # Define regex filters for PDF names
        regex_72_hr_name_filter = r"(?i)72[- _%20]{0,1}hr|72[- _%20]{0,1}hour"
        regex_30_day_name_filter = r"(?i)30[-_ ]?day"
        regex_rollcall_name_filter = r"(?i)(roll[-_ ]?call)|roll"

        if re.search(regex_72_hr_name_filter, pdf.filename):
            pdf_72_hr_found = True
