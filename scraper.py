import re
import time
from typing import List, Optional, Tuple
from PyPDF2 import PdfReader
from pdfminer.pdfdocument import PDFDocument
from pdfminer.pdfparser import PDFParser
from pdfminer.pdfpage import PDFPage
import requests
import hashlib
import os
from terminal import *
from mongodb import *
from bs4 import BeautifulSoup
from urllib.parse import urlparse, urljoin
from urllib.parse import quote
from mongodb import *
from urllib.parse import urlparse
import logging
from datetime import datetime
from pdfminer.high_level import extract_text
import utils
from mongodb import MongoDB

valid_locations = ['AMC CONUS Terminals', 'EUCOM Terminals', 'INDOPACOM Terminals',
                   'CENTCOM Terminals', 'SOUTHCOM TERMINALS', 'Non-AMC CONUS Terminals',
                   'ANG & Reserve Terminals']

# Functions
def get_with_retry(url: str):
    logging.debug('Entering get_with_retry() requesting: %s', url)

    delay = 2

    url = utils.ensure_url_encoded(url)

    for attempt in range(3):

        try:

            # Send GET request to the website
            reponse = requests.get(url)
            return reponse # Exit function if request was successful
        
        except Exception as e: # Catch any execeptions
            logging.error('Request to %s failed in get_with_retry().', url, exc_info=True)

            # If it was not the last attempt
            if attempt < 2:
                logging.info('Retrying request to %s in %d seconds...', url, delay)
                time.sleep(delay) # Wait before next attempt
                delay *= 2
            
            # It was last attempt
            else:
                logging.error('All attempts failed.')

    return None

def normalize_url(url: str):
    logging.debug('Entering normarlize_url()')

    parsedUrl = urlparse(url)
    hostname = str(parsedUrl.netloc)
    normalizedUrl = 'https://' + hostname + '/'
    return normalizedUrl

def get_terminals(url: str) -> List[Terminal]:
    logging.debug('Running get_terminal_info().')

    # Download the AMC travel page
    response = get_with_retry(url)

    # Exit program if AMC travel page fails to download
    if response is None:
        logging.critical('Failed to download AMC Travel page. Exiting program...')
        raise

    # Create empty array of terminals to store data
    listOfTerminals = []

    # Create a BeautifulSoup object from the response content
    soup = BeautifulSoup(response.content, "html.parser")

    # Save all menus
    # Find all <li> tags with the specified criteria
    # Find all <li> tags with specific attributes
    tags = soup.find_all('li', class_='af3AccordionMenuListItem', attrs={'data-index': True, 'tabindex': True, 'aria-expanded': True, 'title': True})

    # Save the group names and the location data
    group = ""
    pagePositon = 1
    filteredTags = [] # Exclude tags with references to groups for processing later
    for tag in tags:
        title = tag.get('title')  # get the title attribute

        # Check if the current title is a group name
        if str(title).lower() in (location.lower() for location in valid_locations):
            group = str(title).upper() # Saved in all caps to match military convention and website
            continue

        # Not a valid terminal name skip
        title_lower = str(title).lower()
        if ',' not in title_lower and 'terminal' not in title_lower or 'click' in title_lower:
            continue

        # If a valid group is set add it to new terminals
        if group != "":
            newTerminal = Terminal()
            newTerminal.group = group
            newTerminal.location = str(title)
            newTerminal.pagePosition = pagePositon
            listOfTerminals.append(newTerminal)

            # Increment page position only for terminal entries
            pagePositon += 1

            # Save valid terminal tag
            filteredTags.append(tag)


    # Define the filter strings for names and link words
    name_filter_strings = ["Transportation Function", "Passenger Terminal", "Air Terminal", "AFB"]
    link_filter_words = ["Terminal", "Passenger", "Transportation", "Gateway"]
    
    # len(tags) == len(listOfTerminals) <-- This will always be true
    index = 0
    # Iterate though the terminal tags and save page links and terminal names to the terminal objects
    for tag in filteredTags:

        a_tags = tag.find_all("a", href=True, target="_blank")

        for a_tag in a_tags:
            href = a_tag["href"]
            name = a_tag.text

            # Check if the name contains any of the filter strings
            name_matches_filter = any(filter_str in name for filter_str in name_filter_strings)

            # Check if the href contains any of the filter words
            link_matches_filter = any(word in href for word in link_filter_words)

            # If both conditions are met, save the page link and terminal name to terminal object
            if name_matches_filter and link_matches_filter:

                # Check if the link starts with "https://"
                if not href.startswith("https://"):
                    # Prepend the base URL to the link
                    base_url = "https://www.amc.af.mil"
                    href = urljoin(base_url, href)

                currentTerminal = listOfTerminals[index]

                # Save the name and link
                currentTerminal.name = name.strip()
                currentTerminal.link = href

        # Increment so next link and name is put in correct terminal object
        index += 1

    return listOfTerminals

def get_terminals_info(db: MongoDB):
    logging.debug('Entering get_terminals_info().')

    # Define regex filters for PDF names
    regex72HourFilter = r"(?i)72[- _%20]{0,1}hr|72[- _%20]{0,1}hour"
    regex30DayFilter = r"(?i)30[-_ ]?day"
    regexRollcallFilter = r"(?i)(roll[-_ ]?call)|roll"

    baseDir = os.getenv('PDF_DIR')

    # Retrieve all stored terminals in Mongo
    listOfTerminals = db.get_all_terminals()

    # Grab terminal pages
    for currentTerminal in listOfTerminals:

        # URL of terminal page
        url = currentTerminal.link
        
        # Skip terminals with no page and remove from DB
        if url == "empty":
            logging.warning('%s terminal has no terminal page link.', currentTerminal.name)
            logging.info(f'Removing {currentTerminal.name} with no page link...')
            db.remove_by_field_value('name', currentTerminal.name)
            continue

        logging.debug('Downloading %s terminal page.', currentTerminal.name)

        # Get terminal page with retry mechanism
        response = get_with_retry(url)

        # If terminal page is not downloaded correctly continue to next terminal
        if response is None:
            continue

        # Get hostname of terminal page
        hostname = normalize_url(url)

        # Create a BeautifulSoup object from the response content
        soup = BeautifulSoup(response.content, "html.parser")

        # Find all <a> tags with href and PDF file extension
        a_tags = soup.find_all('a', attrs={'target': True}, href=lambda href: href and '.pdf' in href)

        # Create array for PDFs that do not match
        noMatchPdfs = []

        # Create bools to shorten searching time
        pdf72HourFound = False
        pdf30DayFound = False
        pdfRollcallFound = False

        for a_tag in a_tags:
            extractedPdfLink = a_tag["href"]

            # Correct encoding from BS4
            pdfLink = extractedPdfLink.replace('×tamp', '&timestamp')

            # Correct relative pdf links
            if not pdfLink.lower().startswith('https://'):
                pdfLink = hostname + pdfLink

            # Get name of PDF as it appears on site
            pdfName = utils.get_pdf_name(pdfLink)

            # Check if 72 hour schedule
            if not pdf72HourFound:
                # If its a 72 hour pdf
                if re.search(regex72HourFilter, pdfName):
                    # Update DB
                    db.set_terminal_field(currentTerminal.name, 'pdfLink72Hour', pdfLink)
                    pdf72HourFound = True
                    continue

            # Check if 30 day schedule
            if not pdf30DayFound:
                # If its a 30 day pdf
                if re.search(regex30DayFilter, pdfName):
                    # Update DB
                    db.set_terminal_field(currentTerminal.name, 'pdfLink30Day', pdfLink)
                    pdf30DayFound = True
                    continue

            # Check if rollcall
            if not pdfRollcallFound:
                # If its a rollcall pdf
                if re.search(regexRollcallFilter, pdfName):
                    # Update DB
                    db.set_terminal_field(currentTerminal.name, 'pdfLinkRollcall', pdfLink)
                    pdfRollcallFound = True
                    continue

            # PDF's name did not match any of the regex filters
            noMatchPdfs.append(pdfLink)
        
        # If all PDFs are found continue to next terminal
        if pdf72HourFound and pdf30DayFound and pdfRollcallFound:
            continue

        # Not all PDFs found sort unmatched PDFs by content and return their paths
        downloadDir = baseDir + 'tmp/'
        pdfs72Hour, pdfs30Day, pdfsRollcall = sort_pdfs_by_content(downloadDir, noMatchPdfs)

        # Check each type of PDF type. If the type was already found skip sorting
        # that type of PDF. If the PDF type was not found sort the PDFs by modification
        # date or creation date. If the array has items in it set the link type in the 
        # terminal to the PDF with the most recent modification or creation date.
        if not pdf72HourFound:
            newest72HourPdf = get_newest_pdf(pdfs72Hour)

            # If there are 72 Hour PDFs
            if newest72HourPdf is not None:
                # Store the link to the PDF
                link = newest72HourPdf
                db.set_terminal_field(currentTerminal.name, 'pdfLink72Hour', link)
 
                # We have found the 72 hour schedule
                pdf72HourFound = True

        # Remove unused downloaded PDFs from the full
        # text PDF search.
        logging.info(f'Removing {len(pdfs72Hour)} left over 72 hour PDFs from {currentTerminal.name} full text search.')
        for link, path in pdfs72Hour:
            logging.debug('Removing 72 hour pdf: %s', path)
            os.remove(path)

        if not pdf30DayFound:
            newest30DayPdf = get_newest_pdf(pdfs30Day)

            if newest30DayPdf is not None:
                # Store the link to the PDF
                link = newest30DayPdf
                db.set_terminal_field(currentTerminal.name, 'pdfLink30Day', link)
               
                # We have found the 30 day schedule
                pdf30DayFound = True
            
        # Remove unused downloaded PDFs from the full
        # text PDF search.
        logging.info(f'Removing {len(pdfs30Day)} left over 30 Day PDFs from {currentTerminal.name} full text search.')
        for link, path in pdfs30Day:
            logging.debug('Removing 30 day pdf: %s', path)
            os.remove(path)

        if not pdfRollcallFound:
            newestRollcallPdf = get_newest_pdf(pdfsRollcall)

            if newestRollcallPdf is not None:
                # Store the link to the PDF
                link = newestRollcallPdf
                db.set_terminal_field(currentTerminal.name, 'pdfLinkRollcall', link)

                # We have found the rollcall
                pdfRollcallFound = True

        # Remove unused downloaded PDFs from the full
        # text PDF search.
        logging.info(f'Removing {len(pdfsRollcall)} left over rollcall PDFs from {currentTerminal.name} full text search.')
        for link, path in pdfsRollcall:
            logging.debug('Removing rollcall pdf: %s', path)
            os.remove(path)


def sort_pdfs_by_content(dir:str, pdfLinks: List[str]) -> List[Tuple[str, str]]:
    logging.debug('Entering sort_pdfs_by_content()')

    # Disable excessive PDF logging
    logging.getLogger('pdfminer').setLevel(logging.INFO)

    pdf72HourOpts = []
    pdf30DayOpts = []
    pdfRollcallOpts = []

    sch72HourKeys = ['roll call', 'destination', 'seats']

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
        text = text.lower()

        # Roll calls
        if 'pax' in text:
            pdfRollcallOpts.append((link, pdfPath))
            continue
        
        # 30 Day schedules
        if '30-day' in text or 'monthly' in text:
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

def get_newest_pdf(pdfs: List[Tuple[str, str]]) -> Optional[Tuple[str, str]]:
    logging.debug('Entering get_newest_pdf().')

    # Create a list to store tuples of (link, path, date)
    pdfs_with_dates = []
    pdfs_no_dates = []
    
    # If list is empty return None
    if len(pdfs) < 1:
        return None
    
    # If there is only one item in the list return the only item
    if len(pdfs) == 1:
        return pdfs[0]

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
        return pdfs_no_dates[0]

def download_pdf(dir: str, url:str) -> str:
    logging.debug('Entering download_pdf()')

    # Get the filename from the URL
    filename = utils.get_pdf_name(url)

    # Send a GET request to the URL
    response = get_with_retry(url)

    # Check if the request is successful
    if response is not None and response.status_code == 200:
        # Make sure the directory exists, if not, create it.
        os.makedirs(dir, exist_ok=True)
        
        # Combine the directory with the filename
        filepath = os.path.join(dir, filename)
        
        # Write the content of the request to a file
        with open(filepath, 'wb') as f:
            f.write(response.content)

        logging.debug('Successfully downloaded: %s. Saved at: %s', url, filepath)

        return filepath

    else:
        logging.warning('Failed to download: %s', url)
        return None
    
def download_terminal_pdfs(terminal: Terminal, baseDir: str) -> Terminal:
    logging.debug('Entering download_terminal_pdfs().')

    pdf72HourSubPath = "tmp/72_HR/"
    pdf30DaySubPath = "tmp/30_DAY/"
    pdfRollcallSubPath = "tmp/ROLLCALL/"

    pdf72HourDownloadDir = baseDir + pdf72HourSubPath
    pdf30DayDownloadDir = baseDir + pdf30DaySubPath
    pdfRollcallDownloadDir = baseDir + pdfRollcallSubPath

    # Download 72 Hour PDF
    if terminal.pdfLink72Hour != "empty":

        filename = download_pdf(pdf72HourDownloadDir, terminal.pdfLink72Hour)

        # If PDF was downloaded successfully
        if filename is not None:
            # Get relative path for compatability when changing PDF_DIR
            relativePath = utils.get_relative_path(pdf72HourSubPath, filename)

            terminal.pdfName72Hour = relativePath

    # Download 30 Day PDF
    if terminal.pdfLink30Day != "empty":

        filename = download_pdf(pdf30DayDownloadDir, terminal.pdfLink30Day)

        # If PDF was downloaded successfully
        if filename is not None:
            # Get relative path for compatability when changing PDF_DIR
            relativePath = utils.get_relative_path(pdf30DaySubPath, filename)

            terminal.pdfName30Day = relativePath
    
    # Download Rollcall PDF
    if terminal.pdfLinkRollcall != "empty":

        filename = download_pdf(pdfRollcallDownloadDir, terminal.pdfLinkRollcall)

        # If PDF was downloaded successfully
        if filename is not None:
            # Get relative path for compatability when changing PDF_DIR
            relativePath = utils.get_relative_path(pdfRollcallSubPath, filename)

            terminal.pdfNameRollcall =  relativePath
      
    return terminal

def calculate_sha256(file_path):
    logging.debug('Entering calculate_sha256().')

    sha256_hash = hashlib.sha256()

    with open(file_path, "rb") as f:
        # Read and update hash in chunks to avoid using too much memory
        for byte_block in iter(lambda: f.read(4096), b""):
            sha256_hash.update(byte_block)

    return sha256_hash.hexdigest()

def calc_terminal_pdf_hashes(terminal: Terminal) -> Terminal:
    logging.debug('Entering calc_terminal_pdf_hashes().')

    baseDir = os.getenv('PDF_DIR')

    pdf72HourPath = baseDir + terminal.pdfName72Hour
    pdf30DayPath = baseDir + terminal.pdfName30Day
    pdfRollcallPath = baseDir + terminal.pdfNameRollcall

    if os.path.exists(pdf72HourPath):
        terminal.pdfHash72Hour = calculate_sha256(pdf72HourPath)
        logging.debug('%s hash was calculated.', pdf72HourPath)
    else:
        logging.warning('72 hour schedule PDF was not found for %s in calc_terminal_pdf_hashes(). Is it missing?', terminal.name)

    if os.path.exists(pdf30DayPath):
        terminal.pdfHash30Day = calculate_sha256(pdf30DayPath)
        logging.debug('%s hash was calculated.', pdf30DayPath)
    else:
        logging.warning('30 day schedule PDF was not found for %s in calc_terminal_pdf_hashes(). Is it missing?', terminal.name)
    
    if os.path.exists(pdfRollcallPath):
        terminal.pdfHashRollcall = calculate_sha256(pdfRollcallPath)
        logging.debug('%s hash was calculated.', pdfRollcallPath)
    else:
        logging.warning('Rollcall PDF was not found for %s in calc_terminal_pdf_hashes(). Is it missing?', terminal.name)

    return terminal