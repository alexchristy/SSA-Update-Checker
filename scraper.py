import re
from typing import List, Tuple
from pdfminer.pdfparser import PDFParser
from pdfminer.pdfdocument import PDFDocument
import requests
import hashlib
import os
import shutil
import inspect
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

valid_locations = ['AMC CONUS Terminals', 'EUCOM Terminals', 'INDOPACOM Terminals',
                   'CENTCOM Terminals', 'SOUTHCOM TERMINALS', 'Non-AMC CONUS Terminals',
                   'ANG & Reserve Terminals']

# Functions
def get_with_retry(url: str):

    delay = 2

    url = utils.ensure_url_encoded(url)

    for attempt in range(5):

        try:

            # Send GET request to the website
            reponse = requests.get(url)
            return reponse # Exit function if request was successful
        
        except Exception as e: # Catch any execeptions
            logging.error('Request to %s failed in %s.', url, inspect.stack()[1], exc_info=True)

            # If it was not the last attempt
            if attempt < 4:
                logging.info('Retrying request to %s in %d seconds...', url, delay)
                time.sleep(delay) # Wait before next attempt
                delay *= 2
            
            # It was last attempt
            else:
                logging.error('All attempts failed.')

    return None

def normalize_url(url: str):
    parsedUrl = urlparse(url)
    hostname = str(parsedUrl.netloc)
    normalizedUrl = 'https://' + hostname + '/'
    return normalizedUrl

def get_terminals(url: str):
    logging.debug('Running get_terminal_info().')

    # Download the AMC travel page
    response = get_with_retry(url)

    # Exit program if AMC travel page fails to download
    if response is None:
        logging.critical('Failed to download AMC Travel page. Exiting program...')
        raise

    '''
    ####################################
    # Find all terminals doing Space A #
    ####################################
    '''

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
                currentTerminal.name = name
                currentTerminal.link = href

        # Increment so next link and name is put in correct terminal object
        index += 1

    return listOfTerminals

def get_terminals_info(listOfTerminals: List[Terminal], baseDir: str) -> List[Terminal]:

    downloadDir = baseDir + 'tmp/'

    terminalsWithPDFLinks = []

    regex72HourFilter = r"(?i)72[- _%20]{0,1}hr|72[- _%20]{0,1}hour"
    regex30DayFilter = r"(?i)30[-_ ]?day"
    regexRollcallFilter = r"(?i)(roll[-_ ]?call)|roll"

    # Grab terminal pages
    for currentTerminal in listOfTerminals:

        # URL of terminal page
        url = currentTerminal.link
        
        # Skip terminals with no page
        if url == "empty":
            logging.warning('%s terminal has no terminal page link.', currentTerminal.name)
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

        # Create strings for naming pdfs.
        basePdfName = currentTerminal.name
        basePdfName = basePdfName.replace(" ", "_")

        # Create array for PDFs that do not match
        noMatchPdfs = []

        # Create bools to shorten searching time
        pdf72HourFound = False
        pdf30DayFound = False
        pdfRollcallFound = False

        for a_tag in a_tags:
            pdfLink = a_tag["href"]

            # Get name of PDF as it appears on site
            pdfName = utils.get_pdf_name(pdfLink)

            # Check if 72 hour schedule
            if not pdf72HourFound:
                if re.search(regex72HourFilter, pdfName):
                    currentTerminal.pdfLink72Hour = hostname + pdfLink
                    pdf72HourFound = True
                    continue

            # Check if 30 day schedule
            if not pdf30DayFound:
                if re.search(regex30DayFilter, pdfName):
                    currentTerminal.pdfLink30Day = hostname + pdfLink
                    pdf30DayFound = True
                    continue

            # Check if rollcall
            if not pdfRollcallFound:
                if re.search(regexRollcallFilter, pdfName):
                    currentTerminal.pdfLinkRollcall = hostname + pdfLink
                    pdfRollcallFound = True
                    continue

            # PDF's name did not match any of the regex filters
            noMatchPdfs.append(pdfLink)
        
        # If all PDFs are found continue to next terminal
        if pdf72HourFound and pdf30DayFound and pdfRollcallFound:
            terminalsWithPDFLinks.append(currentTerminal)
            continue

        # Not all PDFs found sort unmatched PDFs by content and return their paths
        pdfs72Hour, pdfs30Day, pdfsRollcall = sort_pdfs_by_content(downloadDir, noMatchPdfs)

        # Check each type of PDF type. If the type was already found skip sorting
        # that type of PDF. If the PDF type was not found sort the PDFs by modification
        # date or creation date. If the array has items in it set the link type in the 
        # terminal to the PDF with the most recent modification or creation date.
        if not pdf72HourFound:
            pdfs72Hour = sort_pdfs_by_date(pdfs72Hour)

            if pdfs72Hour is not None:
                currentTerminal.pdfLink72Hour = pdfs72Hour[0][0]
                dest = downloadDir + '72_HR/' + pdfs72Hour[0][1]
                shutil.move(pdfs72Hour[0][1], dest)
                currentTerminal.pdfName72Hour = dest
        
        if not pdf30DayFound:
            pdfs30Day = sort_pdfs_by_date(pdfs30Day)

            if pdfs30Day is not None:
                currentTerminal.pdfLink30Day = pdfs30Day[0][0]
                dest = downloadDir + '30_DAY/' + pdfs30Day[0][1]
                shutil.move(pdfs30Day[0][1], dest)
                currentTerminal.pdfName30Day = dest
        
        if not pdfRollcallFound:
            pdfsRollcall = sort_pdfs_by_date(pdfsRollcall)

            if pdfsRollcall is not None:
                currentTerminal.pdfLinkRollcall = pdfsRollcall[0][0]
                dest = downloadDir + 'ROLLCALL/' + pdfsRollcall[0][1]
                shutil.move(pdfsRollcall[0][1], dest)
                currentTerminal.pdfNameRollcall = dest

        terminalsWithPDFLinks.append(currentTerminal)
    
    return terminalsWithPDFLinks


def sort_pdfs_by_content(dir:str, pdfLinks: List[str]):

    pdf72HourOpts = []
    pdf30DayOpts = []
    pdfRollcallOpts = []

    sch72HourKeys = ['roll call', 'destination', 'seats']

    for link in pdfLinks:
        
        pdfPath = download_pdf(dir, link)

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

def sort_pdfs_by_date(pdfs: List[Tuple[str, str]]) -> List[str]:
    # Create a list to store tuples of (path, date)
    pdfs_with_dates = []
    pdfs_without_dates = []

    # If list is empty return None
    if len(pdfs) < 1:
        return None
    
    # If list has only one item return the array
    # as there is nothing to sort.
    if len(pdfs) == 1:
        return pdfs

    for link, path in pdfs:
        if not os.path.isfile(path):
            logging.warning('PDF: %s does not exist. %s called from: %s', path, inspect.stack()[0], inspect.stack()[1])
            continue

        with open(path, 'rb') as f:
            parser = PDFParser(f)
            doc = PDFDocument(parser)
            date = None

            if 'ModDate' in doc.info[0]:
                date = doc.info[0]['ModDate']
            elif 'CreationDate' in doc.info[0]:
                date = doc.info[0]['CreationDate']

            if date:
                # Extract the date
                date = str(date)
                date = datetime.strptime(date[2:16], "%Y%m%d%H%M%S")

                # Add the tuple to the list
                pdfs_with_dates.append((path, date))
            else:
                pdfs_without_dates.append(path)

    # Sort the list by the datetime objects
    pdfs_with_dates.sort(key=lambda x: x[1], reverse=True)

    # Extract the paths, now sorted by mod_date
    sorted_paths = [x[0] for x in pdfs_with_dates]

    # Add the PDFs without dates to the end
    sorted_paths.extend(pdfs_without_dates)

    return sorted_paths

        
def download_pdf(dir: str, url:str) -> str:

    # Get the filename from the URL
    filename = utils.get_pdf_name(url)

    # Send a GET request to the URL
    response = get_with_retry(url)

    # Check if the request is successful
    if response is not None:
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
    
def download_terminal_pdfs(terminal: Terminal, baseDir: str):

    pdf72HourDir = baseDir + "tmp/72_HR/"
    pdf30DayDir = baseDir + "tmp/30_DAY/"
    pdfRollcallDir = baseDir + "tmp/ROLLCALL/"

    # Download 72 Hour PDF
    if terminal.pdfLink72Hour != "empty":

        filename = download_pdf(pdf72HourDir, terminal.pdfLink72Hour)

        # If PDF was downloaded successfully
        if filename is not None:
            terminal.pdfName72Hour = filename
    
    # Download 30 Day PDF
    if terminal.pdfLink30Day != "empty":

        filename = download_pdf(pdf30DayDir, terminal.pdfLink30Day)

        # If PDF was downloaded successfully
        if filename is not None:
            terminal.pdfName30Day = filename
    
    if terminal.pdfLinkRollcall != "empty":

        filename = download_pdf(pdfRollcallDir, terminal.pdfLinkRollcall)

        # If PDF was downloaded successfully
        if filename is not None:
            terminal.pdfNameRollcall = filename

    return terminal

def calculate_sha256(file_path):
    sha256_hash = hashlib.sha256()

    with open(file_path, "rb") as f:
        # Read and update hash in chunks to avoid using too much memory
        for byte_block in iter(lambda: f.read(4096), b""):
            sha256_hash.update(byte_block)

    return sha256_hash.hexdigest()

def calc_terminal_pdf_hashes(terminal: Terminal):

    pdf72HourPath = terminal.pdfName72Hour
    pdf30DayPath = terminal.pdfName30Day
    pdfRollcallPath = terminal.pdfNameRollcall

    if os.path.exists(pdf72HourPath):
        terminal.pdfHash72Hour = calculate_sha256(pdf72HourPath)
        logging.debug('%s hash was calculated.', pdf72HourPath)
    else:
        logging.warning('%s was not found in %s. Is it missing?', pdf72HourPath, inspect.stack()[0])

    if os.path.exists(pdf30DayPath):
        terminal.pdfHash30Day = calculate_sha256(pdf30DayPath)
        logging.debug('%s hash was calculated.', pdf30DayPath)
    else:
        logging.warning('%s was not found in %s. Is it missing?', pdf30DayPath, inspect.stack()[0])
    
    if os.path.exists(pdfRollcallPath):
        terminal.pdfHashRollcall = calculate_sha256(pdfRollcallPath)
        logging.debug('%s hash was calculated.', pdfRollcallPath)
    else:
        logging.warning('%s was not found in %s. Is it missing?', pdfRollcallPath, inspect.stack()[0])

    return terminal