import re
import requests
import hashlib
import os
from terminal import *
from bs4 import BeautifulSoup
from urllib.parse import urlparse, urljoin
from urllib.parse import quote
from mongodb import *
from urllib.parse import urlparse
import logging

valid_locations = ['AMC CONUS Terminals', 'EUCOM Terminals', 'INDOPACOM Terminals',
                   'CENTCOM Terminals', 'SOUTHCOM TERMINALS', 'Non-AMC CONUS Terminals',
                   'ANG & Reserve Terminals']

# Functions
def calculateFileHash(file_path):
    logging.debug('Running calculateFileHash() on %s', file_path)

    md5_hash = hashlib.md5()
    with open(file_path, 'rb') as file:
        for chunk in iter(lambda: file.read(4096), b''):
            md5_hash.update(chunk)
    return md5_hash.hexdigest()

def getTerminalInfo(db, url):
    logging.debug('Running getTerminalInfo().')

    # Wrapped reponse.get(url) to prevent program exiting upon initial error
    # and so it will retry before failing.
    delay = 2  # Initialize delay time

    for attempt in range(5):
        try:

            # Send a GET request to the website
            response = requests.get(url)
            break  # If we've made it this far without an exception, the request succeeded, so exit the loop
        
        except Exception as e: # Catch any exceptions
            logging.error('Request to download AMC homepage failed in getTerminalInfo()', exc_info=True)

            if attempt < 4: # If this wasn't the last attempt
                logging.info('Retring download of AMC homepage in %d seconds...', delay)
                time.sleep(delay) # Wait before next attempt
                delay *= 2 # Double delay time

            else: # If this was the last attempt re-raise the exception and exit program
                logging.critical('All attempts to download the AMC homepage failed. Exiting program...')
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
                currentTerminal.name = name
                currentTerminal.link = href

        # Increment so next link and name is put in correct terminal object
        index += 1

    # Grab terminal pages
    for currentTerminal in listOfTerminals:

        logging.debug('Downloading %s terminal page.', currentTerminal.name)

        # URL of terminal page
        url = currentTerminal.link
        
        # Skip terminals with no page
        if url == "empty":
            logging.warning('%s terminal has no terminal page link.', currentTerminal.name)
            continue

        # Wrapped reponse.get(url) to prevent program exiting upon error
        # and so it will retry before failing.
        delay = 2  # Initialize delay time

        for attempt in range(5):  # Attempt the request up to 5 times
            try:
                response = requests.get(url)
                break  # If we've made it this far without an exception, the request succeeded, so exit the loop

            except Exception as e:  # Catch any exceptions
                logging.error('Request to download %s terminal page failed in getTerminalInfo().', currentTerminal.name ,exc_info=True)

                if attempt < 4:  # If this wasn't the last attempt...
                    logging.info('Retrying download of %s terminal page in %d seconds...', currentTerminal.name, delay)
                    time.sleep(delay)  # Wait before the next attempt
                    delay *= 2  # Double the delay time

                else:  # If this was the last attempt, re-raise the exception
                    logging.error('All attemps to download %s terminal page failed. Skipping...', currentTerminal.name)


        # Get hostname of terminal page
        parsed_url = urlparse(response.url)
        hostname = str(parsed_url.netloc)
        hostname = 'https://' + hostname + '/'

        # Create a BeautifulSoup object from the response content
        soup = BeautifulSoup(response.content, "html.parser")

        # Find all <a> tags with href and PDF file extension
        a_tags = soup.find_all('a', attrs={'target': True}, href=lambda href: href and '.pdf' in href)

        # Bools to stop searching URLs when all PDFs are found
        pdf3DayFound = False
        pdf30DayFound = False
        pdfRollcallFound = False

        # Regex filters for the different PDFs
        regex3DayFilter = r"(?i)72[- _%20]{0,1}hr|72[- _%20]{0,1}hour"
        regex30DayFilter = r"(?i)30[-_ ]?day"
        regexRollcallFilter = r"(?i)(roll[-_ ]?call)|roll"

        for a_tag in a_tags:
            href = a_tag["href"]

            # Remove any URL encoded spaces
            href = href.replace('%20', ' ')

            # 3 Day PDFs searching for 72 hr variations
            if re.search(regex3DayFilter, href):

                # Prepend host if missing from URL
                if not href.startswith("https://"):
                    # Prepend the base URL to the link
                    href = urljoin(hostname, href)

                currentTerminal.pdfLink3Day = href
                pdf3DayFound = True

            # 30 Day PDFs searching for 30 day variations
            if re.search(regex30DayFilter, href):

                # Prepend host if missing from URL
                if not href.startswith("https://"):
                    # Prepend the base URL to the link
                    href = urljoin(hostname, href)

                currentTerminal.pdfLink30Day = href
                pdf30DayFound = True

            # Rollcall PDFs searching for rollcall or roll call variations
            if re.search(regexRollcallFilter, href):

                # Prepend host if missing from URL
                if not href.startswith("https://"):
                    # Prepend the base URL to the link
                    href = urljoin(hostname, href)
                
                currentTerminal.pdfLinkRollcall = href
                pdfRollcallFound = True

            # Break out of loop when all are found
            if pdf3DayFound and pdf30DayFound and pdfRollcallFound:
                break

        # Log when no 3 day pdfs were found; Log set to warning b/c all terminals should have a 3 day schedule
        if not pdf3DayFound:
            logging.warning('No 3 day PDFs found for %s terminal', currentTerminal.name)
        
        # Log when no 30 Day PDFs found; Log set to info b/c most terminals do not have a 30 day schedule
        if not pdf30DayFound:
            logging.info('No 30 day PDFs found for %s terminal', currentTerminal.name)
        
        # Log when no rollcall PDFs found; Log set to info b/c most terminals do no have rollcalls
        if not pdfRollcallFound:
            logging.info('No rollcall PDFs found for %s terminal', currentTerminal.name)
    
    # Write to DB
    logging.info('Writing terminals to DB.')
    for terminal in listOfTerminals:
        db.addTerminal(terminal)
        logging.debug('%s terminal written to DB.', terminal.name)

def download3DayPDFs(db, pdfDir):
    logging.debug('Starting download3DayPDFs().')

    result = db.getTerminalsWithPDFLink()

    for document in result:
        # Download PDF and then capture the filename
        filename = downloadSingle3DayPDF(document, pdfDir)

        # Then set the pdfName3Day attribute to the filename
        db.setPDFName(document["name"], filename)

def downloadSingle3DayPDF(document, pdfDir):
    # Try to download the pdf
    try:
        response = requests.get(document["pdfLink3Day"])

        # Check if the request was successful (status code 200)
        if response.status_code == 200:
            # Get the filename from the URL and exclude any text after the '?'
            filename = document["name"] + ".pdf"
            filename = filename.strip()
            filename = filename.replace(' ', '_')

            # Write the content to a local file
            with open(pdfDir + filename, "wb") as file:
                file.write(response.content)
            
            return filename

    except requests.exceptions.RequestException as e:
        logging.error('Error occurred in downloadSingle3DayPDF() with link: %s.', document["pdfLink3Day"], exc_info=True)

def calcPDFHashes(db, pdfDir):
    logging.debug('Starting calcPDFHashes().')

    updatedTerminals = []

    # Get all PDF files in the directory
    pdfFiles = [file for file in os.listdir(pdfDir) if file.endswith('.pdf')]

    logging.debug('%d PDFs found in PDF directory.', len(pdfFiles))

    # Iterate through the PDF files
    for pdfFile in pdfFiles:
        pdfFile_path = os.path.join(pdfDir, pdfFile)
        pdfHash = calculateFileHash(pdfFile_path)

        hash_match = False
        document = db.get3DayPDFByFileName(pdfFile)

        # If document exists in mongo
        if document:
            storedHash = document.get("pdfHash3Day")

            # Check if no hash is stored
            if storedHash == "empty":
                db.setPDFHash3Day(document["name"], pdfHash)

            # If hashes are different add to list
            if pdfHash != storedHash:
                updatedTerminals.append(document["name"])

                # Update hash
                db.setPDFHash3Day(document["name"], pdfHash)

            # Hashes are the same check next file
            else:
                continue

        # Document does not exist in mongo
        else:
            logging.error('In calcPDFHahes(). No document found when searching MongoDB for 3 day pdf: %s.', pdfFile)

    return updatedTerminals