from typing import List
import os
from terminal import *
from bs4 import BeautifulSoup
from urllib.parse import urljoin
import logging
import utils
from pdf import Pdf

valid_locations = ['AMC CONUS Terminals', 'EUCOM Terminals', 'INDOPACOM Terminals',
                   'CENTCOM Terminals', 'SOUTHCOM TERMINALS', 'Non-AMC CONUS Terminals',
                   'ANG & Reserve Terminals']

# Functions
def get_terminals(url: str) -> List[Terminal]:
    logging.debug('Running get_terminal_info().')

    # Download the AMC travel page
    response = utils.get_with_retry(url)

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

    # Remove terminals with empty names
    nonEmptyTerminals = []
    for terminal in listOfTerminals:
        if terminal.name is not None:
            nonEmptyTerminals.append(terminal)

    return nonEmptyTerminals

def get_terminal_pdfs(terminal: Terminal) -> List[Pdf]:
    logging.debug('Entering get_terminal_pdfs().')

    base_dir = os.getenv('PDF_DIR')

    # URL of terminal page
    url = terminal.link
    
    # Skip terminal with no page and remove from DB
    if url == "empty":
        logging.warning(f'{terminal.name} has no page link. Skipping...')
        return

    logging.info(f'Downloading {terminal.name} page.')

    # Get terminal page with retry mechanism
    response = utils.get_with_retry(url)

    # If terminal page is not downloaded correctly exit
    if response is None:
        logging.warning(f'{terminal.name} page failed to download.')
        return

    # Get hostname of terminal page
    hostname = utils.normalize_url(url)

    # Create a BeautifulSoup object from the response content
    soup = BeautifulSoup(response.content, "html.parser")

    # Find all <a> tags with href and PDF file extension
    a_tags = soup.find_all('a', href=lambda href: href and '.pdf' in href)

    # Create array for PDFs that do not match
    terminal_pdfs = []

    for a_tag in a_tags:
        extracted_link = a_tag["href"]

        # Correct encoding from BS4
        pdf_link = extracted_link.replace('×tamp', '&timestamp')

        # Correct relative pdf links
        if not pdf_link.lower().startswith('https://'):
            pdf_link = hostname + pdf_link
        
        curr_pdf = Pdf(pdf_link)

        if not curr_pdf.seen_before:
            terminal_pdfs.append(curr_pdf)
    
    return terminal_pdfs