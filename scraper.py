import logging
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta
from typing import List
from urllib.parse import urljoin

from bs4 import BeautifulSoup  # type: ignore

import scraper_utils
from firestoredb import FirestoreClient
from pdf import Pdf
from terminal import Terminal

valid_locations = [
    "AMC CONUS Terminals",
    "EUCOM Terminals",
    "INDOPACOM Terminals",
    "CENTCOM Terminals",
    "SOUTHCOM TERMINALS",
    "Non-AMC CONUS Terminals",
    "ANG & Reserve Terminals",
]


# Functions
def update_db_terminals(
    fs: FirestoreClient,
) -> bool:
    """Update the terminals in the database.

    Args:
    ----
        fs: A FirestoreClient object.

    Returns:
    -------
        bool: True if the terminals were updated, False otherwise.

    Raises:
    ------
        SystemExit: If the AMC travel page fails to download.

    """
    try:
        if fs.acquire_terminal_coll_update_lock():
            logging.info("No other instance of the program is updating the terminals.")

            last_update_timestamp = fs.get_terminal_update_lock_timestamp()

            # Default to 666 minutes if no timestamp is found
            # This prevents hanging indefinitely if this is the first time the program is run
            time_diff = timedelta(minutes=666)

            if last_update_timestamp:
                logging.error("Terminal update lock timestamp found.")

                current_time = datetime.now(last_update_timestamp.tzinfo)

                time_diff = current_time - last_update_timestamp
            else:
                logging.info(
                    "No terminal update lock timestamp found. Continuing to update."
                )

            # If the last update was less than 2 minutes ago, then it has
            # already been updated by another instance of the program.
            if time_diff > timedelta(minutes=2):
                logging.info("Terminals last updated at: %s", last_update_timestamp)

                logging.info("Retrieving terminal information.")

                # Set URL to AMC Travel site and scrape Terminal information from it
                url = "https://www.amc.af.mil/AMC-Travel-Site"
                list_of_terminals = get_active_terminals(url)

                if not list_of_terminals:
                    msg = "No terminals found."
                    logging.error(msg)
                    raise ValueError(msg)

                logging.info("Retrieved %s terminals.", len(list_of_terminals))

                if not fs.update_terminals(list_of_terminals):
                    logging.info("No terminals were updated.")

                fs.add_termimal_update_fingerprint()
                fs.set_terminal_update_lock_timestamp()
            else:
                logging.info(
                    "Terminals were updated less than 2 minutes ago. Last updated at: %s",
                    last_update_timestamp,
                )
                fs.safely_release_terminal_lock()
                return False

            fs.safely_release_terminal_lock()
            return True

        logging.info("Another instance of the program is updating the terminals.")
        fs.watch_terminal_update_lock()
        fs.wait_for_terminal_lock_change()
        return False
    except Exception as e:
        logging.error("An error occurred while updating the terminals.")
        logging.error(e)
        fs.safely_release_terminal_lock()
        return False


def get_active_terminals(url: str) -> List[Terminal]:
    """Scan the AMC travel page for terminal information to return a list of SpaceA active terminal objects.

    Args:
    ----
        url: The URL of the AMC travel page.

    Returns:
    -------
        listOfTerminals: A list of Terminal objects.

    """
    logging.debug("Running get_terminal_info().")

    # Download the AMC travel page
    response = scraper_utils.get_with_retry(url)

    # Exit program if AMC travel page fails to download
    if not response or not response.content:
        logging.critical("Failed to download AMC Travel page. Exiting program...")
        raise SystemExit

    # Create empty array of terminals to store data
    list_of_terminals = []

    # Create a BeautifulSoup object from the response content
    soup = BeautifulSoup(response.content, "html.parser")

    # Save all menus
    # Find all <li> tags with the specified criteria
    # Find all <li> tags with specific attributes
    tags = soup.find_all(
        "li",
        class_="af3AccordionMenuListItem",
        attrs={
            "data-index": True,
            "tabindex": True,
            "aria-expanded": True,
            "title": True,
        },
    )

    # Save the group names and the location data
    group = ""
    page_positon = 1
    filtered_tags = []  # Exclude tags with references to groups for processing later
    for tag in tags:
        title = tag.get("title")  # get the title attribute

        # Check if the current title is a group name
        if str(title).lower() in (location.lower() for location in valid_locations):
            group = str(
                title
            ).upper()  # Saved in all caps to match military convention and website
            continue

        # Not a valid terminal name skip
        title_lower = str(title).lower()
        if (
            "," not in title_lower
            and "terminal" not in title_lower
            or "click" in title_lower
        ):
            continue

        # If a valid group is set add it to new terminals
        if group != "":
            new_terminal = Terminal()
            new_terminal.group = group
            new_terminal.location = str(title)
            new_terminal.page_pos = page_positon
            list_of_terminals.append(new_terminal)

            # Increment page position only for terminal entries
            page_positon += 1

            # Save valid terminal tag
            filtered_tags.append(tag)

    # Define the filter strings for names and link words
    name_filter_strings = [
        "Transportation Function",
        "Passenger Terminal",
        "Air Terminal",
        "AFB",
    ]
    link_filter_words = ["Terminal", "Passenger", "Transportation", "Gateway"]

    # len(tags) == len(listOfTerminals) <-- This will always be true
    index = 0
    # Iterate though the terminal tags and save page links and terminal names to the terminal objects
    for tag in filtered_tags:
        a_tags = tag.find_all("a", href=True, target="_blank")

        for a_tag in a_tags:
            href = a_tag["href"]
            name = a_tag.text

            # Check if the name contains any of the filter strings
            name_matches_filter = any(
                filter_str in name for filter_str in name_filter_strings
            )

            # Check if the href contains any of the filter words
            link_matches_filter = any(word in href for word in link_filter_words)

            # If both conditions are met, save the page link and terminal name to terminal object
            if name_matches_filter and link_matches_filter:
                # Check if the link starts with "https://"
                if not href.startswith("https://"):
                    # Prepend the base URL to the link
                    base_url = "https://www.amc.af.mil"
                    href = urljoin(base_url, href)

                current_terminal = list_of_terminals[index]

                # Save the name and link
                current_terminal.name = name.strip()
                current_terminal.link = href

        # Increment so next link and name is put in correct terminal object
        index += 1

    # Remove terminals with empty names
    non_empty_terminals = []
    for terminal in list_of_terminals:
        if terminal.name.strip():
            non_empty_terminals.append(terminal)

    return non_empty_terminals


def create_pdf_object(pdf_link: str, hash_only: bool) -> Pdf:
    """Helper function to create a PDF object from a link.

    Args:
    ----
        pdf_link: Link to the PDF file.
        hash_only: Whether only the hash of the PDF should be populated.

    Returns:
    -------
        Pdf: A PDF object.

    """  # noqa: D401
    if hash_only:
        return Pdf(pdf_link, hash_only=True)

    return Pdf(pdf_link, populate=True)


def get_terminal_pdfs(terminal: Terminal, hash_only: bool = False) -> List[Pdf]:
    """Download the terminal page and extract all PDF links to create PDF objects.

    Args:
    ----
        terminal: A Terminal object.
        hash_only: A boolean to indicate if only the hash should be populated for each PDF object.

    Returns:
    -------
        terminal_pdfs: A list of PDF objects.

    """
    logging.info("Entering get_terminal_pdfs().")

    # URL of terminal page
    url = terminal.link

    # Skip terminal with no page and remove from DB
    if url == "empty":
        logging.warning("{%s has no page link. Skipping...", terminal.name)
        return []

    logging.info("Downloading %s page.", terminal.name)

    # Get terminal page with retry mechanism
    response = scraper_utils.get_with_retry(url)

    # If terminal page is not downloaded correctly exit
    if response is None:
        logging.warning("%s page failed to download.", terminal.name)
        return []

    # Get hostname of terminal page
    hostname = scraper_utils.normalize_url(url)

    # Create a BeautifulSoup object from the response content
    soup = BeautifulSoup(response.content, "html.parser")

    # Find all <a> tags with href and PDF file extension
    a_tags = soup.find_all("a", href=lambda href: href and ".pdf" in href)

    # Prepare a list for PDF links
    pdf_links = []

    for a_tag in a_tags:
        extracted_link = a_tag["href"]

        # Correct encoding from BS4
        pdf_link = extracted_link.replace(
            "×tamp",  # noqa: RUF001 , RUF003 (The "×" is common in PDF links)
            "&timestamp",
        )

        # Correct relative pdf links
        if not pdf_link.lower().startswith("https://"):
            pdf_link = hostname + pdf_link

        pdf_links.append(pdf_link)

    # Use ThreadPoolExecutor to parallelize PDF object creation
    with ThreadPoolExecutor(max_workers=3) as executor:
        futures = [
            executor.submit(create_pdf_object, link, hash_only) for link in pdf_links
        ]
        return [
            future.result() for future in futures if not future.result().seen_before
        ]
