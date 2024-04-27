import logging
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta
from typing import List, Tuple
from urllib.parse import urljoin

from bs4 import BeautifulSoup  # type: ignore

import scraper_utils
from firestoredb import FirestoreClient
from info_extract import InfoExtractor
from pdf import Pdf
from pdf_utils import local_sort_pdf_to_current, sort_terminal_pdfs
from s3_bucket import S3Bucket
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


class TerminalDocumentLockedError(Exception):
    """Exception raised when a terminal document is locked.".

    Attributes
    ----------
        message: The error message.

    """

    def __init__(self: "TerminalDocumentLockedError", message: str) -> None:
        """Initialize the error message."""
        self.message = message
        super().__init__(self.message)

    def __str__(self: "TerminalDocumentLockedError") -> str:
        """Return the error message."""
        return f"{self.message}"


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


def update_terminal_pdfs(  # noqa: PLR0913
    fs: FirestoreClient,
    s3: S3Bucket,
    terminal: Terminal,
    update_fingerprint: str,
    num_pdfs_updated: int,
    terminals_updated: List[str],
    terminals_checked: List[str],
) -> Tuple[bool, int]:
    """Update the PDFs for a terminal.

    Args:
    ----
        fs: A FirestoreClient object.
        s3: An S3Bucket object.
        terminal: A Terminal object.
        update_fingerprint: The fingerprint of the current update run.
        num_pdfs_updated: The number of PDFs updated.
        terminals_updated: A list of terminals updated.
        terminals_checked: A list of terminals checked.

    Returns:
    -------
        bool: True if the terminal was updated, False otherwise.
        int: The number of PDFs updated.

    """
    try:
        logging.info("==========( %s )==========", terminal.name)

        if not fs.acquire_terminal_doc_update_lock(terminal.name):
            msg = f"Terminal {terminal.name} document is locked."
            logging.warning(msg)
            return False, num_pdfs_updated

        # Pull down the latest terminal document
        terminal_update_fingerprint = fs.get_terminal_update_signature(terminal.name)

        # None indicates the terminal document does not exist
        if terminal_update_fingerprint is None:
            msg = "No terminal pdf update run fingerprint found in terminal document."
            logging.error(msg)
            raise ValueError(msg)

        # The terminal has already been updated in this run
        if update_fingerprint == terminal_update_fingerprint:
            logging.info(
                "Terminal %s has already been updated in this run.", terminal.name
            )
            fs.release_terminal_doc_lock(terminal.name)
            return False, num_pdfs_updated

        # Set status of terminal to updating
        fs.set_terminal_update_status(terminal.name, "UPDATING")

        # Get list of PDF objects and only their hashes from terminal
        pdfs = get_terminal_pdfs(terminal, hash_only=True)

        # Check if any PDFs are new
        for pdf in pdfs:
            if not fs.pdf_seen_before(pdf):
                # Populate PDF object with all information
                pdf.populate()
                pdf.set_terminal(terminal.name)
                continue

            # Should discard PDFs we have seen before
            pdf.seen_before = True

            # Discard irrelevant PDFs to reduce processing time
            db_pdf = fs.get_pdf_by_hash(pdf.hash)

            # Only PDFs that have been seen before should be in the DB
            if db_pdf is None:
                msg = f"Unable to find PDF with hash {pdf.hash} in the DB."
                logging.error(msg)
                raise ValueError(msg)

            # We need all other seen PDFs as they
            # provide context when sorting PDFs
            if db_pdf.type == "DISCARD":
                pdf.type = "DISCARD"
                continue

            # If the PDF is useful type, then we keep the db
            # version for context when sorting but we prevent reprocesing it.
            if db_pdf.type in ("72_HR", "30_DAY", "ROLLCALL"):
                pdf.type = db_pdf.type
            else:
                msg = f"PDF type {db_pdf.type} is not recognized."
                logging.error(msg)
                raise ValueError(msg)

        if not pdfs:
            logging.warning("No PDFs found for %s.", terminal.name)

        # Remove DISCARD PDFs from list
        pdfs_cleaned = [pdf for pdf in pdfs if pdf.type != "DISCARD"]

        # Deduplicate PDFs by hash
        pdfs_cleaned = scraper_utils.deduplicate_with_attribute(pdfs_cleaned, "hash")

        pdfs = pdfs_cleaned
        logging.info("Cleaned PDFs: %s", [pdf.hash for pdf in pdfs])

        # Try to determine the type of each new PDF
        # and return the most plausible new PDF of
        # each type.
        pdf_72hr, pdf_30day, pdf_rollcall = sort_terminal_pdfs(pdfs)

        # If new 72 hour schedule was found
        if pdf_72hr and not pdf_72hr.seen_before and terminal.pdf_72hr_hash:
            old_pdf_72hr = fs.get_pdf_by_hash(terminal.pdf_72hr_hash)

            # Check if old 72 hour schedule was found
            # in the DB. Need to exit if it was not found as
            # it means that it was never uploaded to S3.
            if old_pdf_72hr is None:
                msg = (
                    f"Unable to find PDF with hash {terminal.pdf_72hr_hash} in the DB."
                )
                logging.error(msg)
                raise ValueError(msg)

            # Archive the old 72 hour schedule and
            # update it with its new archived path
            # in S3.
            s3.archive_pdf(old_pdf_72hr)
            fs.upsert_pdf_to_archive(old_pdf_72hr)

        # If a new 30 day schedule was found
        if pdf_30day and not pdf_30day.seen_before and terminal.pdf_30day_hash:
            old_30day_pdf = fs.get_pdf_by_hash(terminal.pdf_30day_hash)

            # Check if old 30 day schedule was found
            # in the DB. Need to exit if it was not found as
            # it means that it was never uploaded to S3.
            if old_30day_pdf is None:
                msg = (
                    "Unable to find PDF with hash {terminal.pdf_30day_hash} in the DB."
                )
                logging.error(msg)
                raise ValueError(msg)

            # Archive the old 30 day schedule and
            # update it with its new archived path
            # in S3.
            s3.archive_pdf(old_30day_pdf)
            fs.upsert_pdf_to_archive(old_30day_pdf)

        # If new rollcall was found
        if pdf_rollcall and not pdf_rollcall.seen_before and terminal.pdf_rollcall_hash:
            old_rollcall_pdf = fs.get_pdf_by_hash(terminal.pdf_rollcall_hash)

            # Check if old rollcall was found
            # in the DB. Need to exit if it was not found as
            # it means that it was never uploaded to S3.
            if old_rollcall_pdf is None:
                msg = "Unable to find PDF with hash {terminal.pdf_rollcall_hash} in the DB."
                logging.error(msg)
                raise ValueError(msg)

            # Archive the old rollcall and update
            # it with its new archived path in S3.
            s3.archive_pdf(old_rollcall_pdf)
            fs.upsert_pdf_to_archive(old_rollcall_pdf)

        # Insert new PDFs to PDF Archive/seen before collection
        # in the DB to prevent reprocessing them in subsequent
        # runs.
        for pdf in pdfs:
            if not pdf.seen_before and pdf.type != "DISCARD":
                fs.update_terminal_pdf_hash(pdf)
                fs.set_pdf_last_update_timestamp(
                    terminal_name=terminal.name, pdf_type=pdf.type
                )
                local_sort_pdf_to_current(pdf)
                fs.upsert_pdf_to_archive(pdf)
                s3.upload_pdf_to_current_s3(pdf)
                num_pdfs_updated += 1
                terminals_updated.append(terminal.name)
                logging.info(
                    "A new %s pdf for %s was found called: %s.",
                    pdf.type,
                    terminal.name,
                    pdf.filename,
                )
            elif not pdf.seen_before and pdf.type == "DISCARD":
                fs.upsert_pdf_to_archive(pdf)
                logging.info("A new DISCARD pdf was found called: %s.", pdf.filename)

        # Release the lock
        terminals_checked.append(terminal.name)
        fs.set_terminal_update_signature(terminal.name, update_fingerprint)
        fs.set_terminal_last_check_timestamp(terminal.name)
        fs.set_terminal_update_status(terminal.name, "SUCCESS")
        fs.release_terminal_doc_lock(terminal.name)
        return True, num_pdfs_updated

    except Exception as e:
        logging.error("An error occurred while updating the terminal PDFs.")
        logging.error(e)
        fs.release_terminal_doc_lock(terminal.name)
        fs.set_terminal_update_status(terminal.name, "FAILED")
        return False, num_pdfs_updated


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


def update_terminal_contact_info(fs: FirestoreClient, terminal: Terminal) -> None:
    """Update the contact information for a terminal.

    Args:
    ----
        fs: A FirestoreClient object.
        terminal: A Terminal object.

    """
    logging.info("Updating contact information for %s.", terminal.name)

    if not terminal.link:
        logging.warning("No link found for %s.", terminal.name)
        return

    logging.info("Downloading %s page.", terminal.name)

    # Get terminal page with retry mechanism
    response = scraper_utils.get_with_retry(terminal.link)

    # If terminal page is not downloaded correctly exit
    if not response.text:
        logging.warning("%s page failed to download.", terminal.name)
        return

    # Extract the contact information
    info_extractor = InfoExtractor()

    # Extract the contact information
    contact_info, current_hash = info_extractor.get_gpt_extracted_info(response.text)

    # Check if the contact information has changed
    if current_hash == terminal.contact_info_hash:
        logging.info("Contact information for %s has not changed.", terminal.name)
        return

    # Update the contact information in the database
    terminal.contact_info_hash = current_hash
    terminal.contact_info = contact_info

    fs.upsert_terminal_info(terminal)

    logging.error(
        "Updated contact information for %s.", terminal.name
    )  # This an error to allow for alerts to be sent to discord for manual review later
