import argparse
import logging
import os
import random
import sys
from datetime import datetime, timedelta
from typing import List

import sentry_sdk

import scraper
from firestoredb import (
    FirestoreClient,
    wait_for_terminal_lock_change,
)
from pdf_utils import local_sort_pdf_to_current, sort_terminal_pdfs
from s3_bucket import S3Bucket
from scraper_utils import check_env_variables, check_local_pdf_dirs, clean_up_tmp_pdfs

# Set up Sentry
sentry_sdk.init(
    dsn="https://0206deb398bdf73816001d5aca1f0bce@o4506224652713984.ingest.sentry.io/4506440581775360",
    # Set traces_sample_rate to 1.0 to capture 100%
    # of transactions for performance monitoring.
    traces_sample_rate=1.0,
    # Set profiles_sample_rate to 1.0 to profile 100%
    # of sampled transactions.
    # We recommend adjusting this value in production.
    profiles_sample_rate=1.0,
)

# List of ENV variables to check
vars_to_check = [
    "FS_CRED_PATH",
    "TERMINAL_COLL",
    "PDF_ARCHIVE_COLL",
    "AWS_BUCKET_NAME",
    "AWS_ACCESS_KEY_ID",
    "AWS_SECRET_ACCESS_KEY",
    "PDF_DIR",
    "OPENAI_API_KEY",
    "GOOGLE_MAPS_API_KEY",
    "LOCK_COLL",
]

# Check if all .env variables are set
if not check_env_variables(vars_to_check):
    logging.error("Not all environment variables are set.")
    sys.exit(1)

# Get the absolute path of the script
script_path = os.path.abspath(__file__)

# Set the home directory to the directory of the script
home_dir = os.path.dirname(script_path)

# Enter correct directory
os.chdir(home_dir)

# Create an arguement parser
log_arg_parser = argparse.ArgumentParser(description="Set the logging level.")
log_arg_parser.add_argument("--log", default="INFO", help="Set the logging level.")

args = log_arg_parser.parse_args()

# Map from string level to logging level
levels = {
    "DEBUG": logging.DEBUG,
    "INFO": logging.INFO,
    "WARNING": logging.WARNING,
    "ERROR": logging.ERROR,
    "CRITICAL": logging.CRITICAL,
}

# Set up logging
arg_log_level = levels.get(args.log.upper(), logging.INFO)
logging.basicConfig(
    filename="app.log",
    filemode="w",
    format="%(asctime)s - %(message)s",
    level=arg_log_level,
)


def main() -> None:
    """Run core logic of program."""
    logging.info("Program started.")

    # Prep local dirs
    check_local_pdf_dirs()

    if not clean_up_tmp_pdfs():
        logging.error("Error cleaning up tmp PDFs.")
        sys.exit(1)

    # Create S3 bucket object
    s3 = S3Bucket()

    # Prep s3 bucket
    s3.check_s3_pdf_dirs()

    # Connect to Firestore
    fs = FirestoreClient()

    logging.info("Starting PDF retrieval process.")

    # Got the lock
    if fs.acquire_terminal_coll_update_lock():
        logging.info("No other instance of the program is updating the terminals.")

        last_update_timestamp = fs.get_terminal_update_lock_timestamp()

        if not last_update_timestamp:
            logging.error("No terminal update lock timestamp found.")
            sys.exit(1)

        current_time = datetime.now(last_update_timestamp.tzinfo)

        time_diff = current_time - last_update_timestamp

        # If the last update was less than 2 minutes ago, then it has
        # already been updated by another instance of the program.
        if time_diff > timedelta(minutes=2):
            logging.info("Terminals last updated at: %s", last_update_timestamp)

            logging.info("Retrieving terminal information.")

            # Set URL to AMC Travel site and scrape Terminal information from it
            url = "https://www.amc.af.mil/AMC-Travel-Site"
            list_of_terminals = scraper.get_active_terminals(url)

            if not list_of_terminals:
                logging.error("No terminals found.")
                sys.exit(1)

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

        fs.release_terminal_lock()
    else:
        logging.info("Another instance of the program is updating the terminals.")
        fs.watch_terminal_update_lock()
        wait_for_terminal_lock_change()

    # Retrieve fingerprint for signing off on terminal updates
    update_fingerprint = fs.get_terminal_update_fingerprint()

    if not update_fingerprint:
        logging.error(
            "No terminal pdf update run fingerprint found in terminal_update_lock document."
        )
        sys.exit(1)

    # Retrieve all updated terminal infomation
    list_of_terminals = fs.get_all_terminals()
    random.shuffle(
        list_of_terminals
    )  # Shuffle so some terminals are not always updated first

    # Summary variables
    terminals_updated: List[str] = []
    num_pdfs_updated = 0

    for terminal in list_of_terminals:

        if not fs.acquire_terminal_doc_update_lock(terminal.name):
            continue

        # Pull down the latest terminal document
        terminal_update_fingerprint = fs.get_terminal_update_signature(terminal.name)

        if (
            terminal_update_fingerprint is None
        ):  # None indicates the terminal document does not exist
            logging.error(
                "No terminal pdf update run fingerprint found in terminal document."
            )
            sys.exit(1)

        # The terminal has already been updated in this run
        if update_fingerprint == terminal_update_fingerprint:
            logging.info(
                "Terminal %s has already been updated in this run.", terminal.name
            )
            fs.release_terminal_doc_lock(terminal.name)
            continue

        fs.set_terminal_update_signature(terminal.name, update_fingerprint)

        logging.info("==========( %s )==========", terminal.name)

        # Get list of PDF objects and only their hashes from terminal
        pdfs = scraper.get_terminal_pdfs(terminal, hash_only=True)

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

            if db_pdf is None:
                logging.error("Unable to find PDF with hash %s in the DB.", pdf.hash)
                sys.exit(1)

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
                logging.error("Unknown PDF type: %s", db_pdf.type)
                sys.exit(1)

        # Remove DISCARD PDFs from list
        pdfs_cleaned = [pdf for pdf in pdfs if pdf.type != "DISCARD"]
        pdfs = pdfs_cleaned

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
                logging.error(
                    "Unable to find PDF with hash %s in the DB.",
                    terminal.pdf_72hr_hash,
                )
                sys.exit(1)

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
                logging.error(
                    "Unable to find PDF with hash %s in the DB.",
                    terminal.pdf_30day_hash,
                )
                sys.exit(1)

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
                logging.error(
                    "Unable to find PDF with hash %s in the DB.",
                    terminal.pdf_rollcall_hash,
                )
                sys.exit(1)

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
        fs.release_terminal_doc_lock(terminal.name)

    # Generate summary logs before exiting
    logging.info("======== Summary of Updates ========")

    if terminals_updated:
        logging.info(
            "%d terminals updated: %s",
            len(set(terminals_updated)),
            ", ".join(set(terminals_updated)),
        )
    else:
        logging.info("No terminals were updated in this run.")

    logging.info("%d PDFs were updated.", num_pdfs_updated)

    logging.info("Successfully finished program!")


if __name__ == "__main__":
    main()
