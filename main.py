import argparse
import logging
import os
import sys

import sentry_sdk

import scraper
from firestoredb import FirestoreClient
from pdf_utils import local_sort_pdf, sort_terminal_pdfs
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

    logging.debug("Starting PDF retrieval process.")

    # Set URL to AMC Travel site and scrape Terminal information from it
    url = "https://www.amc.af.mil/AMC-Travel-Site"
    list_of_terminals = scraper.get_active_terminals(url)

    if not list_of_terminals:
        logging.error("No terminals found.")
        sys.exit(1)

    logging.info("Retrieved %s terminals.", len(list_of_terminals))

    if not fs.update_terminals(list_of_terminals):
        logging.info("No terminals were updated.")

    # Retrieve all updated terminal infomation
    list_of_terminals = fs.get_all_terminals()

    for terminal in list_of_terminals:
        logging.info("==========( %s )==========", terminal.name)

        # Get list of PDF objects and only their hashes from terminal
        pdfs = scraper.get_terminal_pdfs(terminal, hash_only=True)

        # Check if any PDFs are new
        for pdf in pdfs:
            if fs.pdf_seen_before(pdf):
                # Should discard PDFs we have seen before
                pdf.seen_before = True

                # Discard irrelevant PDFs to reduce processing time
                db_pdf = fs.get_pdf_by_hash(pdf.hash)

                if db_pdf is None:
                    logging.error(
                        "Unable to find PDF with hash %s in the DB.", pdf.hash
                    )
                    sys.exit(1)

                # We need all other seen PDFs as they
                # provide context when sorting PDFs
                if db_pdf.type == "DISCARD":
                    continue

            # Populate PDF object with all information
            pdf.populate()
            pdf.set_terminal(terminal.name)

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
                local_sort_pdf(pdf)
                fs.upsert_pdf_to_archive(pdf)
                s3.upload_pdf_to_current_s3(pdf)
            else:
                fs.upsert_pdf_to_archive(pdf)

    logging.info("Successfully finished program!")


if __name__ == "__main__":
    main()
