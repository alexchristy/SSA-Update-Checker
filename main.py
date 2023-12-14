import argparse
import logging
import os
import sys

import scraper
from firestoredb import FirestoreClient
from pdf_utils import sort_terminal_pdfs
from s3_bucket import s3Bucket
from scraper_utils import check_env_variables, check_local_pdf_dirs, clean_up_tmp_pdfs

# List of ENV variables to check
vars_to_check = [
    "FS_CRED_PATH",
    "TERMINAL_COLL",
    "PDF_ARCHIVE_COLL",
    "AWS_BUCKET_NAME",
    "AWS_ACCESS_KEY_ID",
    "AWS_SECRET_ACCESS_KEY",
    "PDF_DIR",
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
    s3 = s3Bucket()

    # Prep s3 bucket
    s3.check_s3_pdf_dirs()

    # Connect to Firestore
    fs = FirestoreClient()

    logging.debug("Starting PDF retrieval process.")

    # Set URL to AMC Travel site and scrape Terminal information from it
    url = "https://www.amc.af.mil/AMC-Travel-Site"
    list_of_terminals = scraper.get_active_terminals(url)

    # Populate the DB
    for terminal in list_of_terminals:
        fs.upsert_terminal_info(terminal)

    # Retrieve all updated terminal infomation
    list_of_terminals = fs.get_all_terminals()

    for terminal in list_of_terminals:
        logging.info("==========( %s )==========", terminal.name)

        # Get list of PDF objects from terminal
        pdfs = scraper.get_terminal_pdfs(terminal)

        # Check if any PDFs are new
        for pdf in pdfs:
            if fs.pdf_seen_before(pdf):
                # Should discard PDFs we have seen before
                pdf.seen_before = True

            pdf.set_terminal(terminal.name)

        # Try to determine the type of each new PDF
        # and return the most plausible new PDF of
        # each type.
        pdf_72hr, pdf_30day, pdf_rollcall = sort_terminal_pdfs(pdfs)

        # If new 72 hour schedule was found
        if pdf_72hr is not None and not pdf_72hr.seen_before:
            # Check if there is a PDF to archive
            if terminal.pdf_72hr_hash is not None:
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

            # Upload new PDF to current directory of S3
            s3.upload_pdf_to_current_s3(pdf_72hr)

            # Update terminal with new hash
            fs.update_terminal_pdf_hash(pdf_72hr)

        # If a new 30 day schedule was found
        if pdf_30day is not None and not pdf_30day.seen_before:
            # Check if there is a PDF to archive
            if terminal.pdf_30day_hash is not None:
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

            # Upload new PDF to current directory of S3
            s3.upload_pdf_to_current_s3(pdf_30day)

            # Update terminal with new hash
            fs.update_terminal_pdf_hash(pdf_30day)

        # If new rollcall was found
        if pdf_rollcall is not None and not pdf_rollcall.seen_before:
            # Check if there is a PDF to archive
            if terminal.pdf_rollcall_hash is not None:
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

            # Upload new PDF to current directory of S3
            s3.upload_pdf_to_current_s3(pdf_rollcall)

            # Update terminal with new hash
            fs.update_terminal_pdf_hash(pdf_rollcall)

        # Insert new PDFs to PDF Archive/seen before collection
        # in the DB to prevent reprocessing them in subsequent
        # runs.
        for pdf in pdfs:
            if not pdf.seen_before:
                fs.upsert_pdf_to_archive(pdf)

    logging.info("Successfully finished program!")


if __name__ == "__main__":
    main()
