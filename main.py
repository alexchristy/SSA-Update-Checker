import argparse
import logging
import os
import random
import sys
from typing import List

import sentry_sdk
from dotenv import load_dotenv

import scraper
from firestoredb import FirestoreClient
from s3_bucket import S3Bucket
from scraper_utils import check_env_variables, check_local_pdf_dirs, clean_up_tmp_pdfs


def setup_logging(
    default_level=logging.INFO, log_file: str = "app.log"  # noqa: ANN001
) -> None:
    """Set up logging configuration.

    Args:
    ----
        default_level: The default logging level.
        log_file: The name/path of the log file.

    """
    for handler in logging.root.handlers[
        :
    ]:  # Remove all handlers associated with the root logger.
        logging.root.removeHandler(handler)
    logging.basicConfig(
        filename=log_file,
        filemode="w",
        format="%(asctime)s || %(levelname)s - %(message)s",
        level=default_level,
    )


setup_logging()


def init_sentry() -> bool:
    """Initialize Sentry."""
    sentry_dsn = os.getenv("SENTRY_DSN")

    if not sentry_dsn:
        logging.error("No Sentry DSN enviroment variable found.")
        return False

    try:
        sentry_sdk.init(
            dsn=sentry_dsn,
            # Set traces_sample_rate to 1.0 to capture 100%
            # of transactions for performance monitoring.
            traces_sample_rate=1.0,
            # Set profiles_sample_rate to 1.0 to profile 100%
            # of sampled transactions.
            # We recommend adjusting this value in production.
            profiles_sample_rate=1.0,
        )
        logging.info("Sentry initialized.")
    except Exception as e:
        logging.error("Error initializing Sentry: %s", e)
        return False

    return True


def move_to_working_dir() -> bool:
    """Move to the working directory."""
    try:
        # Get the absolute path of the script
        script_path = os.path.abspath(__file__)

        # Set the home directory to the directory of the script
        home_dir = os.path.dirname(script_path)

        # Enter correct directory
        os.chdir(home_dir)
    except Exception as e:
        logging.error("Error moving to working directory: %s", e)
        return False

    return True


def parse_args() -> argparse.Namespace:
    """Parse command line arguments."""
    # Log level argument
    parser = argparse.ArgumentParser(description="Set the logging level.")
    parser.add_argument(
        "--log", default="INFO", help="Set the logging level.", type=str
    )

    return parser.parse_args()


def initialize_app() -> None:
    """Initialize the program."""
    # Load environment variables
    load_dotenv()

    if not init_sentry():
        logging.error("Error initializing Sentry.")
        sys.exit(1)

    args = parse_args()  # Moved up to ensure logging level is set as early as possible

    # Set up logging with the level from command line arguments
    levels = {
        "DEBUG": logging.DEBUG,
        "INFO": logging.INFO,
        "WARNING": logging.WARNING,
        "ERROR": logging.ERROR,
        "CRITICAL": logging.CRITICAL,
    }
    arg_log_level = levels.get(args.log.upper(), logging.INFO)
    setup_logging(arg_log_level)  # Call setup_logging with the correct level

    # Check for all required environment variables
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
        "SENTRY_DSN",
    ]

    # Check if all .env variables are set
    if not check_env_variables(vars_to_check):
        logging.error("Not all environment variables are set.")
        sys.exit(1)

    # Move to the working directory
    if not move_to_working_dir():
        logging.error("Error moving to working directory.")
        sys.exit(1)

    # Prep local dirs
    check_local_pdf_dirs()

    if not clean_up_tmp_pdfs():
        logging.error("Error cleaning up tmp PDFs.")
        sys.exit(1)


def main() -> None:
    """Run core logic of program."""
    logging.info("Program started.")

    # Create S3 bucket object
    s3 = S3Bucket()

    # Prep s3 bucket
    s3.check_s3_pdf_dirs()

    # Connect to Firestore
    fs = FirestoreClient()

    logging.info("Starting PDF retrieval process.")

    # Update the terminals in the database
    scraper.update_db_terminals(fs)

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
        scraper.update_terminal_pdfs(
            fs, s3, terminal, update_fingerprint, num_pdfs_updated, terminals_updated
        )

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
    initialize_app()
    main()
