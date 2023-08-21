import scraper
from utils import *
from terminal import *
import os
import sys
import argparse
import logging
from s3_bucket import s3Bucket
from firestoredb import FirestoreClient
from pdf_utils import sort_terminal_pdfs

# List of ENV variables to check
variablesToCheck = [
    'FS_CRED_PATH',
    'TERMINAL_COLL',
    'PDF_ARCHIVE_COLL',
    'AWS_BUCKET_NAME',
    'AWS_ACCESS_KEY_ID',
    'AWS_SECRET_ACCESS_KEY',
    'PDF_DIR'
]

# Check if all .env variables are set
try:
    check_env_variables(variablesToCheck)
except ValueError as e:
    print(e)
    sys.exit(1)

# Get the absolute path of the script
scriptPath = os.path.abspath(__file__)

# Set the home directory to the directory of the script
homeDirectory = os.path.dirname(scriptPath)

# Enter correct directory
os.chdir(homeDirectory)

# Create an arguement parser
log_arg_parser = argparse.ArgumentParser(description='Set the logging level.')
log_arg_parser.add_argument('--log', default='INFO', help='Set the logging level.')

args = log_arg_parser.parse_args()

# Map from string level to logging level
levels = {
    'DEBUG': logging.DEBUG,
    'INFO': logging.INFO,
    'WARNING': logging.WARNING,
    'ERROR': logging.ERROR,
    'CRITICAL': logging.CRITICAL
}

# Set up logging
argLoglevel = levels.get(args.log.upper(), logging.INFO)
logging.basicConfig(filename='app.log', filemode='w', 
                    format='%(asctime)s - %(message)s', level=argLoglevel)

def main():
    
    logging.info('Program started.')

    # Prep local dirs
    check_local_pdf_dirs()
    clean_up_tmp_pdfs()

    # Create S3 bucket object
    s3 = s3Bucket()

    # Prep s3 bucket
    s3.check_s3_pdf_dirs()

    # Connect to Firestore
    fs = FirestoreClient()

    # Get enviroment variables
    pdf_archive_coll = os.getenv('PDF_ARCHIVE_COLL')

    logging.debug('Starting PDF retrieval process.')

    # Set URL to AMC Travel site and scrape Terminal information from it
    url = 'https://www.amc.af.mil/AMC-Travel-Site'
    list_of_terminals = scraper.get_terminals(url)

    # Populate the DB
    for terminal in list_of_terminals:
        fs.upsert_terminal_info(terminal)

    # Retrieve all updated terminal infomation
    list_of_terminals = fs.get_all_terminals()

    for terminal in list_of_terminals:

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
        pdf72Hour, pdf30Day, pdfRollcall = sort_terminal_pdfs(pdfs)

        # If new 72 hour schedule was found
        if pdf72Hour is not None and not pdf72Hour.seen_before:

            # Check if there is a PDF to archive
            if terminal.pdf72HourHash is not None:
                old72HourPdf = fs.get_pdf_by_hash(terminal.pdf72HourHash)

                # Check if old 72 hour schedule was found
                # in the DB. Need to exit if it was not found as
                # it means that it was never uploaded to S3.
                if old72HourPdf is None:
                    logging.error(f'Unable to find PDF with hash {terminal.pdf72HourHash} in the DB.')
                    sys.exit(1)

                # Archive the old 72 hour schedule and
                # update it with its new archived path
                # in S3.
                s3.archive_pdf(old72HourPdf)
                fs.upsert_pdf(old72HourPdf)

            # Upload new PDF to current directory of S3
            s3.upload_pdf_to_current_s3(pdf72Hour)

            # Update terminal with new hash
            fs.update_terminal_pdf_hash(pdf72Hour)
        
        # If a new 30 day schedule was found
        if pdf30Day is not None and not pdf30Day.seen_before:

            # Check if there is a PDF to archive
            if terminal.pdf30DayHash is not None:
                old30DayPdf = fs.get_pdf_by_hash(terminal.pdf30DayHash)

                # Check if old 30 day schedule was found
                # in the DB. Need to exit if it was not found as
                # it means that it was never uploaded to S3.
                if old72HourPdf is None:
                    logging.error(f'Unable to find PDF with hash {terminal.pdf30DayHash} in the DB.')
                    sys.exit(1)

                # Archive the old 30 day schedule and
                # update it with its new archived path 
                # in S3.
                s3.archive_pdf(old30DayPdf)
                fs.upsert_pdf(old30DayPdf)

            # Upload new PDF to current directory of S3
            s3.upload_pdf_to_current_s3(pdf30Day)

            # Update terminal with new hash
            fs.update_terminal_pdf_hash(pdf30Day)
        
        # If new rollcall was found
        if pdfRollcall is not None and not pdfRollcall.seen_before:

            # Check if there is a PDF to archive
            if terminal.pdfRollcallHash is not None:
                oldRollcallPdf = fs.get_pdf_by_hash(terminal.pdfRollcallHash)

                # Check if old rollcall was found
                # in the DB. Need to exit if it was not found as
                # it means that it was never uploaded to S3.
                if oldRollcallPdf is None:
                    logging.error(f'Unable to find PDF with hash {terminal.pdfRollcallHash} in the DB.')
                    sys.exit(1)

                # Archive the old rollcall and update
                # it with its new archived path in S3.
                s3.archive_pdf(oldRollcallPdf)
                fs.upsert_pdf(oldRollcallPdf)

            # Upload new PDF to current directory of S3
            s3.upload_pdf_to_current_s3(pdfRollcall)

            # Update terminal with new hash
            fs.update_terminal_pdf_hash(pdfRollcall)


        # Insert new PDFs to PDF Archive/seen before collection
        # in the DB to prevent reprocessing them in subsequent
        # runs.
        for pdf in pdfs:
            if not pdf.seen_before:
                fs.upsert_pdf(pdf)

    logging.info('Successfully finished program!')

if __name__ == "__main__":
    main()