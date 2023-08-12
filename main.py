import scraper
from utils import *
from terminal import *
from mongodb import *
import os
import sys
import argparse
import logging
from s3_bucket import s3Bucket
from typing import List

# List of ENV variables to check
variablesToCheck = [
    'MONGO_DB',
    'MONGO_COLLECTION',
    'MONGO_HOST',
    'MONGO_USERNAME',
    'MONGO_PASSWORD',
    'AWS_BUCKET_NAME',
    'AWS_ACCESS_KEY_ID',
    'AWS_SECRET_ACCESS_KEY',
    'PDF_DIR'
]

# Check if all .env variables are set
try:
    check_env_variables(variablesToCheck)
    
    # Load in environment variables from .env file
    mongoDBName = os.getenv('MONGO_DB')
    mongoCollectionName = os.getenv('MONGO_COLLECTION')
    mongoUsername = os.getenv('MONGO_USERNAME')
    mongoPassword = os.getenv('MONGO_PASSWORD')
    basePDFDir = os.getenv('PDF_DIR')
    mongoHost = os.getenv('MONGO_HOST')
    s3BucketName = os.getenv('AWS_BUCKET_NAME')

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
    check_s3_pdf_dirs(s3)

    # Intialize MongoDB
    logging.info('Starting MongoDB.')
    db = MongoDB(mongoDBName, mongoCollectionName, host=mongoHost, username=mongoUsername, password=mongoPassword)

    # Connect to the correct Mongo server
    if mongoHost == 'localhost':
        db.connect_local()
    else:
        db.connect_atlas()

    logging.debug('Starting PDF retrieval process.')

    # Set URL to AMC Travel site and scrape Terminal information from it
    url = 'https://www.amc.af.mil/AMC-Travel-Site'
    listOfTerminals = scraper.get_terminals(url)

    # Populate the DB
    for terminal in listOfTerminals:
        db.upsert_terminal(terminal)

    # Get links to all the most up to date PDFs on Terminal sites
    scraper.get_terminals_pdf_links(db)

    # Download all the PDFs for each Terminal and
    # save downloaded tmp paths of these PDFs to 
    # a list of terminal objects.
    listOfTerminals = scraper.download_terminals_pdfs(db)

    # Check each PDF directory to confirm something was
    # downloaded.
    dirs_to_check = [basePDFDir + 'tmp/72_HR/', basePDFDir + 'tmp/30_DAY/', basePDFDir + 'tmp/ROLLCALL/']
    successful_downloads = [check_downloaded_pdfs(dir_path) for dir_path in dirs_to_check]
    if all(successful_downloads):
        logging.info("PDFs were successfully downloaded in all directories.")
    else:
        logging.warning("Some directories did not have successful PDF downloads.")

    # Calc hashes of the tmp downloaded terminal PDFs
    # using the temp Terminal objects.
    for terminal in listOfTerminals:
        terminal = scraper.calc_terminal_pdf_hashes(terminal)

    # Check for updates by comparing current terminal objects hashes
    # with the stored hashes in MongoDB. If the document does not exist
    # like during the first run. is_XXX_updated() will return True.
    updatedTerminals = []
    for terminal in listOfTerminals:

        wasUpdated = False

        if db.is_72hr_updated(terminal):
            terminal.is72HourUpdated = True
            wasUpdated = True
        
        if db.is_30day_updated(terminal):
            terminal.is30DayUpdated = True
            wasUpdated = True

        if db.is_rollcall_updated(terminal):
            terminal.isRollcallUpdated = True
            wasUpdated = True
        
        if wasUpdated:
            updatedTerminals.append(terminal)
    
    # Archive the PDFs that will be replaced updated PDFs
    archive_pdfs_s3(db, s3, updatedTerminals)
    rotate_pdfs_to_current_s3(db, s3, updatedTerminals)

    # ##################################################
    # # Place holder for Azure AI Services Upload func #
    # ##################################################
    
    logging.info('Successfully finished program!')

if __name__ == "__main__":
    main()