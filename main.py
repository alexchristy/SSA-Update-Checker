import scraper
from utils import *
from terminal import *
from mongodb import *
import os
import sys
import argparse
import logging
from s3_bucket import s3Bucket

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

    # Create PDF directories if they do not exist
    check_pdf_directories(basePDFDir)

    # Clean up left over PDFs in tmp
    clean_up_tmp_pdfs(basePDFDir)

    # Create S3 bucket object
    bucket = s3Bucket()

    # Sync current directory from S3
    bucket.sync_folder_from_s3(basePDFDir + 'current/', 'current', delete_extra_files_locally=True)

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

    # Get links to all the most up to date PDFs on Terminal sites
    listOfTerminals = scraper.get_terminals_info(listOfTerminals, basePDFDir)

    # Download all the PDFs for each Terminal
    for terminal in listOfTerminals:
        terminal = scraper.download_terminal_pdfs(terminal, basePDFDir)

    # Check each PDF directory
    dirs_to_check = [basePDFDir + 'tmp/72_HR/', basePDFDir + 'tmp/30_DAY/', basePDFDir + 'tmp/ROLLCALL/']
    successful_downloads = [check_downloaded_pdfs(dir_path) for dir_path in dirs_to_check]
    if all(successful_downloads):
        logging.info("PDFs were successfully downloaded in all directories.")
    else:
        logging.warning("Some directories did not have successful PDF downloads.")

    # Calc hashes for terminal's PDFs
    for terminal in listOfTerminals:
        terminal = scraper.calc_terminal_pdf_hashes(terminal)

    # Check for any updates to terminal's PDFs
    terminalUpdates = []
    for terminal in listOfTerminals:

        updatedPdfsDict = {}

        if db.is_72hr_updated(terminal):
            # Rotate the updated PDF to the current directory
            terminal.pdfName72Hour = rotate_pdf_to_current(basePDFDir, terminal.pdfName72Hour)
            terminal.is72HourUpdated = True

            updatedPdfsDict['72_HR'] = terminal.pdfName72Hour
            logging.info('%s updated their 72 hour schedule.', terminal.name)
        
        if db.is_30day_updated(terminal):
            # Rotate the updated PDF to the current directory
            terminal.pdfName30Day = rotate_pdf_to_current(basePDFDir, terminal.pdfName30Day)
            terminal.is30DayUpdated = True

            updatedPdfsDict['30_DAY'] = terminal.pdfName30Day
            logging.info('%s updated their 30 day schedule', terminal.name)
        
        if db.is_rollcall_updated(terminal):
            # Rotate the updated PDF to the current directory
            terminal.pdfNameRollcall = rotate_pdf_to_current(basePDFDir, terminal.pdfNameRollcall)
            terminal.isRollcallUpdated = True

            updatedPdfsDict['ROLLCALL'] = terminal.pdfNameRollcall
            logging.info('%s updated their rollcall.', terminal.name)
        
        # Create tuple of terminal name and update dict
        terminalTuple = (terminal.name, updatedPdfsDict)

        # Save to array of terminal updates
        terminalUpdates.append(terminalTuple)

    # Rotate out old PDFs to archive for AI training data
    archiveDirDict = gen_archive_dirs(listOfTerminals, basePDFDir)
    archive_old_pdfs(db, terminalUpdates, archiveDirDict)

    ##################################################
    # Place holder for Azure AI Services Upload func #
    ##################################################

    # Store any change in MongoDB
    for terminal in listOfTerminals:
        db.store_terminal(terminal)
    
    # Sync PDFs to S3 bucket
    bucket.sync_folder_to_s3(basePDFDir, '', delete_extra_files_in_s3=True)

    logging.info('Successfully finished program!')

if __name__ == "__main__":
    main()