import scraper
from utils import *
from terminal import *
from mongodb import *
from telegram import Bot
import asyncio
import os
import glob
import sys
from dotenv import load_dotenv
import argparse
import logging

# List of ENV variables to check
variablesToCheck = [
    'TELEGRAM_API_TOKEN',
    'MONGO_DB',
    'MONGO_COLLECTION',
    'MONGO_USERNAME',
    'MONGO_PASSWORD',
    'PDF_DIR'
]

# Check if all .env variables are set
try:
    check_env_variables(variablesToCheck)
    
    # Load environment variables from .env file
    api_token = os.getenv('TELEGRAM_API_TOKEN')
    mongoDBName = os.getenv('MONGO_DB')
    mongoCollectionName = os.getenv('MONGO_COLLECTION')
    mongoUsername = os.getenv('MONGO_USERNAME')
    mongoPassword = os.getenv('MONGO_PASSWORD')
    basePDFDir = os.getenv('PDF_DIR')

except ValueError as e:
    print(e)
    sys.exit(1)

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

    # Get the absolute path of the script
    scriptPath = os.path.abspath(__file__)

    # Set the home directory to the directory of the script
    homeDirectory = os.path.dirname(scriptPath)

    # Enter correct directory
    os.chdir(homeDirectory)

    # Create PDF directories if they do not exist
    check_pdf_directories(basePDFDir)

    # Intialize MongoDB
    logging.info('Starting MongoDB.')
    db = MongoDB(mongoDBName, mongoCollectionName, username=mongoUsername, password=mongoPassword)
    db.connect()

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

if __name__ == "__main__":
    main()