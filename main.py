import scraper
from utils import checkEnvVariables
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
    'MONGO_PASSWORD'
]

# Check if all .env variables are set
try:
    checkEnvVariables(variablesToCheck)
    
    # Load environment variables from .env file
    api_token = os.getenv('TELEGRAM_API_TOKEN')
    mongoDBName = os.getenv('MONGO_DB')
    mongoCollectionName = os.getenv('MONGO_COLLECTION')
    mongoUsername = os.getenv('MONGO_USERNAME')
    mongoPassword = os.getenv('MONGO_PASSWORD')

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


async def sendPDF(terminalName, chatID, pdfPath):
    bot = Bot(api_token)
    await bot.send_message(chatID, "Update from " + terminalName)
    with open(pdfPath, 'rb') as f:
        await bot.send_document(chatID, f)

async def main():
    
    logging.info('Program started.')

    # Get the absolute path of the script
    scriptPath = os.path.abspath(__file__)

    # Set the home directory to the directory of the script
    homeDirectory = os.path.dirname(scriptPath)

    url = 'https://www.amc.af.mil/AMC-Travel-Site'

    # Create pdf directory if it doesn't exist
    if not os.path.exists('./pdfs'):
        logging.info('No existing pdf directory. Creating new one.')
        os.mkdir('./pdfs')
    
    pdfDir = './pdfs/'

    # Enter correct directory
    os.chdir(homeDirectory)

    # Intialize MongoDB
    logging.info('Starting MongoDB.')
    db = MongoDB(mongoDBName, mongoCollectionName, username=mongoUsername, password=mongoPassword)
    db.connect()

    # Every 5 mins
    while True:

        logging.debug('Starting PDF retrieval process.')

        # Retrieve all PDFS
        scraper.getTerminalInfo(db, url)
        scraper.download3DayPDFs(db, pdfDir)

        # Check at least one PDF was downloaded
        numPDFFiles = glob.glob(os.path.join(pdfDir, "*.pdf"))
        if len(numPDFFiles) == 0:
            logging.warning("No PDFs were downloaded.")
            await asyncio.sleep(60)
            continue

        # Check which PDFs changed; compare with db stored hashes
        updatedTerminals = scraper.calcPDFHashes(db, pdfDir)

        # Will be always be empty on the first run
        if updatedTerminals != []:

            logging.info('%s terminals have updated their PDFs.', len(updatedTerminals))
            logging.debug('The following terminals have updated their PDFs: %s', updatedTerminals)

            for terminalName in updatedTerminals:
                # Info temrinal info from DB
                currentTerminal = db.getTerminalByName(terminalName)
                subscribers = currentTerminal.chatIDs
                pdfName = currentTerminal.pdfName3Day

                logging.info('%s subscribers will recieve the %s terminal update.', len(subscribers), terminalName)
                logging.debug('The following chatIDs will recieve the %s terminal update: %s', terminalName, subscribers)

                # Send PDFs to all subscribers
                for chatID in subscribers:
                    await sendPDF(terminalName, chatID, os.path.join(pdfDir, pdfName))
        else:
            logging.info('No PDFs were updated.')

        # Wait 10 minutes
        await asyncio.sleep(300)

if __name__ == "__main__":
    asyncio.run(main())