import scraper
from terminal import *
from mongodb import *
from telegram import Bot
import asyncio
import os
import glob
import sys
from dotenv import load_dotenv

# List of variables to check
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

async def main():
    
    # Get the absolute path of the script
    scriptPath = os.path.abspath(__file__)

    # Set the home directory to the directory of the script
    homeDirectory = os.path.dirname(scriptPath)

    url = 'https://www.amc.af.mil/AMC-Travel-Site'

    # Create pdf directory if it doesn't exist
    if not os.path.exists('./pdfs'):
        os.mkdir('./pdfs')
    
    pdfDir = './pdfs/'

    # Enter correct directory
    os.chdir(homeDirectory)

    # Intialize MongoDB
    db = MongoDB(mongoDBName, mongoCollectionName, username=mongoUsername, password=mongoPassword)
    db.connect()

    # Every 10 mins
    while True:

        # Retrieve all PDFS
        scraper.getTerminalInfo(db, url)
        scraper.download3DayPDFs(db, pdfDir)

        # Check at least one PDF was downloaded
        numPDFFiles = glob.glob(os.path.join(pdfDir, "*.pdf"))
        if len(numPDFFiles) == 0:
            print("No downloaded PDFs!")
            await asyncio.sleep(60)
            continue

        # Check which PDFs changed; compare with db stored hashes
        updatedTerminals = scraper.calcPDFHashes(db, pdfDir)

        # Will be always be empty on the first run
        if updatedTerminals != []:

            for terminalName in updatedTerminals:
                # Info temrinal info from DB
                currentTerminal = db.getTerminalByName(terminalName)
                subscribers = currentTerminal.chatIDs
                pdfName = currentTerminal.pdfName3Day

                # Send PDFs to all subscribers
                for chatID in subscribers:
                    await sendPDF(terminalName, chatID, f'./pdfs/{pdfName}')

        # Wait 10 minutes
        await asyncio.sleep(300)

if __name__ == "__main__":
    asyncio.run(main())

def checkEnvVariables(variables):
    # Load environment variables from .env file
    load_dotenv()

    emptyVariables = []
    for var in variables:
        value = os.getenv(var)
        if not value:
            emptyVariables.append(var)

    if emptyVariables:
        errorMessage = f"The following variable(s) are missing or empty in .env: {', '.join(emptyVariables)}"
        raise ValueError(errorMessage)

async def sendPDF(terminalName, chatID, pdfPath):
    bot = Bot(api_token)
    await bot.send_message(chatID, "Update from " + terminalName)
    with open(pdfPath, 'rb') as f:
        await bot.send_document(chatID, f)