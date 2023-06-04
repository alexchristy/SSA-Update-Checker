import scraper
from terminal import *
from mongodb import *
from telegram import Bot
import asyncio
import os
import glob

api_token = '6240797164:AAHpWDh3z1qccY_APkbPteY78ZmrsRIvKGo'

async def sendPDF(terminalName, chatID, pdfPath):
    bot = Bot(api_token)
    await bot.send_message(chatID, "Update from " + terminalName)
    with open(pdfPath, 'rb') as f:
        await bot.send_document(chatID, f)

async def main():
    homeDir = '/home/alex/Documents/SpaceK'
    url = 'https://www.amc.af.mil/AMC-Travel-Site'
    pdfDir = './pdfs/'

    # Enter correct directory
    os.chdir(homeDir)

    # Intialize MongoDB
    db = MongoDB("SmartSpaceA", "Terminals")

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