import logging
import os
from dotenv import load_dotenv

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
    
def checkPDFDirectories(baseDir):

    # Check for trailing '/'
    if baseDir[-1] != '/':
        baseDir = baseDir + '/'

    # Check if base directory exists
    if not os.path.exists(baseDir):
        logging.info('No existing {baseDir} directory. Creating new one.')
        os.mkdir(baseDir)

    pdf3DayDir = baseDir + '3_Day/'
    if not os.path.exists(pdf3DayDir):
        logging.info('No exsting {pdf3DayDir} directory. Creating new one.')
        os.mkdir(pdf3DayDir)

    pdf30DayDir = baseDir + '30_Day/'
    if not os.path.exists(pdf30DayDir):
        logging.info('No existing {pdf30DayDir} directory. Creating new one.')
        os.mkdir(pdf30DayDir)

    pdfRollcallDir = baseDir + 'Rollcall/'
    if not os.path.exists(pdfRollcallDir):
        logging.info('No existing {pdfRollcallDir} directory. Creating new one.')
        os.mkdir(pdfRollcallDir)
    

