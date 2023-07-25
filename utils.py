import glob
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

    modifiedBaseDir = ""

    # Check for trailing '/'
    if baseDir[-1] != '/':
        modifiedBaseDir = baseDir + '/'
    else:
        modifiedBaseDir = baseDir

    # Check if base directory exists
    if not os.path.exists(modifiedBaseDir):
        logging.info('No existing {baseDir} directory. Creating new one.')
        os.mkdir(modifiedBaseDir)

    pdf72HourDir = modifiedBaseDir + '72_HR/'
    if not os.path.exists(pdf72HourDir):
        logging.info('No exsting {pdf72HourDir} directory. Creating new one.')
        os.mkdir(pdf72HourDir)

    pdf30DayDir = modifiedBaseDir + '30_DAY/'
    if not os.path.exists(pdf30DayDir):
        logging.info('No existing {pdf30DayDir} directory. Creating new one.')
        os.mkdir(pdf30DayDir)

    pdfRollcallDir = modifiedBaseDir + 'ROLLCALL/'
    if not os.path.exists(pdfRollcallDir):
        logging.info('No existing {pdfRollcallDir} directory. Creating new one.')
        os.mkdir(pdfRollcallDir)
    
    return baseDir, pdf72HourDir, pdf30DayDir, pdfRollcallDir

def checkDownloadedPDFs(directory_path):
    """Check if at least one PDF was downloaded and log the number of PDFs in the directory."""
    num_pdf_files = len(glob.glob(os.path.join(directory_path, "*.pdf")))
    if num_pdf_files == 0:
        logging.warning("No PDFs were downloaded in the directory: %s", directory_path)
    else:
        logging.info('%d PDFs were downloaded in the directory: %s', num_pdf_files, directory_path)
    return num_pdf_files > 0