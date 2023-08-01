import datetime
import glob
import logging
import os
import shutil
from typing import List, Dict, Tuple
from urllib.parse import quote, unquote, urlparse
from dotenv import load_dotenv
from mongodb import MongoDB

from terminal import Terminal

def check_env_variables(variables):
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
    
def check_pdf_directories(baseDir):

    '''
    The PDFs will be stored in this structure under whatever is set as the
    base directory with the $PDF_DIR env variable. The tmp directory is where
    PDFs will be downloaded everytime the program runs. When sorting the pdfs 
    they will be downloaded into the tmp/ dir but then be sorted into their type
    directories. The current directory will hold all of the most up to date PDFs
    of each type. The archive directory will have a folder for each terminal 
    where all PDFs outdated PDFs will be sorted and stored for training the AI
    models. Since the PDFs will have the name of terminal they are from it will
    be easy to sort by name and pick out the correct PDFs.

    Structure:

    ./{baseDir}/
    |
    +----tmp/
    |    +--72_HR/
    |    +--30_DAY/
    |    +--ROLLCALL/
    |
    +----current/
    |    +------72_HR/
    |    +------30_DAY/
    |    +------ROLLCALL/
    |
    +----archive/

    '''

    pdfUseDir = ['tmp/', 'current/']

    typeOfPdfDirs = ['72_HR/', '30_DAY/', 'ROLLCALL']

    # Check if baseDir ends with '/'; this is to append sub directories later
    if baseDir[-1] != '/':
        baseDir = baseDir + '/'

    # Check base directory exists
    if not os.path.exists(baseDir):
        os.mkdir(baseDir)

    # Create archive directory seperately
    archiveDir = baseDir + 'archive/'
    if not os.path.exists(archiveDir):
        os.mkdir(archiveDir)

    # Iterate through the use directories: tmp/, current/, archive/
    for useDir in pdfUseDir:

        # Create relative path to the current use directory
        useDir = baseDir + useDir

        # Check if the use directory exists
        if not os.path.exists(useDir):
            os.mkdir(useDir)

        # Iterate through the different pdf type directories: 72_HR, 30_DAY, ROLLCALL
        for typeDir in typeOfPdfDirs:

            # Create relative path to pdf type directory
            typeDir = useDir + typeDir

            # Check if it exists
            if not os.path.exists(typeDir):
                os.mkdir(typeDir)
    
    return None

def gen_archive_dirs(listOfTerminals: List[Terminal], dir: str) -> Dict[str, str]:

    archiveDir = dir + 'archive/'

    archiveFolderDict = {}

    pdfTypeDirs = ['72_HR/', '30_DAY/', 'ROLLCALL/']

    for terminal in listOfTerminals:

        # Generate folder name and create path
        terminaName = terminal.name
        folderName = terminaName.replace(' ', '_')
        folderPath = archiveDir + folderName

        # Create name folder if it does not exist
        if not os.path.exists(folderPath):
            os.mkdir(folderPath)

        # Create sub directories for each terminal archive to
        # sort the different types of PDFs that will be stored.
        for typeDir in pdfTypeDirs:
            typeDirPath = folderPath + typeDir

            if not os.path.exists(typeDirPath):
                os.mkdir(typeDirPath)
        
        # Append to dictionary
        archiveFolderDict[terminaName] = folderPath
    
    return archiveFolderDict

def archive_old_pdfs(db: MongoDB, terminalUpdates: List[Tuple[str, Dict[str, str]]], archiveDirDict: Dict[str, str]) -> None:

    for terminalName, updates in terminalUpdates:

        # Retrieve updates
        pdf72HourUpdate = updates.get('72_HR')
        pdf30DayUpdate = updates.get('30_DAY')
        pdfRollcallUpdate = updates.get('ROLLCALL')

        # Retrieve DB document for terminal
        doc = db.get_doc_by_attr_value('name', terminalName)

        if doc is None:
            return None

        terminalArchiveDir = archiveDirDict[terminalName]

        # If 72 Hour schedule PDF was updated
        if pdf72HourUpdate is not None:
            # Retrieve the path from DB to the old 72 hour schedule PDF
            old72HourPdfPath = doc['pdfName72Hour']

            # Generate a new name for the PDF that will be archived
            archivedName = generate_pdf_name(terminalName, '72HR')

            # Move old PDF to archive directory and rename
            archiveDest = terminalArchiveDir + '72_HR/' + archivedName
            shutil.move(old72HourPdfPath, archiveDest)

            logging.info('%s 72 hour PDF from terminal: %s was archived at: %s', old72HourPdfPath, terminalName, archiveDest)
        
        # If 30 Day schedule PDF was updated
        if pdf30DayUpdate is not None:
            # Retrieve the path from DB to the old 30 day schedule PDF
            old30DayPdfPath = doc['pdfName30Day']

            # Generate a new name for the PDF that will be archived
            archivedName = generate_pdf_name(terminalName, '30DAY')

            # Move old PDF to archive directory and rename
            archiveDest = terminalArchiveDir + '30_DAY/' + archivedName
            shutil.move(old30DayPdfPath, archiveDest)

            logging.info('%s 30 day PDF from terminal: %s was archived at: %s', old30DayPdfPath, terminalName, archiveDest)

        if pdfRollcallUpdate is not None:
            # Retrieve the path from the DB to the old rollcall PDF
            oldRollcallPdfPath = doc['pdfNameRollcall']

            # Generate a new name for the PDF that will be archived
            archivedName = generate_pdf_name(terminalName, 'ROLLCALL')

            # Move old PDF to archive directory and rename
            shutil.move(oldRollcallPdfPath, terminalArchiveDir + 'ROLLCALL/' + archivedName)

            logging.info('%s rolllcall PDF from Terminal: %s was archived at: %s', oldRollcallPdfPath, terminalName, archiveDest)
        
    return None


            

def check_downloaded_pdfs(directory_path):
    """Check if at least one PDF was downloaded and log the number of PDFs in the directory."""
    num_pdf_files = len(glob.glob(os.path.join(directory_path, "*.pdf")))
    if num_pdf_files == 0:
        logging.warning("No PDFs were downloaded in the directory: %s", directory_path)
    else:
        logging.info('%d PDFs were downloaded in the directory: %s', num_pdf_files, directory_path)
    return num_pdf_files > 0

def get_pdf_name(url):
    try:
        result = urlparse(url)
        path = unquote(result.path)
        return path.split('/')[-1]
    except Exception as e:
        return str(e)
    
def generate_pdf_name(terminalName, nameModifier):
    # Replace spaces with underscore in terminalName
    terminalName = terminalName.replace(' ', '_')
    
    # Get current date and time
    now = datetime.datetime.now()
    
    # Format date and time as per your requirement
    timestamp = now.strftime('%d-%b-%Y_%H%M')
    
    # Generate new name
    new_name = f"{terminalName}_{nameModifier}_{timestamp}.pdf"
    
    return new_name

def ensure_url_encoded(url):
    # Unquote the URL. If it's already encoded, this will decode it.
    unquoted_url = unquote(url)

    # Check if the URL was encoded by comparing it with the unquoted URL.
    if unquoted_url == url:
        # If the URLs are the same, it wasn't encoded, so we encode it.
        # We specify safe characters that shouldn't be encoded.
        return quote(url, safe=':/.-')
    else:
        # If the URLs are different, it was already encoded.
        return url