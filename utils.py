import glob
import logging
import os
from typing import List, Dict
from urllib.parse import unquote, urlparse
from dotenv import load_dotenv

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
         +-------72_HR/
         +-------30_DAY/
         +--------ROLLCALL/

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