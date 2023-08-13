import datetime
import glob
import logging
import os
import shutil
from typing import List, Dict, Tuple
from urllib.parse import quote, unquote, urlparse
from dotenv import load_dotenv
from mongodb import MongoDB
import uuid
from s3_bucket import s3Bucket

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

def clean_up_tmp_pdfs() -> None:
    baseDir = os.getenv('PDF_DIR')
    tmpDir = os.path.join(baseDir, 'tmp')

    # Look for all PDF files in the directory and its subdirectories
    pdf_files = glob.glob(os.path.join(tmpDir, '**', '*.pdf'), recursive=True)

    for pdf_file in pdf_files:
        try:
            os.remove(pdf_file)
            logging.info(f"Removed {pdf_file}")
        except Exception as e:
            logging.error(f"Error removing {pdf_file}. Error: {e}")

def check_local_pdf_dirs():

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

    ./{$PDF_DIR}/
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
         +----{terminal_name_1}
         |    +-----72_HR/
         |    +-----30_DAY/
         |    +-----ROLLCALL/
         |
       (...)
    '''

    baseDir = os.getenv('PDF_DIR')

    pdfUseDir = ['tmp/']

    typeOfPdfDirs = ['72_HR/', '30_DAY/', 'ROLLCALL']

    # Check if baseDir ends with '/'; this is to append sub directories later
    if baseDir[-1] != '/':
        baseDir = baseDir + '/'

    # Check base directory exists
    if not os.path.exists(baseDir):
        os.mkdir(baseDir)

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

def check_s3_pdf_dirs(s3: s3Bucket):

    currentDir = 'current/'
    archiveDir = 'archive/'
    typeOfPdfDirs = ['72_HR/', '30_DAY/', 'ROLLCALL/']

    if not s3.directory_exists(currentDir):
        s3.create_directory(currentDir)

    for dirType in typeOfPdfDirs:
        currPath = os.path.join(currentDir, dirType)

        if not s3.directory_exists(currPath):
            s3.create_directory(currPath)
    
    if not s3.directory_exists(archiveDir):
        s3.create_directory(archiveDir)
    

def rotate_pdfs_to_current_s3(db: MongoDB, s3: s3Bucket, updatedTerminals: List[Terminal]):

    baseDir = os.getenv('PDF_DIR')

    for terminal in updatedTerminals:

        # If 72 hour is updated
        if terminal.is72HourUpdated:

            # Check if pdf still exists
            tmpPdfPath = os.path.join(baseDir, terminal.pdfName72Hour)
            if os.path.exists(tmpPdfPath):
                tmpPdfName = os.path.basename(terminal.pdfName72Hour)

                # Create s3 destination path
                dest = os.path.join('current/72_HR/', tmpPdfName)

                # Upload to s3 current directory
                s3.upload_to_s3(tmpPdfPath, dest)

                # Update the db to reflect new PDF
                db.set_terminal_field(terminal.name, 'pdfName72Hour', dest)
                db.set_terminal_field(terminal.name, 'pdfHash72Hour', terminal.pdfHash72Hour)
                db.set_terminal_field(terminal.name, 'is72HourUpdated', True)
            else:
                logging.error(f'Unable to upload {tmpPdfPath} to s3.')

        # If 30 day is updated
        if terminal.is30DayUpdated:

            # Check if pdf still exists
            tmpPdfPath = os.path.join(baseDir, terminal.pdfName30Day)
            if os.path.exists(tmpPdfPath):
                tmpPdfName = os.path.basename(terminal.pdfName30Day)

                # Create s3 destination path
                dest = os.path.join('current/30_DAY/', tmpPdfName)

                # Uploaded to s3 current directory
                s3.upload_to_s3(tmpPdfPath, dest)

                # Update the db to reflect the new PDF
                db.set_terminal_field(terminal.name, 'pdfName30Day', dest)
                db.set_terminal_field(terminal.name, 'pdfHash30Day', terminal.pdfHash30Day)
                db.set_terminal_field(terminal.name, 'is30DayUpdated', True)
            else:
                logging.error(f'Unable to upload {tmpPdfPath} to s3.')
        
        # If rollcall is updated
        if terminal.isRollcallUpdated:

            # Check if pdf still exists
            tmpPdfPath = os.path.join(baseDir, terminal.pdfNameRollcall)
            if os.path.exists(tmpPdfPath):
                tmpPdfName = os.path.basename(terminal.pdfNameRollcall)

                # Create s3 destination path
                dest = os.path.join('current/ROLLCALL/', tmpPdfName)

                # Uploaded to s3 current directory
                s3.upload_to_s3(tmpPdfPath, dest)

                # Update the db to reflect the new PDF
                db.set_terminal_field(terminal.name, 'pdfNameRollcall', dest)
                db.set_terminal_field(terminal.name, 'pdfHashRollcall', terminal.pdfHashRollcall)
                db.set_terminal_field(terminal.name, 'isRollcallUpdated', True)
            else:
                logging.error(f'Unable to upload {tmpPdfPath} to s3.')   

def check_downloaded_pdfs(directory_path):
    """Check if at least one PDF was downloaded and log the number of PDFs in the directory."""
    num_pdf_files = len(glob.glob(os.path.join(directory_path, "*.pdf")))
    if num_pdf_files == 0:
        logging.warning("No PDFs were downloaded in the directory: %s", directory_path)
    else:
        logging.info('%d PDFs were downloaded in the directory: %s', num_pdf_files, directory_path)
    return num_pdf_files > 0

def get_pdf_name(url) -> str:
    try:
        result = urlparse(url)
        path = unquote(result.path)
        return path.split('/')[-1]
    except Exception as e:
        return str(e)
    
def gen_pdf_archive_name(terminalName, nameModifier):
    # Replace spaces with underscore in terminalName
    terminalName = terminalName.replace(' ', '_')
    
    # Get current date and time
    now = datetime.datetime.now()
    
    # Format date and time as per your requirement
    timestamp = now.strftime('%d-%b-%Y_%H%M')

    # Generate new name
    new_name = f"{terminalName}_{nameModifier}_{timestamp}.pdf"

    # Add uuid
    new_name = gen_pdf_name_uuid10(new_name)
    
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
    
def gen_pdf_name_uuid10(file_path):
    # Extract the directory, base name, and extension
    dir_path, file_name = os.path.split(file_path)
    base_name, ext = os.path.splitext(file_name)
    
    # Ensure the file is a PDF
    if ext.lower() != '.pdf':
        logging.error("%s is not a PDF!", file_path)
    
    # Generate a random UUID and take the first few characters for brevity
    random_uuid = str(uuid.uuid4())[:10]
    
    # Construct the new file name
    new_name = f"{base_name}_{random_uuid}.pdf"
    
    return new_name

def get_relative_path(subpath, pdf_path):
    # Check if subpath exists in pdf_path
    if subpath in pdf_path:
        # Find the index of the subpath in the pdf_path
        index = pdf_path.index(subpath)
        # Return the relative path
        return pdf_path[index:]
    else:
        return None

def gen_archive_dir_s3(s3: s3Bucket, terminalName: str) -> str:
    logging.info('Creating archive directories in s3 bucket: %s', s3.bucket_name)

    archiveDir = 'archive/'

    # Sub directories for sorting the different types of
    # PDFs a terminal can generate.
    dirTypes = ['72_HR/', '30_DAY/', 'ROLLCALL/']

    # Convert terminal name to snake case
    snakeCaseName = terminalName.replace(' ', '_')
    terminalArchiveDir = archiveDir + snakeCaseName + '/'

    try:
        # Create base terminal folder in archive if it doesn't exist.
        if not s3.directory_exists(terminalArchiveDir):
            s3.create_directory(terminalArchiveDir)
            logging.info('Created directory %s in s3.', terminalArchiveDir)

        for dirType in dirTypes:
            subDir = terminalArchiveDir + dirType

            if not s3.directory_exists(subDir):
                s3.create_directory(subDir)
                logging.info('Created sub directory %s in s3.', subDir)

    except Exception as e:
        logging.error(f"Error while generating archive directories for {terminalName} in bucket {s3.bucket_name}. Error: {str(e)}")
        raise

    return terminalArchiveDir

def archive_pdfs_s3(db: MongoDB, s3: s3Bucket, updatedTerminals: List[Terminal]):

    for terminal in updatedTerminals:

        # Retrieve terminal doc from DB
        storedTerminal = db.get_doc_by_field_value('name', terminal.name)

        # Get archive directory
        if storedTerminal['archiveDir'] == 'empty':
            archiveDir = gen_archive_dir_s3(s3, terminal.name)
            db.set_terminal_field(terminal.name, 'archiveDir', archiveDir)
        else:
            archiveDir = storedTerminal['archiveDir']

        # Check archive directory exists
        if not s3.directory_exists(archiveDir):
            logging.error(f'{archiveDir} was not found in S3. Skipping archive function...')
            continue

        # 72 hour schedule was updated
        if terminal.is72HourUpdated:

            # Retrieve the old current pdf
            oldPdfPath = storedTerminal['pdfName72Hour']

            # If there is a 72 hour pdf in s3
            if oldPdfPath != 'empty':

                # Generate archive name for the PDF
                archiveName = gen_pdf_archive_name(terminal.name, '72_HR')

                # Generate archive path
                dest = os.path.join(archiveDir, '72_HR/')
                dest = os.path.join(dest, archiveName)

                # Move it to archive directory in s3
                s3.move_object(oldPdfPath, dest)
                logging.info(f'Archived {terminal.name} 72 hour pdf {oldPdfPath} at {dest}.')
            else:
                logging.info(f'No 72 hour schedule to archive for {terminal.name}.')
            
        # 30 Day schedule was updated
        if terminal.is30DayUpdated:

            # Retrieve the old current pdf
            oldPdfPath = storedTerminal['pdfName30Day']

            # If there is a 30 day pdf in s3
            if oldPdfPath != 'empty':

                # Generate archive name for the PDF
                archiveName = gen_pdf_archive_name(terminal.name, '30_DAY')

                # Generate archive path
                dest = os.path.join(archiveDir, '30_DAY/')
                dest = os.path.join(dest, archiveName)

                # Move it to archive directory in s3
                s3.move_object(oldPdfPath, dest)
                logging.info(f'Archived {terminal.name} 30 day pdf {oldPdfPath} at {dest}.')
            else:
                logging.info(f'No 30 day schedule to archive for {terminal.name}.')

        # Rollcall was updated
        if terminal.isRollcallUpdated:

            # Retrieve the old current pdf
            oldPdfPath = storedTerminal['pdfNameRollcall']

            # If there is a rollcall pdf in s3
            if oldPdfPath != 'empty':
                # Generate archive name for the PDF
                archiveName = gen_pdf_archive_name(terminal.name, 'ROLLCALL')

                # Generate archive path
                dest = os.path.join(archiveDir, 'ROLLCALL/')
                dest = os.path.join(dest, archiveName)

                # Move it to archive directory in s3
                s3.move_object(oldPdfPath, dest)
                logging.info(f'Archived {terminal.name} rollcall pdf {oldPdfPath} at {dest}.')
            else:
                logging.info(f'No rollcall pdf to archive for {terminal.name}.')






