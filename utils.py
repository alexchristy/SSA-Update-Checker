from functools import wraps
import glob
import hashlib
import logging
import os
import re
import time
from urllib.parse import quote, unquote, urlparse
import uuid
from dotenv import load_dotenv
import requests

def timing_decorator(func):
    @wraps(func)  # Preserve function metadata
    def wrapper(*args, **kwargs):
        try:
            start_time = time.time()  # Start time
            result = func(*args, **kwargs)  # Execute function
            end_time = time.time()  # End time
            elapsed_time = end_time - start_time  # Calculate elapsed time
            logging.info(f"{func.__name__} took {elapsed_time} seconds to run")  # Log elapsed time
            return result  # Return the original function's return value
        except Exception as e:
            logging.error(f"An exception occurred while running {func.__name__}: {e}")
            raise  # Re-raise the caught exception
    return wrapper

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
    
def get_relative_path(subpath, pdf_path):
    # Check if subpath exists in pdf_path
    if subpath in pdf_path:
        # Find the index of the subpath in the pdf_path
        index = pdf_path.index(subpath)
        # Return the relative path
        return pdf_path[index:]
    else:
        return None

# Time the function execution to debug performance and any
# other issues that may arise when downloading PDFs and terminal
# pages
@timing_decorator
def get_with_retry(url: str):
    logging.debug('Entering get_with_retry() requesting: %s', url)

    delay = 2

    url = ensure_url_encoded(url)

    for attempt in range(3):

        try:
            logging.debug("Sending GET request.")
            
            # Send GET request to the website with a timeout
            response = requests.get(url, timeout=5)
            
            logging.debug("GET request successful.")
            return response  # Exit function if request was successful
        
        except requests.Timeout:
            logging.error("Request to %s timed out.", url)
            
        except Exception as e:  # Catch any exceptions
            logging.error('Request to %s failed in get_with_retry().', url)
            logging.debug('Error: %s', e)

        # If it was not the last attempt
        if attempt < 2:
            logging.info('Retrying request to %s in %d seconds...', url, delay)
            time.sleep(delay)  # Wait before next attempt
            delay *= 2
        
        # It was the last attempt
        else:
            logging.error('All attempts failed.')

    return None

def calc_sha256_hash(input_string: str) -> str:
    """
    Calculate the SHA-256 hash of a given input string.
    
    :param input_string: The input string for which to calculate the SHA-256 hash
    :return: The SHA-256 hash of the input string, represented as a hexadecimal string
    """
    # Create a new SHA-256 hash object
    sha256_hash = hashlib.sha256()
    
    # Update the hash object with the bytes of the input string
    sha256_hash.update(input_string.encode('utf-8'))
    
    # Get the hexadecimal representation of the hash
    hex_digest = sha256_hash.hexdigest()
    
    return hex_digest

def is_valid_sha256(s: str) -> bool:
    """
    Check if the given string is a valid SHA256 checksum.
    
    :param s: The string to check
    :type s: str
    :return: True if the input string is a valid SHA256 checksum, False otherwise
    :rtype: bool
    """
    
    if len(s) != 64:
        return False
    
    # Regular expression pattern for a hexadecimal number
    if re.match("^[a-fA-F0-9]{64}$", s):
        return True
    
    return False

def normalize_url(url: str) -> str:
    logging.debug('Entering normarlize_url()')

    parsedUrl = urlparse(url)
    hostname = str(parsedUrl.netloc)
    normalizedUrl = 'https://' + hostname + '/'
    return normalizedUrl

def is_valid_string(value) -> bool:
    return isinstance(value, str)

def get_pdf_name(url) -> str:
    try:
        result = urlparse(url)
        path = unquote(result.path)
        return path.split('/')[-1]
    except Exception as e:
        return str(e)
    
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

def format_pdf_metadata_date(date_str):
    if date_str is None:
        return None
    
    # Regular expression to extract the date components
    match = re.match(r"D:(\d{4})(\d{2})(\d{2})(\d{2})(\d{2})(\d{2})", date_str)
    if match:
        # Format the date to YYYYMMDDHHMMSS
        return ''.join(match.groups())
    else:
        return None