from datetime import datetime
import hashlib
import os
import logging
from urllib.parse import unquote, urlparse
from PyPDF2 import PdfReader
import utils

class Pdf:

    def __init__(self, link):

        self.filename = ""
        self.link = link
        self.hash = ""
        self.first_seen_time = ""
        self.cloud_path = ""
        self.modify_time = ""
        self.creation_time = ""
        self.type = ""
        self.terminal = ""
        self.should_discard = False

        # Set first_seen_time
        self._gen_first_seen_time()

        # Download PDF and set filename and cloud_path
        self._download()
        if self.filename is None:
            self.should_discard = True
            return

        # Calc PDF Hash and set hash
        self._calc_hash()
        if self.hash is None:
            self.should_discard = True
            return

        # Extract the metadata of the PDF
        self._get_pdf_metadata()
    
    def _download(self):
        logging.info(f'Starting download of PDF from {self.link}...')

        download_dir = os.getenv("PDF_DIR")

        if download_dir is None:
            logging.critical('PDF_DIR environment variable is not set.')
            return

        download_dir = os.path.join(download_dir, "tmp/")

        # Get the filename from the URL
        filename = utils.get_pdf_name(self.link)
        filename = utils.gen_pdf_name_uuid10(filename)

        # Get PDF from link
        response = utils.get_with_retry(self.link)

        # If download was successful
        if response is not None and response.status_code == 200:
            # Make sure the directory exists, if not, create it.
            os.makedirs(download_dir, exist_ok=True)
            
            # Combine the directory with the filename
            filepath = os.path.join(download_dir, filename)
            
            # Write the content of the request to a file
            with open(filepath, 'wb') as f:
                f.write(response.content)
            
            logging.debug(f'Successfully downloaded {self.link} at {filepath}')

            # Set filename
            self.filename = filename

            # Store relative path for compatability
            self.cloud_path = utils.get_relative_path('tmp/', filepath)
            return
        else:
            logging.warning(f'Download failed for link: {self.link}')
            self.filename = None
            self.cloud_path = None
            return
        
    def get_local_path(self):

        if self.cloud_path is None:
            return None
        
        base_dir = os.getenv('PDF_DIR')
        local_path = os.path.join(base_dir, self.cloud_path)
        return local_path
    
    def set_type(self, type: str) -> None:

        valid_types = ['72_HR', '30_DAY', 'ROLLCALL']
        
        if type in valid_types:
            self.type = type
            logging.info(f'{self.filename} type set to {self.type}.')
        else:
            self.type = None
            logging.error(f'Failed to set {self.filename} type. Invalid type {type}.')

    def set_terminal(self, terminal_name: str) -> None:

        if utils.is_valid_string(terminal_name):
            self.terminal = terminal_name
            logging.info(f'Set {self.filename} terminal to {terminal_name}.')
        else:
            logging.error(f'Unable to set {self.filename} terminal. Invalid terminal string: {terminal_name}.')
    
    def _calc_hash(self):
        logging.info(f'Calculating hash for {self.filename}.')

        sha256_hash = hashlib.sha256()

        try:
            with open(self.get_local_path(), "rb") as f:
                for byte_block in iter(lambda: f.read(4096), b""):
                    sha256_hash.update(byte_block)
        except FileNotFoundError as e:
            logging.error(f'File {self.get_local_path()} not found. Exception: {e}')
            self.hash = None
        except Exception as e:
            logging.error(f'Unexpected error reading file {self.get_local_path()}. Exception: {e}', exc_info=True)
            self.hash = None
            return

        self.hash = sha256_hash.hexdigest()

    def _gen_first_seen_time(self):
        """
        Generate the current time in YYYYMMDDHHMMSS format and set it as firstSeenTime
        """
        self.first_seen_time = datetime.utcnow().strftime('%Y%m%d%H%M%S')

    def _get_pdf_metadata(self):
        logging.info(f'Reading metadata from {self.get_local_path()}.')

        try:
            with open(self.get_local_path(), 'rb') as file:
                pdf_reader = PdfReader(file)
                metadata = pdf_reader.metadata
                creation_date = metadata.get('/CreationDate', None)
                modification_date = metadata.get('/ModDate', None)
            
            self.creation_time = utils.format_pdf_metadata_date(creation_date)
            self.modify_time = utils.format_pdf_metadata_date(modification_date)
        
        except FileNotFoundError as e:
            logging.error(f'File {self.get_local_path()} not found. Exception: {e}')
            self.creation_time = None
            self.modify_time = None
        except KeyError as e:
            logging.warning(f'PDF metadata does not contain creation or modification dates. Exception: {e}')
            self.creation_time = None
            self.modify_time = None
        except Exception as e:
            logging.error(f'Unexpected error reading PDF metadata for {self.get_local_path()}. Exception: {e}', exc_info=True)
            self.creation_time = None
            self.modify_time = None
          
    def to_dict(self):
        """
        Convert this PDF object to a dictionary, suitable for storing in Firestore.
        The should_discard attribute is excluded from the returned dictionary.
        """
        return {
            'filename': self.filename,
            'link': self.link,
            'hash': self.hash,
            'firstSeenTime': self.first_seen_time,
            'cloud_path': self.cloud_path,
            'modifyTime': self.modify_time,
            'creationTime': self.creation_time,
            'type': self.type,
            'terminal': self.terminal
            # 'shouldDiscard': self.should_discard  # This line is intentionally omitted
        }
    
    @classmethod
    def from_dict(cls, data):
        """
        Create a PDF object from a dictionary (e.g., a Firestore document).
        The should_discard attribute is set to False by default.
        """
        pdf = cls(
            link=data['link'],
        )
        pdf.filename = data['filename']
        pdf.hash = data['hash']
        pdf.first_seen_time = data['firstSeenTime']
        pdf.cloud_path = data['cloud_path']
        pdf.modify_time = data['modifyTime']
        pdf.creation_time = data['creationTime']
        pdf.type = data['type']
        pdf.terminal = data['terminal']

        # Setting should_discard to False when object is unmarshalled from the database
        pdf.should_discard = False

        return pdf