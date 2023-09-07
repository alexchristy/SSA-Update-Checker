import os
from firebase_admin import credentials, firestore, initialize_app
from utils import is_valid_sha256
import logging
from pdf import Pdf
from utils import is_valid_sha256
from terminal import Terminal

class FirestoreClient:
    
    def __init__(self):
        # Get the path to the Firebase Admin SDK service account key JSON file from an environment variable
        fs_creds_path = os.getenv('FS_CRED_PATH')
        
        # Initialize the credentials with the JSON file
        cred = credentials.Certificate(fs_creds_path)
        
        # Initialize the Firebase application with the credentials
        self.app = initialize_app(cred)
        
        # Create the Firestore client
        self.db = firestore.client(app=self.app)

    def set_document(self, collection_name, document_name, data):
        """
        Set the data for a document in a collection
        :param collection_name: The name of the collection
        :param document_name: The name of the document
        :param data: The data to set for the document
        """
        doc_ref = self.db.collection(collection_name).document(document_name)
        doc_ref.set(data)
        
    def upsert_document(self, collection_name, document_name, data):
        """
        Upsert data for a document in a collection.
        This will update the document with the provided data, or create it if it doesn't exist.
        :param collection_name: The name of the collection
        :param document_name: The name of the document
        :param data: The data to set for the document
        """
        doc_ref = self.db.collection(collection_name).document(document_name)
        doc_ref.set(data, merge=True)

    def upsert_terminal_info(self, terminal: Terminal):

        """
        Upsert terminal object in the Terminals collection. This will update
        the fields of the terminal not including the hashes if the document 
        already exists. If the terminal does not exist, then the pdf hash
        fields will also be updated.
        """

        # Get enviroment variable
        terminal_coll = os.getenv('TERMINAL_COLL')

        # Get the terminal document
        doc_ref = self.db.collection(terminal_coll).document(terminal.name)
        doc = doc_ref.get()

        if doc.exists:

            # Specify what fields are terminal info
            updates = {
                'name': terminal.name,
                'link': terminal.link,
                'pagePosition': terminal.pagePosition,
                'location': terminal.location,
                'group': terminal.group,
            }

            doc_ref.update(updates)
        else:
            self.upsert_document(terminal_coll, terminal.name, terminal.to_dict())

    def update_terminal_pdf_hash(self, pdf: Pdf):

        """
        This will update the proper terminal pdf hash based on the type the PDF
        object self reports.
        """

        # Get enviroment variable
        terminal_coll = os.getenv('TERMINAL_COLL')

        doc_ref = self.db.collection(terminal_coll).document(pdf.terminal)

        if pdf.type == '72_HR':
            doc_ref.update({'pdf72HourHash': pdf.hash})
            logging.info(f'Updated {pdf.terminal} with new 72 hour hash.')
        elif pdf.type == '30_DAY':
            doc_ref.update({'pdf30DayHash': pdf.hash})
            logging.info(f'Updated {pdf.terminal} with new 30 day hash.')
        elif pdf.type == 'ROLLCALL':
            doc_ref.update({'pdfRollcallHash': pdf.hash})
            logging.info(f'Updated {pdf.terminal} with new rollcall hash.')
        else:
            logging.error(f'Unable to update terminal with {pdf.filename}. Invalid PDF type: {pdf.type}.')

    def upsert_pdf(self, pdf: Pdf):

        """
        Upsert Pdf object into the PDF Archive/seen before collection. This
        will update the field of the terminal with the provided data or create
        it if it does not exist.
        """

        # Get enviroment variable
        pdf_archive_coll = os.getenv('PDF_ARCHIVE_COLL')

        self.upsert_document(pdf_archive_coll, pdf.hash, pdf.to_dict())

    def pdf_seen_before(self, pdf: Pdf) -> bool:
        """
        Check if a PDF file, identified by its SHA-256 hash, has been seen before.
        :param hash: The SHA-256 hash of the PDF file
        :return: True if the PDF has been seen before, False otherwise
        """
        
        # If hash is not valid return True so that the PDF
        # is discarded.
        if not is_valid_sha256(pdf.hash):
            logging.error(f'{pdf.hash} is not a valid sha256 hash.')
            return True
        
        # Assuming 'pdfs' is the collection where PDF information is stored.
        # Change 'pdfs' to your actual collection name.
        collection_name = os.getenv('PDF_ARCHIVE_COLL')
        
        # Create a reference to the document using the SHA-256 hash as the document ID
        doc_ref = self.db.collection(collection_name).document(pdf.hash)
        
        # Try to retrieve the document
        doc = doc_ref.get()
        
        # Check if the document exists
        if doc.exists:
            # The document exists, indicating that the PDF has been seen before
            logging.info(f'{pdf.filename} with hash {pdf.hash} has been seen before.')
            return True
        else:
            # The document does not exist, indicating that the PDF is new
            logging.info(f'{pdf.filename} with hash {pdf.hash} has NEVER been seen before.')
            return False
        
    def archive_pdf(self, pdf: Pdf) -> bool:

        if pdf.seen_before:
            logging.warning(f'Not archiving {pdf.filename}. Marked for being discarded.')
            return False

        collection_name = os.getenv('PDF_ARCHIVE_COLL')

        self.set_document(collection_name, pdf.hash, pdf.to_dict())
        logging.info(f'Inserted PDF into archive at {pdf.hash}')

    def get_pdf_by_hash(self, hash: str) -> Pdf|None:
        """
        This function takes in a hash and retrieves the corresponding PDF
        object form the PDF archive/seen before collection in the database.
        
        :param hash: The SHA-256 hash of the PDF file
        :return: Pdf object if found, otherwise None
        """

        logging.info('Entering get_pdf_by_hash().')
        
        # Verify that the hash is a valid sha256 hash
        if not is_valid_sha256(hash):
            logging.error(f'Invalid hash supplied: {hash}')
            return None
        
        # Get the name of the collection from an environment variable
        collection_name = os.getenv('PDF_ARCHIVE_COLL')
        
        # Create a reference to the document using the SHA-256 hash as the document ID
        doc_ref = self.db.collection(collection_name).document(hash)
        
        # Try to retrieve the document
        doc = doc_ref.get()
        
        # Check if the document exists
        if doc.exists:
            # The document exists, so we retrieve its data and create a Pdf object
            logging.info(f'PDF with hash {hash} found in the database.')
            
            # Get the document's data
            pdf_data = doc.to_dict()
            
            # Create a Pdf object from the retrieved data
            pdf_obj = Pdf.from_dict(pdf_data)
            
            # Return the Pdf object
            return pdf_obj
        else:
            # The document does not exist
            logging.warning(f'PDF with hash {hash} does not exist in the database.')
            
            # Return None to indicate that no PDF was found
            return None
        
    def get_all_terminals(self):

        """
        This function returns all the stored terminal objects stored in the database.
        """

        terminal_coll = os.getenv('TERMINAL_COLL')

        terminals_ref = self.db.collection(terminal_coll)

        terminals = terminals_ref.stream()

        terminal_objects = []

        for terminal in terminals:
            curr_terminal = Terminal.from_dict(terminal.to_dict())  # Convert to dict here
            terminal_objects.append(curr_terminal)

        return terminal_objects
