import os
from firebase_admin import credentials, firestore, initialize_app
from utils import is_valid_sha256
import logging
from pdf import Pdf

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
            logging.info(f'PDF with hash {pdf.hash} has been seen before.')
            return True
        else:
            # The document does not exist, indicating that the PDF is new
            logging.info(f'PDF with hash {pdf.hash} has not been seen before.')
            return False
        
    def archive_pdf(self, pdf: Pdf) -> bool:

        if pdf.should_discard:
            logging.warning(f'Not archiving {pdf.filename}. Marked for being discarded.')
            return False

        collection_name = os.getenv('PDF_ARCHIVE_COLL')

        self.set_document(collection_name, pdf.hash, pdf.to_dict())
        logging.info(f'Inserted PDF into archive at {pdf.hash}')