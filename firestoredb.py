import os
from firebase_admin import credentials, firestore, initialize_app

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