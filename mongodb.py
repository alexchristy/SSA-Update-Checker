import logging
from typing import List
from pymongo import MongoClient
from pymongo.server_api import ServerApi
from pymongo.errors import WriteError, DuplicateKeyError, PyMongoError
from terminal import Terminal
from urllib.parse import quote_plus

class MongoDB:

    def __init__(self, db_name, collection_name, host='localhost', port=27017, username=None, password=None):
        self.db_name = db_name
        self.collection_name = collection_name
        self.host = host
        self.port = port
        self.username = quote_plus(username)
        self.password = quote_plus(password)

    def connect_local(self):
        try:
            if self.username and self.password:
                uri = f"mongodb://{self.username}:{self.password}@{self.host}:{self.port}/{self.db_name}?authMechanism=SCRAM-SHA-256&authSource=admin"
                self.client = MongoClient(uri)
            else:
                self.client = MongoClient(self.host, self.port)
            
            # Try to list databases which will verify the authentication
            self.client.list_database_names()
            
            self.db = self.client[self.db_name]
            self.collection = self.db[self.collection_name]
            
        except Exception as e:
            logging.error(f"MongoDB authentication failed: {e}")
            raise

    def connect_atlas(self):
        try:
            if self.username and self.password and self.host != 'localhost':
                uri = f'mongodb+srv://{self.username}:{self.password}@{self.host}/?retryWrites=true&w=majority'
                self.client = MongoClient(uri, server_api=ServerApi('1'))
            else:
                logging.error('Failed to connect to Atlas cluster: %s', self.host)
        except Exception as e:
            logging.error(f'MongoDB authentication failed: {e}')
            raise

        # Send a ping to confirm a successful connection
        try:
            self.client.admin.command('ping')
            logging.info("Pinged your deployment. You successfully connected to Atlas cluster!")
        except Exception as e:
            print(e)

        # Once connected
        self.db = self.client[self.db_name]
        self.collection = self.db[self.collection_name]

    def set_terminal_attr(self, terminalName, attr, value):
        return self.collection.update_one({"name": terminalName}, {"$set": {attr: value}})
    
    def get_doc_by_field_value(self, attr, value):
        return self.collection.find_one({attr: {"$eq": value}})

    def remove_by_field_value(self, field_name: str, value: any) -> None:
        try:
            result = self.collection.delete_one({field_name: value})

            if result.deleted_count:
                logging.info(f"Successfully deleted {result.deleted_count} document(s) with {field_name} = {value}.")
            else:
                logging.warning(f"No documents found with {field_name} = {value}.")

        except PyMongoError as e:
            logging.error(f"An error occurred while trying to delete a document with {field_name} = {value}. Error: {str(e)}")

    def is_72hr_updated(self, terminal: Terminal) -> bool:

        document = self.collection.find_one({"name": {"$eq": terminal.name}})

        if not document:
            logging.warning('is_72hr_updated(): Terminal %s was not found in Mongo.', terminal.name)
            return True

        storedHash = document["pdfHash72Hour"]
        currentHash = terminal.pdfHash72Hour

        if currentHash != storedHash:
            logging.info('%s: 72 hour schedule updated.', terminal.name)
            return True
        else:
            return False
        
    def is_30day_updated(self, terminal: Terminal) -> bool:

        document = self.collection.find_one({"name": {"$eq": terminal.name}})

        if not document:
            logging.warning('is_30day_updated(): Terminal %s was not found in Mongo.', terminal.name)
            return True

        storedHash = document["pdfHash30Day"]
        currentHash = terminal.pdfHash30Day

        if currentHash != storedHash:
            logging.info('%s: 30 day schedule updated', terminal.name)
            return True
        else:
            return False
    
    def is_rollcall_updated(self, terminal: Terminal) -> bool:

        document = self.collection.find_one({"name": {"$eq": terminal.name}})

        if not document:
            logging.warning('is_rollcall_updated(): Terminal %s was not found in Mongo.', terminal.name)
            return True

        storedHash = document["pdfHashRollcall"]
        currentHash = terminal.pdfHashRollcall

        if currentHash != storedHash:
            logging.info('%s: rollcall updated.', terminal.name)
            return True
        else:
            return False
        
    def get_all_terminals(self) -> List[Terminal]:
        documents = self.collection.find()
        terminals = [Terminal.from_dict(document) for document in documents]
        return terminals

    def upsert_terminal(self, terminal: Terminal):
        # Try to find the terminal by its name
        existing_terminal = self.collection.find_one({'name': terminal.name})
        
        if not existing_terminal:
            # If terminal doesn't exist, insert it
            try:
                # Use the to_dict method to convert the Terminal object to its dict representation
                terminal_dict = terminal.to_dict()
                
                # Inserting the terminal into the collection
                self.collection.insert_one(terminal_dict)
                logging.info(f"Successfully inserted terminal {terminal.name} into the database.")
            except DuplicateKeyError:
                # Handle duplicate key error, if needed (for example if 'name' is a unique index)
                logging.warning(f"Terminal {terminal.name} already exists in the database. Consider updating it instead of inserting.")
            except Exception as e:
                # Handle other errors
                logging.error(f"Error inserting terminal {terminal.name}. Error: {str(e)}")
        else:
            # If terminal exists, you can consider updating it.
            # For simplicity, I'm just logging the existence. However, you can enhance this part to handle updates if needed.
            logging.info(f"Terminal with name {terminal.name} already exists in the database.")

