import time
from pymongo import MongoClient
from pymongo.errors import WriteError
from terminal import Terminal

class MongoDB:

    def __init__(self, db_name, collection_name, host='localhost', port=27017, username=None, password=None):
        self.db_name = db_name
        self.collection_name = collection_name
        self.host = host
        self.port = port
        self.username = username
        self.password = password

    def connect(self):
        if self.username and self.password:
            uri = f"mongodb://{self.username}:{self.password}@{self.host}:{self.port}/{self.db_name}?authMechanism=DEFAULT&authSource=admin"
            self.client = MongoClient(uri)
        else:
            self.client = MongoClient(self.host, self.port)

        self.db = self.client[self.db_name]
        self.collection = self.db[self.collection_name]

    def addTerminal(self, terminal: Terminal):
        existing_document = self.collection.find_one({'name': terminal.name, 'link': terminal.link})

        if existing_document is None:
            # If no such document exists, insert the new Terminal object into the MongoDB collection
            result = self.collection.insert_one(terminal.to_dict())
            return result
        
        # Document exists check if pdfLink3Day has changed
        else:
            stored3DayLink = existing_document['pdfLink3Day']

            # PDF 3 Day link has changed
            if terminal.pdfLink3Day != stored3DayLink:
                self.collection.update_one({'name': terminal.name}, {'$set': {'pdfLink3Day': terminal.pdfLink3Day}})



    def isUserSubscribed(self, terminalName, userId):
        terminalDocument = self.collection.find_one({"name": terminalName})
        
        # If terminal does not exist
        if terminalDocument is None:
            return False

        # If userId is in the chatIDs list
        if userId in terminalDocument["chatIDs"]:
            return True
        else:
            return False

    def addSubscription(self, terminalName, chatID):
        # Check if document exists
        existingDocument = self.collection.find_one({"name": terminalName})
        if existingDocument:
            try:
                result = self.collection.update_one(
                    {"name": terminalName},
                    {"$push": {"chatIDs": chatID}}
                )
                if result.modified_count > 0:
                    print("New subscription added")
                else:
                    print("No matching document found for the provided terminalID")

            except WriteError as e:
                print("Error occurred while inserting subscription:", str(e))
        else:
            print(f"No document found with terminalID: {terminalName}")

    def removeSubscription(self, terminalName, chatID):
        # Check if document exists
        existingDocument = self.collection.find_one({"name": terminalName})
        if existingDocument:
            try:
                result = self.collection.update_one(
                    {"name": terminalName},
                    {"$pull": {"chatIDs": chatID}}
                )
                if result.modified_count > 0:
                    print("Subscription removed")
                else:
                    print("No matching document found for the provided terminalID or chatID is not in chatIDs")

            except WriteError as e:
                print("Error occurred while removing subscription:", str(e))
        else:
            print(f"No document found with terminalID: {terminalName}")
    
    def getTerminalsWithPDFLink(self):
        return self.collection.find({"pdfLink3Day": {"$ne": "empty"}})
    
    def setPDFHash3Day(self, terminalName, hash):
        return self.collection.update_one({'name': terminalName}, {'$set': {'pdfHash3Day': hash}})
    
    def setPDFName(self, terminalName, pdfName):
        return self.collection.update_one({"name": terminalName}, {"$set": {"pdfName3Day": pdfName}})

    def getTerminalByName(self, terminalName):
        document = self.collection.find_one({"name": terminalName})
        
        if document is not None:
            # Convert the document into a Terminal object
            terminal = Terminal.from_dict(document)
            return terminal
        
        return None

    def get3DayPDFByFileName(self, pdfName3Day):
        return self.collection.find_one({"pdfName3Day": pdfName3Day})
    
    def getAllTerminals(self):
        documents = self.collection.find()
        return list(documents)
    
    def getSubscribedTerminals(self, chatID):
        # find all documents where chatIDs contains chatID
        documents = self.collection.find({"chatIDs": chatID})

        # convert to list
        documents = list(documents)

        # create a list to hold the terminal names
        subscribed_terminals = []

        # iterate over documents
        for doc in documents:
            # append the terminal name to the list
            subscribed_terminals.append(doc['name'])

        return subscribed_terminals