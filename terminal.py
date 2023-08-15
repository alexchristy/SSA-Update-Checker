class Terminal:
    def __init__(self):
        self.name = ""
        self.link = ""
        self.pdf72HourHash = ""
        self.pdf30DayHash = ""
        self.pdfRollcallHash = ""
        self.group = ""
        self.pagePosition = ""
        self.location = ""
        self.chatIDs = []
        self.archiveDir = ""

    def to_dict(self):
        """
        Convert this Terminal object to a dictionary, suitable for storing in Firestore or another database
        """
        return {
            'name': self.name,
            'link': self.link,
            'pdf72HourHash': self.pdf72HourHash,
            'pdf30DayHash': self.pdf30DayHash,
            'pdfRollcallHash': self.pdfRollcallHash,
            'group': self.group,
            'pagePosition': self.pagePosition,
            'location': self.location,
            'chatIDs': self.chatIDs,
            'archiveDir': self.archiveDir
        }

    @classmethod
    def from_dict(cls, data):
        """
        Create a Terminal object from a dictionary (e.g., a Firestore document)
        """
        terminal = cls()
        terminal.name = data.get('name', "")
        terminal.link = data.get('link', "")
        terminal.pdf72HourHash = data.get('pdf72HourHash', "")
        terminal.pdf30DayHash = data.get('pdf30DayHash', "")
        terminal.pdfRollcallHash = data.get('pdfRollcallHash', "")
        terminal.group = data.get('group', "")
        terminal.pagePosition = data.get('pagePosition', "")
        terminal.location = data.get('location', "")
        terminal.chatIDs = data.get('chatIDs', [])
        terminal.archiveDir = data.get('archiveDir', "")
        
        return terminal
