class Terminal:
    def __init__(self):
        self.name = None
        self.link = None
        self.pdf72HourHash = None
        self.pdf30DayHash = None
        self.pdfRollcallHash = None
        self.group = None
        self.pagePosition = None
        self.location = None
        self.archiveDir = None

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
            'archiveDir': self.archiveDir
        }

    @classmethod
    def from_dict(cls, data):
        """
        Create a Terminal object from a dictionary (e.g., a Firestore document)
        """
        terminal = cls()
        terminal.name = data.get('name', None)
        terminal.link = data.get('link', "")
        terminal.pdf72HourHash = data.get('pdf72HourHash', None)
        terminal.pdf30DayHash = data.get('pdf30DayHash', None)
        terminal.pdfRollcallHash = data.get('pdfRollcallHash', None)
        terminal.group = data.get('group', None)
        terminal.pagePosition = data.get('pagePosition', None)
        terminal.location = data.get('location', None)
        terminal.archiveDir = data.get('archiveDir', None)
        
        return terminal
