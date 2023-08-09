class Terminal:
    def __init__(self):
        self.name = "empty"
        self.link = "empty"

        self.pdfLink72Hour = "empty"
        self.pdfName72Hour = "empty"
        self.pdfHash72Hour = "empty"
        self.is72HourUpdated = False

        self.pdfLink30Day = "empty"
        self.pdfName30Day = "empty"
        self.pdfHash30Day = "empty"
        self.is30DayUpdated = False

        self.pdfLinkRollcall = "empty"
        self.pdfNameRollcall = "empty"
        self.pdfHashRollcall = "empty"
        self.isRollcallUpdated = False

        self.group = "empty"
        self.pagePosition = "empty"
        self.location = "empty"
        self.chatIDs = []

    def print(self):
        print("Name: " + self.name)
        print("Page Link: " + self.link)
        print("PDF (3 day) Link: " + self.pdfLink72Hour)
        print("\n\n")

    @classmethod
    def from_dict(cls, data: dict) -> 'Terminal':
        instance = cls()
        for key, value in data.items():
            if hasattr(instance, key):
                setattr(instance, key, value)
        return instance

    def to_dict(self):
        return {
            'name': self.name,
            'link': self.link,

            'pdfLink72Hour': self.pdfLink72Hour,
            'pdfName72Hour': self.pdfName72Hour,
            'pdfHash72Hour': self.pdfHash72Hour,
            'is72HourUpdated': self.is72HourUpdated,

            'pdfLink30Day': self.pdfLink30Day,
            'pdfName30Day': self.pdfName30Day,
            'pdfHash30Day': self.pdfHash30Day,
            'is30DayUpdated': self.is30DayUpdated,

            'pdfLinkRollcall': self.pdfLinkRollcall,
            'pdfNameRollcall': self.pdfNameRollcall,
            'pdfHashRollcall': self.pdfHashRollcall,
            'isRollcallUpdated': self.isRollcallUpdated,

            'group': self.group,
            'pagePosition': self.pagePosition,
            'location': self.location,
            'chatIDs': self.chatIDs
        }