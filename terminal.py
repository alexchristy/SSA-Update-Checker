class Terminal:
    def __init__(self):
        self.name = "empty"
        self.link = "empty"
        self.pdfLink3Day = "empty"
        self.pdfName3Day = "empty"
        self.pdfHash3Day = "empty"
        self.group = "empty"
        self.pagePosition = -1
        self.location = "empty"
        self.chatIDs = []

    def print(self):
        print("Name: " + self.name)
        print("Page Link: " + self.link)
        print("PDF (3 day) Link: " + self.pdfLink3Day)
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
            'pdfLink3Day': self.pdfLink3Day,
            'pdfName3Day': self.pdfName3Day,
            'pdfHash3Day': self.pdfHash3Day,
            'group': self.group,
            'pagePosition': self.pagePosition,
            'location': self.location,
            'chatIDs': self.chatIDs
        }