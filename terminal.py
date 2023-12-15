from typing import Any, Dict, Type


class Terminal:
    """Class that represents a Space A terminal."""

    def __init__(self: "Terminal") -> None:
        """Initialize a Terminal object.

        Attributes
        ----------
            name: The name of the terminal.
            link: The URL of the terminal's page.
            pdf_72hr_hash: The hash of the 72 hour PDF.
            pdf_30day_hash: The hash of the 30 day PDF.
            pdf_rollcall_hash: The hash of the roll call PDF.
            group: The group the terminal belongs to.
            page_pos: The position of the terminal on the page.
            location: The location of the terminal.
            archive_dir: The directory where the terminal's PDFs are archived.

        Sets all attributes to None.
        """
        self.name = None
        self.link = None
        self.pdf_72hr_hash = None
        self.pdf_30day_hash = None
        self.pdf_rollcall_hash = None
        self.group = None
        self.page_pos = None
        self.location = None
        self.archive_dir = None
        self.updating_72hr = False
        self.updating_30day = False
        self.updating_rollcall = False

    def to_dict(self: "Terminal") -> Dict[str, Any]:
        """Convert this Terminal object to a dictionary, suitable for storing in Firestore or another database.

        Returns
        -------
            A dictionary representation of this Terminal object
        """
        return {
            "name": self.name,
            "link": self.link,
            "pdf72HourHash": self.pdf_72hr_hash,
            "pdf30DayHash": self.pdf_30day_hash,
            "pdfRollcallHash": self.pdf_rollcall_hash,
            "group": self.group,
            "pagePosition": self.page_pos,
            "location": self.location,
            "archiveDir": self.archive_dir,
            "updating72Hour": self.updating_72hr,
            "updating30Day": self.updating_30day,
            "updatingRollcall": self.updating_rollcall,
        }

    @classmethod
    def from_dict(cls: Type["Terminal"], data: Dict[str, Any]) -> "Terminal":
        """Create a Terminal object from a dictionary (e.g., a Firestore document)."""
        terminal = cls()
        terminal.name = data.get("name", None)
        terminal.link = data.get("link", "")
        terminal.pdf_72hr_hash = data.get("pdf72HourHash", None)
        terminal.pdf_30day_hash = data.get("pdf30DayHash", None)
        terminal.pdf_rollcall_hash = data.get("pdfRollcallHash", None)
        terminal.group = data.get("group", None)
        terminal.page_pos = data.get("pagePosition", None)
        terminal.location = data.get("location", None)
        terminal.archive_dir = data.get("archiveDir", None)
        terminal.updating_72hr = data.get("updating72Hour", False)
        terminal.updating_30day = data.get("updating30Day", False)
        terminal.updating_rollcall = data.get("updatingRollcall", False)

        return terminal
