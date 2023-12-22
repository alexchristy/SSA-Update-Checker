from typing import Any, Dict, Optional, Type


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
        self.name = ""
        self.link = ""
        self.pdf_72hr_hash = ""
        self.pdf_30day_hash = ""
        self.pdf_rollcall_hash = ""
        self.group = ""
        self.page_pos: Optional[int] = None
        self.location = ""
        self.archive_dir = ""
        self.timezone = ""

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
            "timezone": self.timezone,
        }

    def __eq__(self: "Terminal", other: object) -> bool:
        """Compare the info of a Terminal object with another for equality.

        Info compared:
            name
            link
            group
            page_pos
            location
            timezone

        Parameters
        ----------
        other : Terminal
            The other Terminal object to compare with.

        Returns
        -------
        bool
            True if all member variables are equal, False otherwise.
        """
        if not isinstance(other, Terminal):
            # The other object is not a Terminal, so they are not equal
            return False

        return (
            self.name == other.name
            and self.link == other.link
            and self.group == other.group
            and self.page_pos == other.page_pos
            and self.location == other.location
            and self.timezone == other.timezone
        )

    def __hash__(self: "Terminal") -> int:
        """Return the hash of this Terminal object's info ONLY.

        Info hashed:
            name
            link
            group
            page_pos
            location
            timezone

        Returns
        -------
        int
            The hash of this Terminal object.
        """
        return hash(
            (
                self.name,
                self.link,
                self.group,
                self.page_pos,
                self.location,
                self.timezone,
            )
        )

    @classmethod
    def fully_equal(cls: Type["Terminal"], a: "Terminal", b: "Terminal") -> bool:
        """Compare all attributes of two Terminal objects for equality.

        Parameters
        ----------
        a : Terminal
            The first Terminal object to compare.
        b : Terminal
            The second Terminal object to compare.

        Returns
        -------
        bool
            True if all member variables are equal, False otherwise.
        """
        return (
            a.name == b.name
            and a.link == b.link
            and a.pdf_72hr_hash == b.pdf_72hr_hash
            and a.pdf_30day_hash == b.pdf_30day_hash
            and a.pdf_rollcall_hash == b.pdf_rollcall_hash
            and a.group == b.group
            and a.page_pos == b.page_pos
            and a.location == b.location
            and a.archive_dir == b.archive_dir
            and a.timezone == b.timezone
        )

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
        terminal.timezone = data.get("timezone", None)

        return terminal
