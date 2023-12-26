from typing import Type


class PdfPage:
    """Class that represents a page in a PDF file."""

    def __init__(self: "PdfPage", page_number: int) -> None:
        """Initialize a PdfPage object."""
        self.page_number = page_number
        self._degrees_of_rotation = -1
        self._width = -1
        self._height = -1
        self._num_words = 0
        self._num_chars = 0

    @property
    def degrees_of_rotation(self: "PdfPage") -> int:
        """Get the degrees of rotation for the page."""
        return self._degrees_of_rotation

    @degrees_of_rotation.setter
    def degrees_of_rotation(self: "PdfPage", value: int) -> None:
        if value >= 0:
            self._degrees_of_rotation = value

    @property
    def width(self: "PdfPage") -> int:
        """Get the width of the page in points (1/72 of an inch)."""
        return self._width

    @width.setter
    def width(self: "PdfPage", value: int) -> None:
        if value >= 0:
            self._width = value

    @property
    def height(self: "PdfPage") -> int:
        """Get the height of the page in points (1/72 of an inch)."""
        return self._height

    @height.setter
    def height(self: "PdfPage", value: int) -> None:
        if value >= 0:
            self._height = value

    @property
    def num_words(self: "PdfPage") -> int:
        """Get the number of words on the page."""
        return self._num_words

    @num_words.setter
    def num_words(self: "PdfPage", value: int) -> None:
        if value >= 0:
            self._num_words = value

    @property
    def num_chars(self: "PdfPage") -> int:
        """Get the number of characters on the page."""
        return self._num_chars

    @num_chars.setter
    def num_chars(self: "PdfPage", value: int) -> None:
        if value >= 0:
            self._num_chars = value

    def to_dict(self: "PdfPage") -> dict:
        """Convert PdfPage attributes to a dictionary with camelCase keys."""
        return {
            "pageNumber": self.page_number,
            "degreesOfRotation": self.degrees_of_rotation,
            "width": self.width,
            "height": self.height,
            "numWords": self.num_words,
            "numChars": self.num_chars,
        }

    @classmethod
    def from_dict(cls: Type["PdfPage"], data: dict) -> "PdfPage":
        """Create a PdfPage instance from a dictionary with camelCase keys."""
        page = cls(data["pageNumber"])
        page.degrees_of_rotation = data.get("degreesOfRotation", -1)
        page.width = data.get("width", -1)
        page.height = data.get("height", -1)
        page.num_words = data.get("numWords", 0)
        page.num_chars = data.get("numChars", 0)
        return page
