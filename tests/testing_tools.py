import pickle

import requests
from bs4 import BeautifulSoup  # type: ignore


def remove_pdf_links(html_content: str) -> str:
    """Remove <a> tags that link to .pdf files from HTML content.

    Args:
    ----
        html_content (str): The HTML content as a string.

    Returns:
    -------
        str: The modified HTML content without <a> tags linking to .pdf files.

    """
    soup = BeautifulSoup(html_content, "html.parser")
    a_tags = soup.find_all("a", href=lambda href: href and ".pdf" in href)
    for tag in a_tags:
        tag.decompose()  # Remove the tag from the soup
    return str(soup)


def pickle_response_attributes(
    url: str, filename: str, remove_pdfs: bool = False
) -> None:
    """Fetch a URL and pickle selected attributes of the response, with an option to remove PDF links.

    Args:
    ----
        url (str): The URL to fetch.
        filename (str): The filename to save the pickled attributes to.
        remove_pdfs (bool, optional): Flag to remove <a> tags linking to .pdf files. Defaults to False.

    Raises:
    ------
        ValueError: If `url` or `filename` are not provided.

    """
    if not url:
        msg = "URL is required"
        raise ValueError(msg)
    if not filename:
        msg = "Filename is required"
        raise ValueError(msg)

    response = requests.get(url, timeout=5)
    content = response.content
    if remove_pdfs:
        # Convert bytes to string for BeautifulSoup, assuming UTF-8 encoding
        content_str = content.decode("utf-8")
        cleaned_content = remove_pdf_links(content_str)
        content = cleaned_content.encode("utf-8")  # Convert back to bytes

    attributes_to_save = {
        "status_code": response.status_code,
        "content": content,  # Use modified content if remove_pdfs is True
        "headers": dict(response.headers),
    }

    with open(filename, "wb") as f:
        pickle.dump(attributes_to_save, f)


pickle_response_attributes(
    url="https://www.amc.af.mil/AMC-Travel-Site/Terminals/CONUS-Terminals/Joint-Base-Charleston-Passenger-Terminal/",
    filename="charleston_page_02-17-24_NO_PDFS.pkl",
    remove_pdfs=True,
)
