import pickle

import firebase_admin  # type: ignore
from firebase_admin import credentials, firestore  # type: ignore

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


# Adjusted to check for existing Firebase app instances
def initialize_firestore(service_account_key, name="default"):
    # Read service account key file
    cred = credentials.Certificate(service_account_key)

    # Check if app already exists
    app = firebase_admin.initialize_app(cred, name=name)

    db = firestore.client(app=app)
    return db


# Adjusted clone_firestore function
def clone_firestore(source_key, target_key):
    # Initialize source and target Firestore databases with unique names
    source_db = initialize_firestore(source_key, name="source")
    target_db = initialize_firestore(target_key, name="target")

    collections = source_db.collections()
    for collection in collections:
        docs = collection.stream()
        for doc in docs:
            data = doc.to_dict()
            target_db.collection(collection.id).document(doc.id).set(data)
            print(f"Cloned document {doc.id} in collection {collection.id}")


# Example usage
clone_firestore("./creds.json", "./testcreds.json")
