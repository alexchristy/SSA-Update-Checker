from typing import Optional
import os
import urllib.request
import logging
from scraper_utils import get_with_retry
from bs4 import BeautifulSoup  # type: ignore


def get_terminal_image_url(url: str) -> Optional[str]:
    """
    Retrieves the image URL from a webpage given its URL.

    Args:
        url (str): The URL of the webpage.

    Returns:
        Optional[str]: The image URL or None if the image could not be found.
    """
    try:
        response = get_with_retry(url)
        if not response:
            logging.info("Failed to get a response from %s", url)
            return None

        soup = BeautifulSoup(response.text, "html.parser")
        img_tag = (
            soup.find("figure", class_="hero banner")
            .find("picture", class_="fixed-aspect")
            .find("img")
        )

        if img_tag and "src" in img_tag.attrs:
            return img_tag["src"]
        else:
            logging.info("Image tag with 'src' attribute not found.")
            return None
    except Exception as e:
        logging.error(
            "An error occurred while retrieving the image URL (%s): %s", url, e
        )
        return None


def download_image(image_url: str, save_path: str) -> bool:
    """
    Downloads an image from the specified URL and saves it to the given path.

    Args:
        image_url (str): The URL of the image to download.
        save_path (str): The path to save the downloaded image.

    Returns:
        bool: True if the image was successfully downloaded, False otherwise.
    """
    try:
        urllib.request.urlretrieve(image_url, save_path)
        logging.info(f"Image successfully downloaded: {save_path}")
        return True
    except Exception as e:
        logging.error(
            "An error occurred while downloading the image (%s): %s", image_url, e
        )
        return False


def download_terminal_image(url: str) -> Optional[str]:
    """
    Downloads an image from a webpage given its URL. This function uses `get_image_url_from_html`
    to retrieve the image URL and `download_image` to download the image.

    Args:
        url (str): The URL of the webpage to download the image from.

    Returns:
        Optional[str]: The full path to the downloaded image or None if the image could not be downloaded.
    """
    image_url = get_terminal_image_url(url)
    if image_url:
        image_name = os.path.basename(image_url)
        full_path = os.path.join(os.getcwd(), image_name)
        if download_image(image_url, full_path):
            return full_path
    return None
