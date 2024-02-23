from typing import Optional
import os
import logging
from scraper_utils import get_with_retry
from bs4 import BeautifulSoup  # type: ignore
from s3_bucket import S3Bucket
import tempfile


def _get_terminal_image_url(url: str) -> Optional[str]:
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


def save_terminal_image(url: str, s3_bucket: S3Bucket) -> Optional[str]:
    image_url = _get_terminal_image_url(url)
    if image_url:
        image_name = os.path.basename(image_url)
        s3_key = f"terminal_images/{image_name}"

        # Temporary workaround: download to a temporary file and then upload
        with tempfile.NamedTemporaryFile() as temp_file:
            response = get_with_retry(image_url)
            if response and response.status_code == 200:
                temp_file.write(response.content)
                temp_file.flush()
                s3_bucket.upload_to_s3(temp_file.name, s3_key)
                return s3_bucket.get_public_url(s3_key)
            else:
                logging.error("Failed to download the image.")
    return None
