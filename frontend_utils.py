import os
import requests
from bs4 import BeautifulSoup  # type: ignore
import urllib.request


def download_image_from_html(url):
    try:
        # Send an HTTP GET request to the URL
        response = requests.get(url)
        response.raise_for_status()  # Raise an exception for HTTP errors

        # Parse the HTML content
        soup = BeautifulSoup(response.text, "html.parser")

        # Find the <img> tag inside <figure class="hero banner"> and <picture class="fixed-aspect">
        img_tag = (
            soup.find("figure", class_="hero banner")
            .find("picture", class_="fixed-aspect")
            .find("img")
        )

        if img_tag and "src" in img_tag.attrs:
            image_url = img_tag["src"]
            # Get the image name from the image URL
            image_name = os.path.basename(image_url)

            # Download the image
            urllib.request.urlretrieve(image_url, image_name)
            print(f"Image successfully downloaded: {image_name}")
        else:
            print("Image tag with 'src' attribute not found.")
    except requests.exceptions.RequestException as e:
        print(f"Request failed: {e}")
    except Exception as e:
        print(f"An error occurred: {e}")
