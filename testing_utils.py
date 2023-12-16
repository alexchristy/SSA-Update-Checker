import pickle

import requests

from scraper_utils import get_with_retry


def serialize_response(response: requests.Response) -> bytes:
    """Serialize a requests.Response object."""
    serialized_data = {
        "status_code": response.status_code,
        "headers": dict(response.headers),
        "content": response.content,
        "url": response.url,
        "reason": response.reason,
        # Add other fields if necessary
    }
    return pickle.dumps(serialized_data)


def serialize_page_as_response(url: str, file_path: str) -> bool:
    """Serialize a page as a requests.Response object."""
    if not url:
        return False

    if not file_path:
        return False

    response = get_with_retry(url)

    sucessful_response = 200
    if response is None or response.status_code != sucessful_response:
        return False

    serialized_response = serialize_response(response)

    with open(file_path, "wb") as f:
        f.write(serialized_response)

    return True
