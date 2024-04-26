import hashlib


def create_sha256_hash(content: str) -> str:
    """Create a SHA256 hash of the given content.

    Args:
    ----
        content (str): The content to hash.

    Returns:
    -------
        str: The SHA256 hash of the content.

    """
    # Create hash object
    hash_object = hashlib.sha256()
    # Update hash object with the content
    hash_object.update(content.encode("utf-8"))
    # Get hexadecimal representation of the hash
    return hash_object.hexdigest()
