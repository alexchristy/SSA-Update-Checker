import os
import pickle
import sys

from dotenv import load_dotenv

current_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.append(current_dir + "/../../../")

from scraper import get_terminal_pdfs  # noqa: E402 (Relative import)
from terminal import Terminal  # noqa: E402 (Relative import)


def create_pdf_pkls(terminal_link: str, path: str) -> bool:
    """Create PDF pickle files for testing the sorting algorithm.

    Args:
    ----
        terminal_link (str): The link to the terminal.
        path (str): The path to the directory to save the PDF pickle files.

    Returns:
    -------
        bool: True if successful, False otherwise.
    """
    load_dotenv()

    terminal = Terminal()
    terminal.link = terminal_link

    pdfs = get_terminal_pdfs(terminal)

    for pdf in pdfs:
        with open(f"{path}/{pdf.filename}.pkl", "wb") as f:
            pickle.dump(pdf, f)

    return True


terminal_link = "https://www.amc.af.mil/AMC-Travel-Site/Terminals/CONUS-Terminals/Baltimore-Washington-International-Airport-Passenger-Terminal/"
path = "tests/assets/TestSortTerminalPdfs/test_bwi"

create_pdf_pkls(terminal_link, path)
