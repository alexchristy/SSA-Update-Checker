import os
import pickle
import sys

from dotenv import load_dotenv

current_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.append(current_dir + "/../../../")

from pdf import Pdf  # noqa: E402 (Relative import)
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


# Code below if for generating PDFs for sorting metadata tests
# os.environ["PDF_DIR"] = "./pdfs"

# pdf1 = Pdf(
#     "https://www.amc.af.mil/Portals/12/AMC%20Tvl%20Pg/Passenger%20Terminals/AMC%20CONUS%20Terminals/BWI%20Passenger%20Terminal/BWI_72HR_25-27DEC23.pdf?ver=DYWTERnPIKRJe4h30YPYUw%3d%3d",
#     populate=True,
# )
# pdf2 = Pdf(
#     "https://www.amc.af.mil/Portals/12/AMC%20Tvl%20Pg/Passenger%20Terminals/AMC%20CONUS%20Terminals/Dover%20AFB%20Passenger%20Terminal/DOV_72HR_24DEC23.pdf?ver=GYkHLuiBgY3Em80VpzWIQQ%3d%3d",
#     populate=True,
# )
# pdf3 = Pdf(
#     "https://www.amc.af.mil/Portals/12/AMC%20Tvl%20Pg/Passenger%20Terminals/AMC%20CONUS%20Terminals/MacDill%20AFB%20Passenger%20Terminal/MACDILL_72HR_21_DEC%2023.pdf?ver=pWbZxL9T8SAfZebKIQuGIg%3d%3d",
#     populate=True,
# )
# pdf4 = Pdf(
#     "https://www.amc.af.mil/Portals/12/AMC%20Tvl%20Pg/Passenger%20Terminals/AMC%20CONUS%20Terminals/NAS%20Jacksonville%20Passenger%20Terminal/72%20HOUR%20SCHEDULE/72%20HOUR%20SCHEDULE%2002%20NOV%202023.pdf?ver=tjdchF3_Kp-YceNvEay_wg%3d%3d",
#     populate=True,
# )
# pdf5 = Pdf(
#     "https://www.amc.af.mil/Portals/12/AMC%20Tvl%20Pg/Passenger%20Terminals/AMC%20CONUS%20Terminals/Travis%20AFB%20Passenger%20Terminal/Travis_72HR_25DEC23.pdf?ver=mzU1-k6Ze3dRiFc7MPczlg%3d%3d&timestamp=1703490047464",
#     populate=True,
# )

# pdf_list = [pdf1, pdf5, pdf2, pdf4, pdf3]

# for i, pdf in enumerate(pdf_list):
#     new_path = f"tests/assets/TestSortPdfsByCreationTime/test_sort_pdfs_by_creation_time/pdf{i+1}"

#     new_pdf_path = f"{new_path}.pdf"
#     print(os.getcwd())
#     os.rename("./pdfs/" + pdf.cloud_path, new_pdf_path)
#     pdf.cloud_path = new_pdf_path

#     with open(
#         f"{new_path}.pkl",
#         "wb",
#     ) as f:
#         pickle.dump(pdf, f)
