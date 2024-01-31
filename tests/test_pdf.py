import http.server
import os
import socketserver
import threading
import unittest
from typing import Type

from pdf import Pdf


class PdfTestServer(socketserver.TCPServer):
    """Test server for PDF object creation tests."""

    allow_reuse_address = True


class TestPdfCreation(unittest.TestCase):
    """Test PDF object creation."""

    test_dir: str
    handler: Type[http.server.SimpleHTTPRequestHandler]
    httpd: PdfTestServer
    server_thread: threading.Thread
    port: int

    @classmethod
    def setUpClass(cls: Type["TestPdfCreation"]) -> None:
        """Set up test server and test directory with PDFs and PPTXs."""
        # Set up the HTTP server
        cls.test_dir = "tests/assets/TestPdfCreation"  # Path to your test directory
        os.chdir(cls.test_dir)

        cls.handler = http.server.SimpleHTTPRequestHandler
        cls.httpd = PdfTestServer(("localhost", 0), cls.handler)

        cls.server_thread = threading.Thread(target=cls.httpd.serve_forever)
        cls.server_thread.daemon = True
        cls.server_thread.start()

        cls.port = cls.httpd.server_address[1]

        # Set PDF_DIR env variable to empty string
        if "PDF_DIR" in os.environ:
            os.environ["PDF_DIR"] = ""

    @classmethod
    def tearDownClass(cls: Type["TestPdfCreation "]) -> None:
        """Shut down the test server."""
        cls.httpd.shutdown()
        cls.server_thread.join()

    def test_download_convert_pptx(self) -> None:
        """Test downloading and converting a PPTX to PDF."""
        test_pptx_url = f"http://localhost:{self.port}/altus_test_72hr_schedule.pptx"
        pdf = Pdf(link=test_pptx_url, populate=True)
        # Add assertions and checks here


# Add more test methods as needed

if __name__ == "__main__":
    unittest.main()
