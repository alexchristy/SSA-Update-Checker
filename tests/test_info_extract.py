import unittest

from info_extract import InfoExtractor


class TestInfoExtractor(unittest.TestCase):
    """Class for testing InfoExtractor class."""

    def test_bwi_042624(self: "TestInfoExtractor") -> None:
        """Test the correct informaton is extracted from the bwi_042624 terminal html content.

        The file is located at `tests/assets/TestInfoExtractor/bwi_042624_terminal_page.html`

        """
        with open(
            "tests/assets/TestInfoExtractor/bwi_042624_terminal_page.html"
        ) as file:
            content = file.read()

        info_extractor = InfoExtractor()

        info, hash_to_test = info_extractor.get_gpt_extracted_info(content)

        # Phone numbers that should be extracted
        # Note that there are duplicates on purpose as the same
        # number might have multiple uses
        phone_numbers = [
            "6092538825",
            "5688825",
            "6092538825",
            "5688825",
            "6092538823",
            "5688823",
            "6092538815",
        ]

        # Emails that should be extracted
        emails = ["305APS.DET1.BWIPAX@US.AF.MIL"]

        # Hours of operation
        hours_of_operation = [
            "0800-1600",
        ]

        # Address
        address = ["7050 Friendship Rd, Baltimore, MD 21240"]

        # Check if the extracted information is correct
        correct_hash = (
            "542dea849807f1a4ab1c189f400a62330455d0b803bf69caf50232f565dac30f"
        )
        self.assertEqual(hash_to_test, correct_hash)

        # Extract all phone numbers from the info
        extracted_phone_numbers = [
            phone for item in info["phone_numbers"] for phone in item["value"]
        ]
        self.assertCountEqual(extracted_phone_numbers, phone_numbers)

        # Extract all emails from the info
        extracted_emails = [entry["value"] for entry in info["emails"]]
        self.assertCountEqual(extracted_emails, emails)

        # Extract all hours of operation from the info
        extracted_hours = [entry["value"] for entry in info["hours"]]
        self.assertCountEqual(extracted_hours, hours_of_operation)

        # Extract the address from the info
        extracted_address = [entry["value"] for entry in info["addresses"]]
        self.assertCountEqual(extracted_address, address)

    def test_incirlik_042624(self: "TestInfoExtractor") -> None:
        """Test the correct informaton is extracted from the incirlik_042624 terminal html content.

        The file is located at `tests/assets/TestInfoExtractor/incirlik_042624_terminal_page.html`

        """
        with open(
            "tests/assets/TestInfoExtractor/incirlik_042624_terminal_page.html"
        ) as file:
            content = file.read()

        info_extractor = InfoExtractor()

        info, hash_to_test = info_extractor.get_gpt_extracted_info(content)

        # Phone numbers that should be extracted
        # Note that there are duplicates on purpose as the same
        # number might have multiple uses
        phone_numbers = [
            "011903223166111",
            "3146766111",
        ]

        # Emails that should be extracted
        emails = [
            "728AMS.TRP.SpaceASignup@us.af.mil",
        ]

        # Hours of operation
        hours = [
            "0600-1800",
        ]

        # Address
        addresses = [
            "Building 500, Incirlik AB, Turkey 09824",
        ]

        # Check if the extracted information is correct
        correct_hash = (
            "3129db4bd5020ed5dd64b2499eb45882ef879a8a088a3a7292ae3df61c2f42f1"
        )
        self.assertEqual(hash_to_test, correct_hash)

        # Extract all phone numbers from the info
        extracted_phone_numbers = [
            phone for item in info["phone_numbers"] for phone in item["value"]
        ]
        self.assertCountEqual(extracted_phone_numbers, phone_numbers)

        # Extract all emails from the info
        extracted_emails = [entry["value"] for entry in info["emails"]]
        self.assertCountEqual(extracted_emails, emails)

        # Extract all hours of operation from the info
        extracted_hours = [entry["value"] for entry in info["hours"]]
        self.assertCountEqual(extracted_hours, hours)

        # Extract the address from the info
        extracted_address = [entry["value"] for entry in info["addresses"]]
        self.assertCountEqual(extracted_address, addresses)

    def test_ramstein_042624(self: "TestInfoExtractor") -> None:
        """Test the correct informaton is extracted from the ramstein_042624 terminal html content.

        The file is located at `tests/assets/TestInfoExtractor/ramstein_042624_terminal_page.html`

        """
        with open(
            "tests/assets/TestInfoExtractor/ramstein_042624_terminal_page.html"
        ) as file:
            content = file.read()

        info_extractor = InfoExtractor()

        info, hash_to_test = info_extractor.get_gpt_extracted_info(content)

        # Phone numbers that should be extracted
        # Note that there are duplicates on purpose as the same
        # number might have multiple uses
        phone_numbers = [
            "496371464440",
            "3144794440",
            "496371464441",
            "496371464442",
            "496371464754",
            "3144794441",
            "3144794442",
            "3144794754",
        ]

        # Emails that should be extracted
        emails = [
            "721aps.ramstein.spaceasignup@us.af.mil",
        ]

        # Hours of operation
        hours = ["0600-2200", "0400-2200"]

        # Address
        addresses = [
            "Bldg 3333, Ramstein-Miesenbach, Germany 66877",
        ]

        # Check if the extracted information is correct
        correct_hash = (
            "e736e4c757083e6e7f7455b727c953a04ba51c82b098ef3fa58860d693a4321a"
        )
        self.assertEqual(hash_to_test, correct_hash)

        # Extract all phone numbers from the info
        extracted_phone_numbers = [
            phone for item in info["phone_numbers"] for phone in item["value"]
        ]
        self.assertCountEqual(extracted_phone_numbers, phone_numbers)

        # Extract all emails from the info
        extracted_emails = [entry["value"] for entry in info["emails"]]
        self.assertCountEqual(extracted_emails, emails)

        # Extract all hours of operation from the info
        extracted_hours = [entry["value"] for entry in info["hours"]]
        self.assertCountEqual(extracted_hours, hours)

        # Extract the address from the info
        extracted_address = [entry["value"] for entry in info["addresses"]]
        self.assertCountEqual(extracted_address, addresses)

    def test_al_udeid_042624(self: "TestInfoExtractor") -> None:
        """Test the correct informaton is extracted from the al_udeid_042624 terminal html content.

        The file is located at `tests/assets/TestInfoExtractor/al_udeid_042624_terminal_page.html`

        """
        with open(
            "tests/assets/TestInfoExtractor/al_udeid_042624_terminal_page.html"
        ) as file:
            content = file.read()

        info_extractor = InfoExtractor()

        info, hash_to_test = info_extractor.get_gpt_extracted_info(content)

        # Phone numbers that should be extracted
        # Note that there are duplicates on purpose as the same
        # number might have multiple uses
        phone_numbers = [
            "3184555285",
        ]

        # Emails that should be extracted
        emails = [
            "8eams.paxsignup@us.af.mil",
        ]

        # Hours of operation
        hours = [
            "0000-2359",
        ]

        # Address
        addresses = [
            "BLDG 3976",
        ]

        # Check if the extracted information is correct
        correct_hash = (
            "d1e3054c74c76eda84a3e9d80c56e654b3c0235cb0a2b4cdf22e48d4a38939b6"
        )
        self.assertEqual(hash_to_test, correct_hash)

        # Extract all phone numbers from the info
        extracted_phone_numbers = [
            phone for item in info["phone_numbers"] for phone in item["value"]
        ]
        self.assertCountEqual(extracted_phone_numbers, phone_numbers)

        # Extract all emails from the info
        extracted_emails = [entry["value"] for entry in info["emails"]]
        self.assertCountEqual(extracted_emails, emails)

        # Extract all hours of operation from the info
        extracted_hours = [entry["value"] for entry in info["hours"]]
        self.assertCountEqual(extracted_hours, hours)

        # Extract the address from the info
        extracted_address = [entry["value"] for entry in info["addresses"]]
        self.assertCountEqual(extracted_address, addresses)

    def test_iwakuni_042624(self: "TestInfoExtractor") -> None:
        """Test the correct informaton is extracted from the iwakuni_042624 terminal html content.

        The file is located at `tests/assets/TestInfoExtractor/iwakuni_042624_terminal_page.html`

        """
        with open(
            "tests/assets/TestInfoExtractor/iwakuni_042624_terminal_page.html"
        ) as file:
            content = file.read()

        info_extractor = InfoExtractor()

        info, hash_to_test = info_extractor.get_gpt_extracted_info(content)

        # Phone numbers that should be extracted
        # Note that there are duplicates on purpose as the same
        # number might have multiple uses
        phone_numbers = [
            "01181827795509",
            "0827795509",
            "01181827795507",
            "0827795507",
            "01181827795262",
            "0827795262",
            "2535509",
            "2535507",
            "2535262",
        ]

        # Emails that should be extracted
        emails = [
            "M_IWKN_SMBIwakuniSpa@usmc.mil",
        ]

        # Hours of operation
        hours = [
            "0730-1630",
            "0630-1630",
            "0630-1830",
            "0730-1400",
        ]

        # Address
        addresses = [
            "BLDG  727, MARINE CORPS AIR STATION    IWAKUNI, JAPAN 96310",
        ]

        # Check if the extracted information is correct
        correct_hash = (
            "4cf58ad8bf7038a499d5a18ed582aeb2828bebb69a9091a3ddaee541e59ac706"
        )
        self.assertEqual(hash_to_test, correct_hash)

        # Extract all phone numbers from the info
        extracted_phone_numbers = [
            phone for item in info["phone_numbers"] for phone in item["value"]
        ]
        self.assertCountEqual(extracted_phone_numbers, phone_numbers)

        # Extract all emails from the info
        extracted_emails = [entry["value"] for entry in info["emails"]]
        self.assertCountEqual(extracted_emails, emails)

        # Extract all hours of operation from the info
        extracted_hours = [entry["value"] for entry in info["hours"]]
        self.assertCountEqual(extracted_hours, hours)

        # Extract the address from the info
        extracted_address = [entry["value"] for entry in info["addresses"]]
        self.assertCountEqual(extracted_address, addresses)
