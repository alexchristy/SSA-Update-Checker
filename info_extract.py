import json
import logging
import os
from typing import Dict, List, Optional, Tuple, TypedDict

from bs4 import BeautifulSoup  # type: ignore
from openai import OpenAI  # type: ignore

from utils import create_sha256_hash


class PhoneDetail(TypedDict):
    """Type definition for a phone number detail.

    This is a dictionary containing the phone number, description, and notes.
    """

    value: List[str]
    description: str
    notes: str


class InfoExtractor:
    """Class to extract information from HTML content.

    It extracts DSN phone numbers, commercial phone numbers, fax DSN and commercial numbers,
    emails, addresses, hours of operations, and related notes that a traveler would find helpful.
    """

    def __init__(self: "InfoExtractor") -> None:
        """Initialize the InfoExtractor class.

        This retrieves API keys from the environment for OpenAI.
        """
        gpt_key = os.getenv("OPENAI_API_KEY", None)

        if not gpt_key:
            msg = "OPENAI_API_KEY environment variable not set."
            raise EnvironmentError(msg)

        self.gpt_client = OpenAI(api_key=gpt_key)

    def get_gpt_extracted_info(self: "InfoExtractor", content: str) -> Tuple[dict, str]:
        """Extract information from HTML content using ChatGPT.

        Args:
        ----
            content (str): The entire HTML content from the Terminal page.

        Returns:
        -------
            dict: A dictionary containing extracted information.
            str: The hash of the content for tracking updates.

        """
        # No log since it's already logged in _download_html_info_content
        if not content:
            logging.error("Cannot extract information from empty content.")
            return {}, ""

        # Extract the Contact Information div content
        div_content = self._extract_div_content(content)

        if not div_content:
            logging.error("Failed to extract the Contact Information div content.")
            return {}, ""

        # Hash the content for tracking updates
        div_content_hash = create_sha256_hash(div_content)

        # Extract phone numbers
        phone_numbers = self._extract_phone_numbers(div_content)

        if not phone_numbers:
            logging.error("Failed to extract phone numbers.")

        combined_phones = self._combine_phone_numbers(phone_numbers)

        # Extract email addresses
        emails = self._extract_emails(div_content)

        # Extract Hours of Operation
        hours = self._extract_hours(div_content)

        # Extract Addresses
        addresses = self._extract_address(div_content)

        # Combine all extracted information into a single dictionary
        return {
            "phone_numbers": combined_phones.get("phone_nums", []),
            "emails": emails.get("emails", []),
            "hours": hours.get("hours", []),
            "addresses": addresses.get("addresses", []),
        }, div_content_hash

    def _extract_phone_numbers(self: "InfoExtractor", content: str) -> dict:
        """Extract phone numbers from HTML content.

        Args:
        ----
            content (str): The HTML content to extract phone numbers from.

        Returns:
        -------
            dict: A dictionary containing extracted phone numbers.

        """
        response = self.gpt_client.chat.completions.create(
            model="gpt-3.5-turbo",
            messages=[
                {
                    "role": "system",
                    "content": "You are a helpful assistant. You are adept at extracting useful information for travelers from travel websites. You are excellent at extracting phone numbers, emails, hours of operation, and addresses from HTML websites.",
                },
                {
                    "role": "user",
                    "content": f"Examine the provided HTML content to extract ALL Defense Service Network (DSN) and commercial phone numbers even those in a list. For each phone number, identify if it is a DSN or commercial type. Include the type along with the phone number's descriptor. Expand any shorthand for phone numbers to ensure all are represented individually (e.g. '253-5509/5507/5262' -> ['2535509', '2535507', '2535262']) and format all phone numbers to remove any non-numeric characters. Additionally, phone numbers will be from different countries so they will not all have American formatting and might include country codes (e.g. 011-90-322-316-6111 -> '011903223166111'). If a phone number does not have an explicit purpose or descriptor, infer its purpose by examining the closest preceding phone number or text that provides a context (such as 'Service Counter', '24 Hour Flight Recording', '72 Hour Flight Recording', 'Lost and Found', 'Office', etc.). Assume that the  phone number shares the same purpose as this nearest described element. Output this information in a nested JSON format under the list 'phone_nums', where each entry contains keys 'value' (the phone number), 'description' (a brief descriptor of the phone's purpose, either provided or inferred), and 'notes' (any relevant notes about the phone number).\n\nExample output:\n\n{{\n    \"phone_nums\": [\n        {{\n            \"value\": \"97317859009\",\n            \"description\": \"Service Counter (Commercial)\",\n            \"notes\": \"Not always staffed.\"\n        }}\n      ]\n}}\n\nContent:\n\n```html\n{content}\n```",
                },
            ],
            temperature=0,
            max_tokens=1024,
            top_p=1,
            frequency_penalty=0,
            presence_penalty=0,
        )

        response_str = response.choices[0].message.content

        if not response_str:
            logging.error("No response from OpenAI API.")
            return {"phone_nums": []}

        # Extract the JSON response
        # Keep first curly brace through last curly brace
        json_str = response_str[response_str.find("{") : response_str.rfind("}") + 1]

        try:
            response_dict = json.loads(json_str)

        except json.JSONDecodeError as e:
            logging.error("Error decoding JSON response: %s", e)
            return {"phone_nums": []}

        except Exception as e:
            logging.error("Error parsing JSON response: %s", e)
            return {"phone_nums": []}

        return response_dict

    def _combine_phone_numbers(self: "InfoExtractor", phone_numbers: dict) -> dict:
        """Combine multiple phone numbers with the same description into a single list entry.

        Args:
        ----
            phone_numbers (dict): A dictionary containing extracted phone numbers.

        Returns:
        -------
            dict: A dictionary containing combined phone numbers. List under key 'phone_nums'.

        """
        # Access list of phone numbers
        phone_nums = phone_numbers.get("phone_nums", [])

        # Check if the phone_nums list is empty
        if not phone_nums:
            logging.error("No phone numbers found in the input dictionary.")
            return {"phone_nums": []}

        # Dictionary to hold combined phone numbers
        combined_phones: Dict[str, PhoneDetail] = {}

        # Combine phone numbers by description
        for item in phone_nums:
            description = item.get("description", "")
            phone_value = item.get("value", "")
            notes = item.get("notes", "")

            if description in combined_phones:
                # Append the phone number to the existing list under the same description
                combined_phones[description]["value"].append(phone_value)
            else:
                # Create a new entry for this description
                combined_phones[description] = {
                    "value": [phone_value],
                    "description": description,
                    "notes": notes,
                }

        # Convert the dictionary back to the list format expected in the output
        result = {"phone_nums": list(combined_phones.values())}
        logging.info("Phone numbers combined successfully.")
        return result

    def _extract_emails(self: "InfoExtractor", content: str) -> dict:
        """Extract email addresses from HTML content.

        Args:
        ----
            content (str): The HTML content to extract email addresses from.

        Returns:
        -------
            dict: A dictionary containing extracted email addresses.

        """
        response = self.gpt_client.chat.completions.create(
            model="gpt-3.5-turbo",
            messages=[
                {
                    "role": "system",
                    "content": "You are a helpful assistant. You are adept at extracting useful information for travelers from travel websites. You are excellent at extracting phone numbers, emails, hours of operation, and addresses from HTML websites.",
                },
                {
                    "role": "user",
                    "content": f"Identify and list all email addresses from the HTML content. Each email address should be added to the 'emails' list in the JSON output, with each item having 'value' (the email address), 'description' (a brief explanation of whose or what email it is), and 'notes' (any relevant notes related only to the email address). \nContent:\n\n```html\n{content}\n```",
                },
            ],
            temperature=0,
            max_tokens=256,
            top_p=1,
            frequency_penalty=0,
            presence_penalty=0,
        )

        response_str = response.choices[0].message.content

        if not response_str:
            logging.error("No response from OpenAI API.")
            return {"emails": []}

        # Extract the JSON response
        # Keep first curly brace through last curly brace
        json_str = response_str[response_str.find("{") : response_str.rfind("}") + 1]

        try:
            response_dict = json.loads(json_str)

        except json.JSONDecodeError as e:
            logging.error("Error decoding JSON response: %s", e)
            return {"emails": []}

        except Exception as e:
            logging.error("Error parsing JSON response: %s", e)
            return {"emails": []}

        return response_dict

    def _extract_hours(self: "InfoExtractor", content: str) -> dict:
        """Extract hours of operation from HTML content.

        Args:
        ----
            content (str): The HTML content to extract hours of operation from.

        Returns:
        -------
            dict: A dictionary containing extracted hours of operation.

        """
        response = self.gpt_client.chat.completions.create(
            model="gpt-3.5-turbo",
            messages=[
                {
                    "role": "system",
                    "content": "You are a helpful assistant. You are adept at extracting useful information for travelers from travel websites. You are excellent at extracting phone numbers, emails, hours of operation, and addresses from HTML websites.",
                },
                {
                    "role": "user",
                    "content": f"Extract hours of operation from the HTML content and convert them into a 24-hour format Open Hour-Close Hour (e.g., 0000-1000, 0400-2359). Document these hours in the 'hours' list within the JSON structure. Each entry should include 'value', which represents the operational hours in 24-hour format (Only one range per value). The 'days' should list the full names of the days and ranges (e.g., Monday-Friday, Sunday-Saturday, Tuesday-Monday) these hours apply to; if no specific days are mentioned, check if there are other words that indicate days of operation (e.g. Daily, Weekend, etc.). If there are no words related to days of operation, this field should be left empty. The 'description' should list what location (e.g., Service Desk, Annex, Shoppette, Exchange, Terminal, etc.) these hours apply to; if no location is mentioned, this field should be left empty. The 'notes' field should be used exclusively for special circumstances that alter standard hours of operation, such as a terminal opening later for a late-night flight. Ensure that the 'description' field only includes information about the days, and that 'notes' are strictly reserved for exceptions or irregularities affecting the listed hours. \n\nContent:\n\n```html\n{content}\n```",
                },
            ],
            temperature=0,
            max_tokens=256,
            top_p=1,
            frequency_penalty=0,
            presence_penalty=0,
        )

        response_str = response.choices[0].message.content

        if not response_str:
            logging.error("No response from OpenAI API.")
            return {"emails": []}

        # Extract the JSON response
        # Keep first curly brace through last curly brace
        json_str = response_str[response_str.find("{") : response_str.rfind("}") + 1]

        try:
            response_dict = json.loads(json_str)

        except json.JSONDecodeError as e:
            logging.error("Error decoding JSON response: %s", e)
            return {"emails": []}

        except Exception as e:
            logging.error("Error parsing JSON response: %s", e)
            return {"emails": []}

        return response_dict

    def _extract_address(self: "InfoExtractor", content: str) -> dict:
        """Extract addresses from HTML content.

        Args:
        ----
            content (str): The HTML content to extract addresses from.

        Returns:
        -------
            dict: A dictionary containing extracted addresses.

        """
        response = self.gpt_client.chat.completions.create(
            model="gpt-3.5-turbo",
            messages=[
                {
                    "role": "system",
                    "content": "You are a helpful assistant. You are adept at extracting useful information for travelers from travel websites. You are excellent at extracting phone numbers, emails, hours of operation, and addresses from HTML websites.",
                },
                {
                    "role": "user",
                    "content": f"Locate all formatted street addresses within the HTML content that match the typical street address format. These should be added to the 'addresses' list in the JSON output as 'value' (the address itself). Each address should also have a  'description' (a descriptor of the location), and 'notes' (any pertinent notes about the address).\n\n\nContent:\n\n```html\n{content}\n```",
                },
            ],
            temperature=0,
            max_tokens=256,
            top_p=1,
            frequency_penalty=0,
            presence_penalty=0,
        )

        response_str = response.choices[0].message.content

        if not response_str:
            logging.error("No response from OpenAI API.")
            return {"emails": []}

        # Extract the JSON response
        # Keep first curly brace through last curly brace
        json_str = response_str[response_str.find("{") : response_str.rfind("}") + 1]

        try:
            response_dict = json.loads(json_str)

        except json.JSONDecodeError as e:
            logging.error("Error decoding JSON response: %s", e)
            return {"emails": []}

        except Exception as e:
            logging.error("Error parsing JSON response: %s", e)
            return {"emails": []}

        return response_dict

    def _extract_div_content(self: "InfoExtractor", html_content: str) -> Optional[str]:
        """Extract the parent div content of a specific span with id and class.

        This is the div that will contain all the contact information Space A terminals.

        Args:
        ----
            html_content (str): The HTML content to extract information from.

        Returns:
        -------
            str: The extracted parent div content.

        """
        # Parse the HTML content using BeautifulSoup
        soup = BeautifulSoup(html_content, "html.parser")

        # Find the specific span by id, class, and text
        # First parent div of span with id that contains "dnnTITLE_titleLabel",
        # class "title" and with Value of "Contact Information".
        span = soup.find(
            "span",
            {"id": lambda x: x and "dnnTITLE_titleLabel" in x, "class": "title"},
            text=lambda t: t and "contact information".lower() in t.lower(),
        )

        if span:
            # Find the first parent div of the found span
            parent_div = span.find_parent("div")
            if parent_div:
                return str(parent_div)  # Return the HTML content of the parent div

        return None
