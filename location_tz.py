import logging
import os
from typing import Optional, Tuple

import geocoder  # type: ignore
import geocoder.osm  # type: ignore
from openai import OpenAI
from timezonefinder import TimezoneFinder


class BadLocationError(Exception):
    """Custom exception for a specific purpose."""

    def __init__(
        self: "BadLocationError",
        message: str = "The location was not able to be geocoded.",
    ) -> None:
        """Exception class to be thrown when locations can't be geocoded."""
        super().__init__(message)


class TerminalTzFinder:
    """Class to find the timezone of a location string.

    It geocodes the location string and then determines the PYTZ timezone of
    the location string. It uses ChatGPT to estimate the location if Google Maps
    and OSM fail to geocode the original string.
    """

    def __init__(self: "TerminalTzFinder") -> None:
        """Initialize the TimezoneFinder class.

        This retrieves API keys from the environment for OpenAI and Google Maps.
        """
        gpt_key = os.getenv("OPENAI_API_KEY", None)
        google_key = os.getenv("GOOGLE_MAPS_API_KEY", None)

        if not gpt_key:
            msg = "OPENAI_API_KEY environment variable not set."
            raise EnvironmentError(msg)

        if not google_key:
            msg = "GOOGLE_MAPS_API_KEY environment variable not set."
            raise EnvironmentError(msg)

        self.google_key = google_key
        self.gpt_client = OpenAI(api_key=gpt_key)

    def _get_geocode(
        self: "TerminalTzFinder", location: str
    ) -> Optional[Tuple[float, float]]:
        """Get the geocode of a location string.

        This first tries to geocode the location string using Google Maps. If
        that fails, it tries to geocode the location string using OSM Nominatim.

        Args:
        ----
            location (str): The location string to geocode.

        Returns:
        -------
            Optional[Tuple[float, float]]: A tuple of latitude and longitude
        """
        # Try Google Maps first
        g = geocoder.google(location, method="timezone", key=self.google_key)
        if g.ok and g.latlng:
            return g.latlng

        logging.info("Google Maps failed to geocode: %s", location)

        # Fallback to OpenStreetMap Nominatim
        g = geocoder.osm(location)
        if g.ok and g.latlng:
            return g.latlng

        logging.info("OSM Nominatim failed to geocode: %s", location)

        # If both fail
        return None

    def _estimate_location_gpt(self: "TerminalTzFinder", location: str) -> str:
        """Estimate the location of a location string using ChatGPT.

        Args:
        ----
            location (str): The location string to geocode.

        Returns:
        -------
            list: A list of latitude and longitude coordinates.
        """
        response = self.gpt_client.chat.completions.create(
            model="gpt-3.5-turbo",
            messages=[
                {
                    "role": "system",
                    "content": 'Can you tell me the location of the following string? If you are not confident in making a guess, tell me the largest city that is probably nearby. Only return a geocodable string. For example, "NS Rota Terminal" you return only "NS Rota, Spain" or "Rota, Spain" depending which is more appropriate based on your confidence. ',
                },
                {"role": "user", "content": "Al Udeid Terminal"},
                {"role": "assistant", "content": "Al Udeid, Qatar"},
                {"role": "user", "content": "Osan AB, ROK"},
                {"role": "assistant", "content": "Osan, South Korea"},
                {"role": "user", "content": "Altus AFB, OK"},
                {"role": "assistant", "content": "Altus, Oklahoma, United States"},
                {
                    "role": "user",
                    "content": location,
                },
            ],
            temperature=0,
            max_tokens=256,
            top_p=1,
            frequency_penalty=0,
            presence_penalty=0,
        )

        if response.choices:
            # Retrieve the first Choice object
            first_choice = response.choices[0]

            # Extract the message content
            gpt_response = first_choice.message.content

            if not gpt_response:
                msg = f"GPT-3 failed to estimate location of string: {location}"
                raise BadLocationError(msg)

            return gpt_response

        msg = "No valid response from GPT-3 for timzone string."
        raise ValueError(msg)

    def get_timezone(self: "TerminalTzFinder", location: str) -> str:
        """Get the timezone of a location string.

        Args:
        ----
            location (str): The location string to geocode.

        Returns:
        -------
            str: The timezone string in the format of "America/New_York"
        """
        latlng = self._get_geocode(location)

        # If geocoding fails, use ChatGPT to estimate the location
        if latlng is None:
            logging.info("Geocoding failed for location: %s", location)
            estimated_location = self._estimate_location_gpt(location)
            if estimated_location:
                latlng = self._get_geocode(estimated_location)

        if latlng is None:
            msg = f"Could not geocode the location: {location}"
            raise BadLocationError(msg)

        # Convert geocode to Pytz timezone
        tz_finder = TimezoneFinder()
        timezone_str = tz_finder.timezone_at(lat=latlng[0], lng=latlng[1])

        if timezone_str is None:
            msg = "Could not determine the timezone for the given coordinates"
            raise ValueError(msg)

        return timezone_str
