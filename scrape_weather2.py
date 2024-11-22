"""
Description: Contains all code required to scrape the weather data. 
Author: Paramveer Kaur, Ravneet Kaur Sandhu, Stephanie Charriere
Section Number: 254277
Date Created: November 12 2024
Credit: 
Updates: Updated the code to include the database operations. 
"""

from html.parser import HTMLParser
from datetime import datetime
import requests
from db_operations import DBOperations


class WeatherScraper(HTMLParser):
    """
    A class to scrape weather data from a given URL.
    """
    def __init__(self):
        super().__init__()
        self.weather_data = {}  # Dictionary to store results
        self.current_date = None  # Full date string
        self.temp_values = {}  # Dictionary to store temperatures
        self.is_in_row = False
        self.column_index = 0
        self.previous_month_button = False  # Flag to track the "Previous Month" button

    def handle_starttag(self, tag, attrs):
        if tag == "li":
            # Check for the "Previous Month" button
            for attr in attrs:
                if attr[0] == "id" and attr[1] == "nav-prev1":
                    # Check if the button is disabled
                    if not any(
                        attr[0] == "class" and "disabled" in attr[1] for attr in attrs
                    ):
                        self.previous_month_button = True  # Button is enabled, continue scraping
                    break

        if tag == "tr":  # Start of a table row
            self.is_in_row = True
            self.temp_values = {}  # Reset temperature values
            self.column_index = 0  # Reset column counter

        if tag == "abbr" and self.is_in_row:  # Look for the date
            for attr in attrs:
                if attr[0] == "title":  # Extract the `title` attribute
                    try:
                        # Validate the date format
                        datetime.strptime(attr[1], "%B %d, %Y")
                        self.current_date = attr[1]  # Valid date string
                    except ValueError:
                        self.current_date = None  # Skip invalid dates

    def handle_data(self, data):
        if self.is_in_row and data.strip():  # Handle cell data
            data = data.strip()
            self.column_index += 1

            try:
                # Map columns to temperatures
                if self.column_index == 1:
                    pass  # Skip since it's the date, already handled
                elif self.column_index == 2:  # Max Temp
                    self.temp_values["Max"] = float(data)
                elif self.column_index == 3:  # Min Temp
                    self.temp_values["Min"] = float(data)
                elif self.column_index == 4:  # Mean Temp
                    self.temp_values["Mean"] = float(data)
            except ValueError:
                # Handle cases where data is missing or invalid
                pass

    def handle_endtag(self, tag):
        if tag == "tr" and self.is_in_row:  # End of a table row
            self.is_in_row = False

            if self.current_date and self.temp_values:
                # Only save rows with valid Max, Min, and Mean temperatures
                if all(key in self.temp_values for key in ["Max", "Min", "Mean"]):
                    try:
                        # Confirm that the date is valid
                        datetime.strptime(self.current_date, "%B %d, %Y")
                        self.weather_data[self.current_date] = self.temp_values
                    except ValueError:
                        pass  # Skip rows with invalid dates

            # Reset for the next row
            self.current_date = None
            self.temp_values = {}

    def fetch_weather_data(self, base_url, start_year, start_month):
        """
        Fetches weather data starting from the given year and month, and goes backwards in time.

        Args:
            base_url (str): The base URL for the weather data.
            start_year (int): The starting year for scraping.
            start_month (int): The starting month for scraping.

        Returns:
            dict: A dictionary containing the weather data.
        """
        year, month = start_year, start_month

        continue_scraping = True

        while continue_scraping:

            # Construct the URL for the current month and year
            url = f"{base_url}&Year={year}&Month={month}"
            print(f"Fetching: {url}")
            response = requests.get(url, timeout=10)

            if response.status_code != 200:
                print(
                    f"Failed to fetch data for {year}-{month}: {response.status_code}"
                )
                break

            # Parse the page content
            self.feed(response.text)

            # Check if the "Previous Month" button is missing, stop scraping
            if not self.previous_month_button:
                print(
                    f"No 'Previous Month' button found. Stopping scraping at {year}-{month}."
                )
                continue_scraping = False  # Set flag to False to stop the loop

            # Reset `previous_month_button` flag to False after parsing
            self.previous_month_button = False

            # Move to the previous month
            if month == 1:
                month = 12
                year -= 1
            else:
                month -= 1

        return self.weather_data


# Usage
if __name__ == "__main__":
    BASE_URL = "http://climate.weather.gc.ca/climate_data/daily_data_e.html?StationID=27174&timeframe=2"

    scraper = WeatherScraper()
    today = datetime.now()

    weather_data = scraper.fetch_weather_data(BASE_URL, today.year, today.month)

    # Database operations
    operation = DBOperations()
    operation.initialize_db()
    operation.save_data(weather_data)

    # Close database connection
    operation.close_connection()
