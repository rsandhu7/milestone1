"""
Description: Contains all code to populate database.
Author: Paramveer Kaur, Ravneet Kaur Sandhu, Stephanie Charriere
Section Number: 254277
Date Created: November 12 2024
Credit: 
Updates:
"""

import sqlite3

class DBOperations:
    """
    Class to handle all database operations for weather data.
    """

    def __init__(self, db_name="Weather_data"):
        self.db_name = db_name
        self.connection = sqlite3.connect(self.db_name)  # Persistent connection
        self.cursor = self.connection.cursor()

    def initialize_db(self):
        """
        Creates the weather table if it doesn't already exist.
        """

        self.cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS weather (
            id INTEGER PRIMARY KEY AUTOINCREMENT not null,
            date TEXT NOT NULL,
            location TEXT NOT NULL,
            min_temp REAL,
            max_temp REAL,
            avg_temp REAL,
            UNIQUE(date));"""
        )
        self.connection.commit()

    def save_data(self, weather_data, location="Winnipeg"):
        """
        Saves weather data to the database.
        :param weather_data: Dictionary of dictionaries with date as key and temperature data as values.
        :param location: Location name (default is 'Winnipeg').
        """

        sql = """INSERT OR IGNORE INTO weather (date, location, min_temp, max_temp, avg_temp) values (?,?,?,?,?)"""
        try:
            for date, temp_data in weather_data.items():
                min_temp = temp_data.get("Min")
                max_temp = temp_data.get("Max")
                avg_temp = temp_data.get("Mean")

                line = (date, location, min_temp, max_temp, avg_temp)

                self.cursor.execute(sql, line)
            self.connection.commit()
        except sqlite3.Error as e:
            print(f"Error while inserting data: {e}")

    # Just for testing purposes
    def print(self):
        """Print out the data currently in the database."""
        self.cursor.execute("SELECT * FROM weather")
        for row in self.cursor.fetchall():
            print(row)

    def close_connection(self):
        """Close the database connection."""
        if self.connection:
            self.connection.close()
