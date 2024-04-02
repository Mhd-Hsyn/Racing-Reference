from fastapi import FastAPI, HTTPException
from fastapi.responses import JSONResponse
import json
from selenium import webdriver
from selenium.webdriver.common.by import By
from webdriver_manager.chrome import ChromeDriverManager
from pymongo import MongoClient
from baseurl import get_mongo_connection, get_database
import uuid
from datetime import datetime

app = FastAPI()

db = get_mongo_connection()
collection = db["f1"]  # Replace "your_database_name" with your actual database name


def scrape_f1_series(url):
    try:
        # Creating a Chrome WebDriver without specifying the path (it assumes the chromedriver is in the system's PATH)
        driver = webdriver.Chrome(ChromeDriverManager().install())

        driver.get(url)

        # WebDriver ko thoda wait karein, taki page load ho sake
        driver.implicitly_wait(10)

        # Accept All Cookies button ko dhundhe aur click karein
        accept_button = driver.find_element(By.ID, "onetrust-accept-btn-handler")
        accept_button.click()

        # Find the table element
        table = driver.find_element(By.CLASS_NAME, "tb")

        # Find all rows in the table
        rows = table.find_elements(By.TAG_NAME, "tr")

        # Initialize list to store data
        data_list = []

        # Iterate through rows and append data to the list of dictionaries
        for row in rows:
            columns = row.find_elements(By.XPATH, ".//td[@class='col']")
            year = columns[0].text.strip()

            Races = columns[1].text.strip()
            Driver_Champion = columns[2].text.strip()
            Driver_Points = columns[3].text.strip()
            Constructor_Champion = columns[4].text.strip()
            Constructor_Points = columns[5].text.strip()
            Engine = columns[6].text.strip()

            # Generate UUID and current timestamp
            unique_id = str(uuid.uuid4())
            timestamp = datetime.utcnow().isoformat()

            series_champion_data = {
                "UUID": unique_id,
                "created_at": timestamp,
                "Year": year,
                "Races": Races,
                "Driver_Champion": Driver_Champion,
                "Driver_Points": Driver_Points,
                "Constructor_Champion": Constructor_Champion,
                "Constructor_Points": Constructor_Points,
                "Engine": Engine,
            }

            data_list.append(series_champion_data)

        collection.insert_many(data_list)

        return "Data inserted into MongoDB successfully."

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error scraping data: {str(e)}")

    finally:
        # WebDriver ko close karein
        if "driver" in locals():
            driver.quit()
