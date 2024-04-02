from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException
from webdriver_manager.chrome import ChromeDriverManager
from pymongo import MongoClient
from fastapi.responses import JSONResponse
from fastapi import FastAPI
from baseurl import get_mongo_connection, get_database
from datetime import datetime
import uuid

app = FastAPI()

db = get_mongo_connection()

collection = db["Imsa"]


def generate_uuid():
    return str(uuid.uuid4())


def accept_cookies(driver):
    try:
        # Wait for the cookie banner to appear
        cookie_banner = WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.ID, "onetrust-button-group"))
        )

        # Click on the "Accept All Cookies" button
        accept_button = cookie_banner.find_element(By.ID, "onetrust-accept-btn-handler")
        accept_button.click()

    except TimeoutException:
        # Cookie banner did not appear within the timeout, continue without accepting
        pass


def scrape_imsa_series(url):
    # Set up the WebDriver (make sure you have ChromeDriver or another appropriate WebDriver installed)
    driver = webdriver.Chrome(ChromeDriverManager().install())

    series_champions_data = (
        []
    )  # Initialize an empty list to store series champions data

    try:
        # Open the webpage
        driver.get(url)

        # Accept cookies
        accept_cookies(driver)

        # Wait for the element to be present in the DOM
        wait = WebDriverWait(driver, 10)
        series_champions_table = wait.until(
            EC.presence_of_element_located((By.XPATH, "//table[@class='tb']"))
        )

        # Extract data from the table
        rows = series_champions_table.find_elements(
            By.XPATH, "//tr[@class='even' or @class='odd']"
        )
        for row in rows:
            columns = row.find_elements(By.XPATH, ".//td[@class='col']")
            year = columns[0].text.strip()
            races = columns[1].text.strip()
            prototype_gtp = columns[2].text.strip()
            prototype_challenge_lmp2 = columns[3].text.strip()
            lmp3 = columns[4].text.strip()
            gt_le_mans_gtd_pro = columns[5].text.strip()
            gt_daytona = columns[6].text.strip()

            # Generate a UUID for each document
            document_uuid = generate_uuid()

            series_champion_data = {
                "UUID": document_uuid,
                "Year": year,
                "Races": races,
                "Prototype_GTP": prototype_gtp.split("\n") if prototype_gtp else [],
                "Prototype_Challenge_LMP2": prototype_challenge_lmp2.split("\n")
                if prototype_challenge_lmp2
                else [],
                "LMP3": lmp3.split("\n") if lmp3 else [],
                "GT_LeMans_GTD_Pro": gt_le_mans_gtd_pro.split("\n")
                if gt_le_mans_gtd_pro
                else [],
                "GT_Daytona": gt_daytona.split("\n") if gt_daytona else [],
                "created_at": datetime.utcnow().isoformat(),
            }

            series_champions_data.append(series_champion_data)

        # Use insert_many with a list of documents
        if series_champions_data:
            collection.insert_many(series_champions_data)

    except Exception as e:
        print(driver.page_source)  # Print the page source for debugging
        raise e  # Re-raise the exception after printing page source

    finally:
        # Close the WebDriver
        driver.quit()

    # Print the series champions data for all years in JSON-like format
    print("\nseries_champions_data", series_champions_data)


# Example usage
# scrape_imsa_series("https://www.racing-reference.info/imsa-series/")
