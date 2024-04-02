from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException
from webdriver_manager.chrome import ChromeDriverManager
from pymongo import MongoClient
from fastapi.responses import JSONResponse
from fastapi import FastAPI
import uuid
from bs4 import BeautifulSoup
import time
from baseurl import get_mongo_connection, get_database

app = FastAPI()

db = get_mongo_connection()

owner_collection = db["owners"]


def accept_cookies(driver):
    try:
        cookie_banner = WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.ID, "onetrust-button-group"))
        )
        accept_button = cookie_banner.find_element(By.ID, "onetrust-accept-btn-handler")
        accept_button.click()
    except TimeoutException:
        pass


def scrape_owners(url):
    driver = webdriver.Chrome(ChromeDriverManager().install())
    owner_data_list = []  # Use a list to store owner data dictionaries

    try:
        driver.get(url)
        accept_cookies(driver)

        wait = WebDriverWait(driver, 10)
        owner_rows = wait.until(
            EC.presence_of_all_elements_located((By.XPATH, "//td[@class='col']/b/a"))
        )

        for owner_row in owner_rows:
            owner_name = owner_row.text.strip()
            owner_link = owner_row.get_attribute("href")  # Get the link of the owner
            owner_uuid = str(uuid.uuid4())  # Generate UUID for each owner

            owner_data = {"UUID": owner_uuid, "Owner": owner_name, "Link": owner_link}

            owner_data_list.append(owner_data)
            print(owner_data)  # Print each owner data

        # Insert list of owner data dictionaries into MongoDB
        if owner_data_list:
            owner_collection.insert_many(owner_data_list)
            print("Data inserted into MongoDB")

    except Exception as e:
        print(driver.page_source)
        raise e

    finally:
        driver.quit()
