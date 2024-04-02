from fastapi import FastAPI, HTTPException
from fastapi.responses import JSONResponse
import json
from selenium import webdriver
from selenium.webdriver.common.by import By
from webdriver_manager.chrome import ChromeDriverManager
from baseurl import get_mongo_connection, get_database
from uuid import uuid4

app = FastAPI()
db = get_mongo_connection()

collection = db["cups"]


def scrape_cups(url):
    try:
        driver = webdriver.Chrome(ChromeDriverManager().install())

        driver.get(url)

        driver.implicitly_wait(10)

        accept_button = driver.find_element(By.ID, "onetrust-accept-btn-handler")
        accept_button.click()

        table = driver.find_element(By.CLASS_NAME, "tb")

        rows = table.find_elements(By.TAG_NAME, "tr")

        data_list = []

        for row in rows[1:]:
            cells = row.find_elements(By.TAG_NAME, "td")

            (
                year,
                races,
                champion,
                car_owner,
                crew_chief,
                rookie_of_the_year,
                popular_driver,
                manufacturers_championship,
            ) = [cell.text for cell in cells]

            car_owner_list = [owner.strip() for owner in car_owner.split("\n")]
            crew_chief_list = [chief.strip() for chief in crew_chief.split("\n")]

            # Generate a UUID for each row
            row_uuid = str(uuid4())

            row_data = {
                "UUID": row_uuid,
                "Year": year,
                "Races": races,
                "Champion": champion,
                "Car Owner": car_owner_list,
                "Champion's Crew Chief": crew_chief_list,
                "Rookie of the Year": rookie_of_the_year,
                "Most Popular Driver": popular_driver,
                "Manufacturers' Championship": manufacturers_championship,
            }

            data_list.append(row_data)

        collection.insert_many(data_list)

        return "Data inserted into MongoDB successfully."

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error scraping data: {str(e)}")

    finally:
        # WebDriver ko close karein
        if "driver" in locals():
            driver.quit()
