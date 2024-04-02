"""
Automate Script

Script to scrap the data of tarcks record 

https://www.racing-reference.info/tracks-landing-page/

"""


import time, json, uuid
import pandas as pd
from bs4 import BeautifulSoup
from pyvirtualdisplay import Display
from bs4 import BeautifulSoup
import pandas as pd
from urllib.parse import urlparse, parse_qs
from selenium import webdriver
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
from fake_useragent import UserAgent
from selenium.webdriver.support.ui import Select
from baseurl import get_mongo_connection, get_database


db = get_mongo_connection()

track_firstpage_collection = db["track_page_1"]


def get_random_headers():
    ua = UserAgent()
    headers = {"User-Agent": ua.random, "Accept-Language": "en-US, en;q=0.5"}
    return headers


headers = get_random_headers()

# Set Chrome options
options = Options()
# options.headless = False
options.add_argument("--enable-logging")
options.add_argument("--log-level=0")
# options.add_argument('user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3')
options.add_argument(f'user-agent={headers["User-Agent"]}')
options.add_argument("--no-sandbox")


def scrap_data(html, series_name):
    soup = BeautifulSoup(html, "html.parser")
    table = soup.find("table", class_="tb")
    headers_row = table.find("tr", class_="newhead")
    headers = [th.text.strip() for th in headers_row.find_all("th")]
    headers = headers + ["link", "UUID"]

    all_rows = table.find_all("tr", class_=["odd", "even"])
    data = []
    for row in all_rows:
        all_tds = row.find_all("td")
        row_data = {}
        row_data["series_name"] = series_name

        # Loop through each cell in the row and add it to the dictionary
        for index, td in enumerate(all_tds):
            # Check if the cell contains a link
            if td.find("a"):
                row_data["link"] = td.a["href"]
                text_list = (
                    [td.a.get_text(strip=True), td.small.get_text(strip=True)]
                    if td.small
                    else [td.a.get_text(strip=True)]
                )
                row_data[headers[index]] = text_list

            elif td.find("img"):
                img = td.img["src"]  # Add image URL to 'country' key in the dictionary
                if not img.startswith("http:"):
                    img = "https:" + img
                row_data[headers[index]] = img

            else:
                row_data[
                    headers[index]
                ] = td.text.strip()  # Add text to corresponding key in the dictionary

        # print("\n\n ROW DATA \n\n")
        # print(json.dumps(row_data))

        # Append the row data to the list of data
        row_data["UUID"] = str(uuid.uuid4())
        data.append(row_data)

    track_firstpage_collection.insert_many(data)
    print("\n\n\n\n *************** ALL DATA **************** \n\n")
    print(data)
    return data


def sysInit():
    display = Display(visible=0, size=(800, 600))
    # display.start()
    driver = None
    try:
        print("Starting........")
        driver = webdriver.Chrome(ChromeDriverManager().install(), options=options)
        driver.maximize_window()
        driver.get("https://www.racing-reference.info/tracks-landing-page/")
        time.sleep(5)

        try:
            cookie_consent = WebDriverWait(driver, 10).until(
                EC.element_to_be_clickable(
                    (By.XPATH, "//button[text()='Accept All Cookies']")
                )
            )
            cookie_consent.click()
        except Exception as e:
            print("Error handling cookie consent:", str(e))

        time.sleep(2)

        dropdown = Select(driver.find_element(By.NAME, "series"))
        all_options = dropdown.options
        all_option_values = [option.get_attribute("value") for option in all_options]
        all_option_texts = [option.text for option in all_options]
        # Now, 'all_option_values' contains option values, and 'all_option_texts' contains option texts
        print("Option Values:", all_option_values)
        print("Option Texts:", all_option_texts)

        all_data = []
        count = 0
        for option_value, option_text in zip(all_option_values, all_option_texts):
            print("*******", option_value, "********")
            print("Text:", option_text)

            if option_value == "ALL":
                print("ALL is bypass")
                continue

            dropdown = Select(driver.find_element(By.NAME, "series"))
            dropdown.select_by_value(option_value)

            submit_button = driver.find_element(By.CLASS_NAME, "submitButton")
            submit_button.click()
            time.sleep(3)
            html = driver.page_source
            data = scrap_data(html, series_name=option_text)
            all_data.extend(data)

            count += count
            if count >= 3:
                break

        return all_data

    finally:
        print("QUIT WEB DRIVER ______________")
        # display.stop()
        if driver:
            driver.quit()
