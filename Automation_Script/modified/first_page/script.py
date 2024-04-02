"""
Automate Script

Script to scrap the data of Modified-Tour

https://www.racing-reference.info/nascar-whelen-modified-tour/

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
from pymongo import MongoClient
from baseurl import get_mongo_connection, get_database


db = get_mongo_connection()
modified_firstpage_collection = db["modified_page_1"]


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


def scrap_data_table1(table1):
    headers_row = table1.find_all("tr")[0]
    headers = [td.text.strip() for td in headers_row]
    headers = headers + ["year_link", "year_uuid"]
    print(headers)

    all_rows = table1.find_all("tr", class_=["odd", "even"])
    data = []
    for row in all_rows:
        all_tds = row.find_all("td")
        row_data = {}
        # Extract the link from the first td in the row
        link_td = all_tds[0]
        link = link_td.find("a")["href"] if link_td.find("a") else ""
        row_data["year_link"] = link

        # Loop through each cell in the row and add it to the dictionary
        for index, td in enumerate(all_tds):
            # Check if the cell contains a link
            row_data[
                headers[index]
            ] = td.text.strip()  # Add text to corresponding key in the dictionary

        # Append the row data to the list of data
        row_data["year_uuid"] = str(uuid.uuid4())
        data.append(row_data)

    modified_firstpage_collection.insert_many(data)
    print("\n\n\n\n *************** ALL DATA **************** \n\n")
    # print(data)
    # print(json.dumps(data))
    return data


def sysInit():
    display = Display(visible=0, size=(800, 600))
    # display.start()
    driver = None
    try:
        print("Starting........")
        driver = webdriver.Chrome(ChromeDriverManager().install(), options=options)
        driver.maximize_window()
        driver.get("https://www.racing-reference.info/nascar-whelen-modified-tour/")
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

        html = driver.page_source
        soup = BeautifulSoup(html, "html.parser")
        table1 = soup.find("table", class_="tb")
        data = scrap_data_table1(table1)

    finally:
        print("QUIT WEB DRIVER ______________")
        # display.stop()
        if driver:
            driver.quit()
