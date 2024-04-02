"""
Automate Script
Script to INNER 2ND PAGE of tarcks record  

1st page link
https://www.racing-reference.info/tracks-landing-page/

2nd page link
https://www.racing-reference.info/tracks/Airborne_Speedway/

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
from datetime import datetime
from baseurl import get_mongo_connection, get_database

db = get_mongo_connection()
track_firstpage_collection = db["track_page_1"]
track_secondpage_collection = db["track_page_2"]


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


def scrap_data(html):
    soup = BeautifulSoup(html, "html.parser")
    parent_table = soup.find(
        "table", {"width": "100%", "border": "0", "bgcolor": "WHITE"}
    )
    all_child_tables = parent_table.find_all("table", {"width": "100%"})
    table_names = [
        child_table.find("h2").get_text(strip=True)
        for child_table in all_child_tables
        if child_table.find("h2")
    ]

    all_tables = parent_table.find_all("table", {"class": "tb"})

    counter = 0
    all_data_all_table = []

    for table in all_tables:
        headers_tr = table.find("tr", class_="newhead")
        all_headers_th = headers_tr.find_all("th") if headers_tr else ""
        headers = [
            headers_th.get_text(strip=True).replace(" ", "")
            for headers_th in all_headers_th
        ]
        headers = headers + ["row_uuid", "created_at"]

        all_data = []

        rows = table.find_all("tr", {"class": ["odd", "even"]})
        for row in rows:
            data_in_row = row.find_all("td")
            data = [datarow.get_text(strip=True) for datarow in data_in_row]
            created_at = datetime.utcnow().isoformat()
            data = data + [str(uuid.uuid4()), created_at]
            all_data.append(dict(zip(headers, data)))

        created_at = datetime.utcnow().isoformat()
        table_data = {
            "table_name": table_names[counter],
            "table_data": all_data,
            "table_uuid": str(uuid.uuid4()),
            "created_at": created_at,
        }
        all_data_all_table.append(table_data)
        counter += 1

    return all_data_all_table


def sysInit():
    display = Display(visible=0, size=(800, 600))
    # display.start()
    driver = None
    try:
        print("Starting........")
        driver = webdriver.Chrome(ChromeDriverManager().install(), options=options)
        driver.get("https://www.racing-reference.info/tracks-landing-page/")
        time.sleep(2)

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

        tracks = track_firstpage_collection.find()
        count = 0

        for track in tracks:
            track_inner_page_link = track.get("link", "")
            track_uuid = track.get("UUID", "")
            track_name = track.get("Track", "")[0]
            print(f"\n\n\n************* Starting {count+1}  ***************")
            print("Track Name: ", track_name)
            print("Track UUID: ", track_uuid)
            print("Track Link : ", track_inner_page_link)

            driver.get(track_inner_page_link)
            time.sleep(2)
            html = driver.page_source
            all_tables_data = scrap_data(html)
            obj = {
                "all_tables": all_tables_data,
                "track_name": track_name,
                "track_uuid": track_uuid,
                "track_page_link": track_inner_page_link,
            }
            print("\n")
            print(json.dumps(obj))
            track_secondpage_collection.insert_many([obj])
            count = count + 1
            if count == 20:
                break

    finally:
        print("QUIT WEB DRIVER ______________")
        # display.stop()
        if driver:
            driver.quit()
