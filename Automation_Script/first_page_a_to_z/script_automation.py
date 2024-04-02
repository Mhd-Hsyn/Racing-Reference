"""
Automate Script

check the first_page_all_data.csv if their is any missing rider then it save in the first_page_all_data.csv
as well as    new_data_first_page.csv

http://0.0.0.0:9002/api/set-schedule-interval/

"""

import time, uuid
import pandas as pd
from bs4 import BeautifulSoup
import os
from bs4 import BeautifulSoup
import pandas as pd
from selenium import webdriver
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
from fake_useragent import UserAgent


def get_random_headers():
    ua = UserAgent()
    headers = {"User-Agent": ua.random, "Accept-Language": "en-US, en;q=0.5"}
    return headers


def scrap_first_page(options):
    driver = None
    try:
        print(
            "\n\n ****** Starting 1st page missing drivers name........ ******** \n\n"
        )
        driver = webdriver.Chrome(ChromeDriverManager().install(), options=options)
        driver.get("https://www.racing-reference.info/active-drivers/")
        time.sleep(5)

        try:
            cookie_consent = WebDriverWait(driver, 10).until(
                EC.element_to_be_clickable(
                    (By.XPATH, "//button[text()='I Accept']")
                )
            )
            cookie_consent.click()
        except Exception as e:
            print("Error handling cookie consent:", str(e))

        time.sleep(2)

        current_directory = os.getcwd()
        file_path = os.path.join(
            current_directory,
            "Automation_Script/first_page_a_to_z/first_page_all_data.csv",
        )

        new_data = []

        # Go A to Z all links one by one
        for letter in range(ord("A"), ord("B") + 1):
            # if chr(letter) == "B":
            #     continue
            letter_link = (
                f"https://www.racing-reference.info/driver-list/?ltr={chr(letter)}"
            )
            # Open the link for the current letter
            driver.get(letter_link)
            time.sleep(2)

            html_content = driver.page_source
            if html_content:
                soup = BeautifulSoup(html_content, "html.parser")
                table = soup.find("table", class_="tb")
                if table:
                    # Find all rows with class 'even' or 'odd'
                    rows = table.find_all("tr", class_=["even", "odd"])

                    # Manually specify column names based on the provided HTML structure
                    column_names = [
                        "Driver",
                        "Born",
                        "Died",
                        "Home",
                        "Active?",
                        "Link",
                        "Rider_Country_Img",
                        "UUID",
                    ]

                    for index, row in enumerate(rows, start=1):
                        # if index > 1 or index <= 0:
                        #     continue
                        # Extract driver name and link from the first column
                        driver_col = row.find("td", class_="col")
                        driver_link_ele = (
                            driver_col.find("a")["href"] if driver_col.find("a") else ""
                        )
                        driver_link = (
                            f"https://www.racing-reference.info/{driver_link_ele}"
                            if driver_link_ele
                            else ""
                        )

                        driver_name = driver_col.text.strip() if driver_col else ""
                        # Extract image URL from the <img> tag
                        img_tag = driver_col.find("img")
                        rider_country_img_ele = img_tag["src"] if img_tag else ""
                        rider_country_img = (
                            rider_country_img_ele.lstrip("//")
                            if rider_country_img_ele
                            else ""
                        )

                        row_data = (
                            [driver_name]
                            + [col.text.strip() for col in row.find_all("td")[1:]]
                            + [driver_link, rider_country_img]
                        )

                        # check if the file exists or not
                        if os.path.exists(file_path):
                            # get the csv
                            existing_df = pd.read_csv(file_path)
                            print("\n\n\n\tEXISTING DF\n", existing_df)
                            # compare the csv link
                            if driver_link not in existing_df["Link"].values:
                                print("DRIVER not exists ---------")
                                row_uuid = str(uuid.uuid4())
                                row_data = row_data + [row_uuid]
                                old_df = pd.read_csv(file_path)
                                new_df = pd.DataFrame([row_data], columns=column_names)
                                final_df = pd.concat(
                                    [old_df, new_df], ignore_index=True
                                )
                                # Save the updated DataFrame to the CSV file
                                final_df.to_csv(file_path, index=False)
                                new_data.append(row_data)
                        else:
                            print("FILE not exists ---------")
                            row_uuid = str(uuid.uuid4())
                            row_data = row_data + [row_uuid]
                            df = pd.DataFrame([row_data], columns=column_names)
                            df.to_csv(file_path, index=False)

        if new_data:
            print("NEW DATA\n", new_data)
            new_data_df = pd.DataFrame(new_data, columns=column_names)
            new_data_file_path = os.path.join(
                current_directory,
                "Automation_Script/first_page_a_to_z/new_data_first_page.csv",
            )
            new_data_df.to_csv(new_data_file_path, index=False)
            print("New riders data save in the new_data_first_page.csv ")

        else:
            new_data_df = pd.DataFrame(new_data, columns=column_names)
            new_data_file_path = os.path.join(
                current_directory,
                "Automation_Script/first_page_a_to_z/new_data_first_page.csv",
            )
            new_data_df.to_csv(new_data_file_path, index=False)
            print(
                "No new riders in the website thats why all previous riders remove from new_data_first_page.csv"
            )

    finally:
        print("QUIT WEB DRIVER ______________")
        if driver:
            driver.quit()



def sysInit():
    headers = get_random_headers()
    print(headers)

    # Set Chrome options
    options = Options()
    options.headless = True
    options.add_argument("--enable-logging")
    options.add_argument("--log-level=0")
    # options.add_argument('user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3')
    options.add_argument(f'user-agent={headers["User-Agent"]}')
    options.add_argument("--no-sandbox")
    scrap_first_page(options)