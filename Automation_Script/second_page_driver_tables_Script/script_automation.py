"""

first_page_a_to_z/new_data_first_page.csv  sy ek ek kar k driver k name utha k, us k url me ja k tables fetch kr raha h

drivers_data/ k folder andr b record save kr raha h new_riders k tables ka 
ar new_drivers_data/ k folder sy previous data delete kr k isi (new_drivers_data/) folder me new rider ka data save kr raha h

jo  ( first_page_a_to_z/new_data_first_page.csv )  me new riders h 

"""

import csv, os, re, shutil, time
import pandas as pd
from selenium import webdriver
from selenium.webdriver.support.ui import WebDriverWait
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from bs4 import BeautifulSoup
from selenium.webdriver.chrome.options import Options
from fake_useragent import UserAgent


def get_random_headers():
    ua = UserAgent()
    headers = {"User-Agent": ua.random, "Accept-Language": "en-US, en;q=0.5"}
    return headers


def delete_contents_of_directory(directory_path):
    try:
        # Iterate over all files and subdirectories in the given directory
        for item in os.listdir(directory_path):
            item_path = os.path.join(directory_path, item)
            # Check if it's a file, then delete it
            if os.path.isfile(item_path):
                os.unlink(item_path)
            # If it's a directory, recursively delete its contents
            elif os.path.isdir(item_path):
                shutil.rmtree(item_path)
        print(f"All files and folders in {directory_path} have been deleted.")
    except Exception as e:
        print(f"Error: {e}")


def scrap_second_page(options):
    driver = None
    try:
        print(
            "\n\n\n\t ******  Starting 2nd page Driver Race and Races Table Script........ ***** \n\n"
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

        time.sleep(3)

        ##############################################################################3

        count = 1
        current_directory = os.getcwd()

        # for delete old records and data in folder
        newdriverdata_folder_path = (
            "Automation_Script/second_page_driver_tables_Script/new_drivers_data"
        )
        delete_contents_of_directory(newdriverdata_folder_path)

        # Specify the relative path to your CSV file
        relative_path = "Automation_Script/first_page_a_to_z/new_data_first_page.csv"
        csv_file_path = os.path.join(current_directory, relative_path)

        # Read the CSV file
        with open(csv_file_path, "r", encoding="utf-8") as csv_file:
            reader = csv.DictReader(csv_file)
            for csv_row in reader:
                driver_url = csv_row["Link"]
                driver_name = csv_row["Driver"]
                print(
                    "___________      ********   DRIVER  *********           _______________"
                )
                print(f"{count+1}  {driver_name}")
                print(driver_url)

                # driver_name = "A.J. ALLMENDINGER"
                driver_name = driver_name.lstrip()
                driver_name = driver_name.replace(" ", "_")
                driver_name = driver_name.replace(",", "")
                driver_name = driver_name.replace(".", "_")

                driver_uid = csv_row["UUID"]
                driver_uid = driver_uid.lstrip()
                driver_uid = driver_uid.replace("-", "")
                driver_name = f"{driver_uid}___{driver_name}"
                print(driver_name)

                time.sleep(5)

                current_directory = os.getcwd()
                new_folder_path = os.path.join(
                    current_directory,
                    f"Automation_Script/second_page_driver_tables_Script/drives_data/{driver_name}",
                )
                new_rec_folder_path = os.path.join(
                    current_directory,
                    f"Automation_Script/second_page_driver_tables_Script/new_drivers_data/{driver_name}",
                )

                # Create the folder
                try:
                    os.makedirs(new_folder_path)
                except FileExistsError:
                    pass
                try:
                    os.makedirs(new_rec_folder_path)
                except FileExistsError:
                    pass

                retry = True
                while retry:
                    driver.get(driver_url)
                    html_content = driver.page_source

                    if html_content:
                        soup = BeautifulSoup(html_content, "html.parser")
                        # Extracting table data
                        parent_table = soup.find("table", class_="statsTbl")
                        if parent_table:
                            retry = False
                        else :
                            print(f"\n\n\n************** RETRY THE {driver_name} ________ {driver_url}")
                            continue
                        child_tables = parent_table.find_all("table") if parent_table else []
                        h1_list = [
                            child_table.find("h1").text.strip()
                            for child_table in child_tables
                            if child_table.find("h1") and child_tables
                        ]

                        tables = soup.find_all("table", class_="tb")
                        if tables:
                            table_counter = 1
                            for i, table in enumerate(tables):
                                rows = table.find_all("tr")
                                headers = [th.text.strip() for th in rows[0].find_all("th")]
                                data = []

                                for row in rows[1:]:
                                    values = [td.text.strip() for td in row.find_all("td")]
                                    # Check if the row has class "tot"
                                    if "tot" in row.get("class", []):
                                        modified_values = [values[0], ""] + values[1:]
                                    else:
                                        modified_values = values
                                    data.append(dict(zip(headers, modified_values)))

                                if headers and data:
                                    # Create a DataFrame for the current table
                                    df = pd.DataFrame(data)

                                    # Save the current table's data to a separate CSV file
                                    h1_value = (
                                        h1_list[table_counter - 1]
                                        if table_counter <= len(h1_list)
                                        else f"Table{table_counter}"
                                    )
                                    h1_value = re.sub(r"[^a-zA-Z0-9]", "_", h1_value)
                                    table_counter += 1
                                    output_file_path = f"Automation_Script/second_page_driver_tables_Script/drives_data/{driver_name}/{driver_name}___TABLE_NAME___{h1_value}.csv"
                                    new_data_output_file_path = f"Automation_Script/second_page_driver_tables_Script/new_drivers_data/{driver_name}/{driver_name}___TABLE_NAME___{h1_value}.csv"

                                    df.to_csv(output_file_path, index=False)
                                    df.to_csv(new_data_output_file_path, index=False)
                                else:
                                    pass
                            count += 1
                        else:
                            pass
                    else:
                        print("______________________problem ______________________")
                        print(f"Skipping {driver_name} due to failure in fetching HTML")
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
    scrap_second_page(options)