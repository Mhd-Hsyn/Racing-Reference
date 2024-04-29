"""
AUTOMATION IN DB

Accept cookies as well as Dynamic handling every rider 
    - read csv where all riders save ( one by one )
    - Go to the rider page and and save all links for inner page
    - Go one by one innerpage and scrap data and make folders and csvs according to the senarios 
"""

import csv, time, re, os
from selenium import webdriver
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from bs4 import BeautifulSoup
from .race_results_helper import save_races_results
from .race_statics_helpers import save_race_statics
from selenium.webdriver.chrome.options import Options
from fake_useragent import UserAgent
from webdriver_manager.chrome import ChromeDriverManager
import Automation_Script.third_inner_page_main.helpers as hp


def get_random_headers():
    ua = UserAgent()
    headers = {"User-Agent": ua.random, "Accept-Language": "en-US, en;q=0.5"}
    return headers

def get_chromedrvier_options():
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
    return options

def scrap_third_page():
    driver = None
    try:
        table_number = 0
        ##############################################################################

        count = 1
        current_directory = os.getcwd()
        my_newdata_directory = (
            f"{current_directory}/Automation_Script/third_inner_page_main/new_Races/"
        )
        # DELETE all previous folders _______ For new record in new_Races Folder
        hp.delete_contents_of_directory(my_newdata_directory)

        # Specify the relative path to your CSV file
        relative_path = "Automation_Script/first_page_a_to_z/new_data_first_page.csv"
        csv_file_path = os.path.join(current_directory, relative_path)

        # Read the CSV file
        with open(csv_file_path, "r", encoding="utf-8") as csv_file:
            reader = csv.DictReader(csv_file)
            for csv_row in reader:
                # Fetch the driver's name dynamically
                driver_url = csv_row["Link"]
                print(driver_url)

                driver_name = csv_row["Driver"]
                print(
                    "\n\n\n\n___________      ********   DRIVER  *********           _______________"
                )
                print(f"{count+1}  {driver_name}")

                driver_name = driver_name.lstrip()
                driver_name = driver_name.replace(" ", "_")
                driver_name = driver_name.replace(",", "")
                driver_name = driver_name.replace(".", "_")

                driver_uid = csv_row["UUID"]
                driver_uid = driver_uid.lstrip()
                driver_uid = driver_uid.replace("-", "")
                driver_name = f"{driver_uid}___{driver_name}"

                print(driver_name)
                parent_table= None
                retry = True
                while retry:
                    try:
                        options = get_chromedrvier_options()
                        driver = webdriver.Chrome(ChromeDriverManager().install(), options=options)
                        driver.get(driver_url)
                        max_wait_time = 100
                        try:
                            btn=  WebDriverWait(driver, max_wait_time).until(EC.presence_of_element_located((By.ID, "onetrust-accept-btn-handler")))
                            btn.click()
                            # btn1=  WebDriverWait(driver, max_wait_time).until(EC.presence_of_element_located((By.ID, "button1")))
                            # btn1.click()
                            driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
                            time.sleep(2)  # Wait for 2 seconds after scrolling
                            driver.execute_script("window.scrollTo(0, -document.body.scrollHeight);")
                            time.sleep(2)  # Wait for 2 seconds after scrolling
                        except Exception as e:
                            print(e)
                        WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.CLASS_NAME, "statsTbl")))
                        time.sleep(5)
                        
                        html_content = driver.page_source

                        if html_content:
                            soup = BeautifulSoup(html_content, "html.parser")
                            # Extracting table Name
                            parent_table = soup.find("table", class_="statsTbl")
                            if parent_table:
                                retry = False
                            else :
                                retry = True
                                print(f"\n\n\n************** RETRY THE {driver_name} ________ {driver_url}")
                    except:
                        print("\n\n****************** EXCEPTION IN RIDER **************\n\n")
                        retry = True
                    finally:
                        print("\n\nQUIT WEB DRIVER ______________ ")
                        if driver:
                            driver.quit()
                if parent_table:
                    child_tables = parent_table.find_all("table") if parent_table else []
                    h1_list = [
                        child_table.find("h1").text.strip()
                        for child_table in child_tables
                        if child_table.find("h1") and child_tables
                    ]
                    tables = soup.find_all("table", class_="tb")
                    if not tables:
                        print("No Tables")

                    race_links_list = []
                    races_links_list = []

                    # Initialize an empty DataFrame
                    table_counter = 1
                    for i, table in enumerate(tables):
                        rows = table.find_all("tr")
                        header_row = rows[0].find_all("th")

                        # Dynamically find the positions of 'Race' and 'Races' columns
                        race_index = None
                        races_index = None
                        for idx, header in enumerate(header_row):
                            if "Races" in header.text:
                                races_index = idx
                            elif "Race" in header.text:
                                race_index = idx

                        if race_index is not None or races_index is not None:
                            table_number = table_number + 1
                            for row_idx, row in enumerate(rows[1:]):
                                values = [td.text.strip() for td in row.find_all("td")]

                                if (
                                    races_index is not None
                                    and row_idx == len(rows[1:]) - 1
                                ):
                                    races_index -= 1

                                # Dynamically find the positions of 'Race' and 'Races' columns for each row
                                race_link = (
                                    row.find_all("td")[race_index].find("a")["href"]
                                    if race_index is not None
                                    else None
                                )
                                races_link = (
                                    row.find_all("td")[races_index].find("a")["href"]
                                    if races_index is not None
                                    else None
                                )

                                if race_link and race_link.startswith("/race-results/"):
                                    race_link_info = {
                                        "Race Link": None,
                                        "Table Name": f"{table_counter}",
                                        "Race Name": None,
                                    }
                                    full_race_link = (
                                        f"https://www.racing-reference.info{race_link}"
                                    )
                                    # race_id = race_link.split('/')[-2]  # Extracting the race ID from the link
                                    race_name = values[0].replace(" ", "_")
                                    race_link_info["Race Link"] = full_race_link
                                    race_link_info["Race Name"] = race_name

                                    h1_value = (
                                        h1_list[table_counter - 1]
                                        if table_counter <= len(h1_list)
                                        else f"Table{table_counter}"
                                    )
                                    h1_value = re.sub(r"[^a-zA-Z0-9]", "_", h1_value)
                                    race_link_info["Table Name"] = h1_value
                                    race_links_list.append(race_link_info)

                                elif race_link:
                                    race_link_info = {
                                        "Race Link": None,
                                        "Table Name": f"{table_counter}",
                                        "Race Name": None,
                                    }
                                    race_name = f"{values[0]}_{values[2]}".replace(
                                        " ", "_"
                                    )
                                    race_link_info["Race Link"] = race_link
                                    race_link_info["Race Name"] = race_name

                                    h1_value = (
                                        h1_list[table_counter - 1]
                                        if table_counter <= len(h1_list)
                                        else f"Table{table_counter}"
                                    )
                                    h1_value = re.sub(r"[^a-zA-Z0-9]", "_", h1_value)
                                    race_link_info["Table Name"] = h1_value
                                    races_links_list.append(race_link_info)

                                if races_link:
                                    races_link_info = {
                                        "Race Link": None,
                                        "Table Name": f"{table_counter}",
                                        "Race Name": None,
                                    }
                                    race_name = f"{values[0]}_{values[2]}".replace(
                                        " ", "_"
                                    )
                                    races_link_info["Race Link"] = races_link
                                    races_link_info["Race Name"] = race_name

                                    h1_value = (
                                        h1_list[table_counter - 1]
                                        if table_counter <= len(h1_list)
                                        else f"Table{table_counter}"
                                    )
                                    h1_value = re.sub(r"[^a-zA-Z0-9]", "_", h1_value)
                                    races_link_info["Table Name"] = h1_value
                                    races_links_list.append(races_link_info)

                            table_counter += 1

                        else:
                            continue

                    driver_name = driver_name.replace(".", "")

                    # 1 of 13
                    for link_info in races_links_list:
                        link = link_info["Race Link"]
                        race_name = link_info["Race Name"]
                        table_name = link_info["Table Name"]

                        # counter= 0
                        myretry= True
                        main_menu= None
                        # while myretry and counter < 100:
                        while myretry:
                            try:
                                options = get_chromedrvier_options()
                                driver = webdriver.Chrome(ChromeDriverManager().install(), options=options)
                                driver.get(link)
                                max_wait_time = 100
                                time.sleep(5)
                                try:
                                    btn=  WebDriverWait(driver, max_wait_time).until(EC.presence_of_element_located((By.ID, "onetrust-accept-btn-handler")))
                                    btn.click()
                                    time.sleep(10)
                                    driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
                                    time.sleep(2)  # Wait for 2 seconds after scrolling
                                    driver.execute_script("window.scrollTo(0, -document.body.scrollHeight);")
                                    time.sleep(2)  # Wait for 2 seconds after scrolling
                                except Exception as e:
                                    print(e)
                                html_content = driver.page_source
                                # print("\n\n ______ HTML _______________", html_content)
                                if html_content:
                                    soup = BeautifulSoup(html_content, "html.parser")
                                    main_menu = soup.find('ul',{"id":"menu-main-nav"})
                                    if main_menu:
                                        print(f"\n\n *********** Tables    FOUND *************")
                                        myretry= False
                                    if not main_menu:
                                        print("\n\n*********** No main_menu  IN RACE 1 of 13  Continue")
                                        myretry= True
                            except:
                                print("\n\n****************** EXCEPTION IN RACE 1 of 13 **************\n\n")
                                myretry= True
                            finally:
                                print("QUIT WEB DRIVER ______________")
                                if driver:
                                    driver.quit()
                        if main_menu:
                            save_race_statics(
                                html_content=html_content,
                                driver_name=driver_name,
                                table_name=table_name,
                                race_name=race_name,
                            )

        ###########################################################################

                    #  Table2_2020_24_Hours_of_Le_Mans_Pos
                    for link_info in race_links_list:
                        link = link_info["Race Link"]
                        # link = "https://www.racing-reference.info/race-results/2017-26/X/"
                        race_name = link_info["Race Name"]
                        table_name = link_info["Table Name"]

                        # counter= 0
                        main_menu= None
                        myretry= True
                        while myretry:
                            try:
                                options = get_chromedrvier_options()
                                driver = webdriver.Chrome(ChromeDriverManager().install(), options=options)
                                driver.get(link)
                                time.sleep(5)
                                max_wait_time = 100
                                try:
                                    btn=  WebDriverWait(driver, max_wait_time).until(EC.presence_of_element_located((By.ID, "onetrust-accept-btn-handler")))
                                    btn.click()
                                    time.sleep(10)
                                    driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
                                    time.sleep(2)  # Wait for 2 seconds after scrolling
                                    driver.execute_script("window.scrollTo(0, -document.body.scrollHeight);")
                                    time.sleep(2)  # Wait for 2 seconds after scrolling
                                except Exception as e:
                                    print(e)
                                html_content = driver.page_source
                                # print("\n\n ______ HTML _______________", html_content)
                                if html_content:
                                    soup = BeautifulSoup(html_content, "html.parser")
                                    main_menu = soup.find('ul',{"id":"menu-main-nav"})
                                    if main_menu:
                                        print(f"\n\n *********** Tables    FOUND *************")
                                        myretry= False
                                    if not main_menu:
                                        # time.sleep(2)
                                        myretry= True
                                        print(f"\n\n *********** No Tables    Continue")
                                        # counter +=1
                            except:
                                print("\n\n****************** EXCEPTION IN RACE `````EXHIBITION````` **************\n\n")
                                # time.sleep(50)
                                myretry= True
                            finally:
                                print("QUIT WEB DRIVER ______________")
                                if driver:
                                    driver.quit()
                        if main_menu:
                            save_races_results(
                                    driver_name=driver_name,
                                    html_content=html_content,
                                    race_name=race_name,
                                    table_name=table_name,
                                )

                    count += 1
    except Exception as e :
        print(e)



def sysInit():
    scrap_third_page()
