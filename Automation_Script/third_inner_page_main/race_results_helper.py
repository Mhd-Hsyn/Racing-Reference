"""
statics table
https://www.racing-reference.info/race-results/2021-02/Q/

"""

import csv
import re, os
import pandas as pd
import requests
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
import time
from bs4 import BeautifulSoup
from selenium.common.exceptions import NoSuchElementException
import Automation_Script.third_inner_page_main.helpers as hp


def extract_race_results_table_data(table):
    """
    1st Table

    This method for 1st and main method on this page whose link in mentioned on above
    in that table there are simples rows and columns
    the image and text both availabe in single td tag
        - (image link is fetch and text data fetch and seprate with coma)
    """
    skip_conditions = [
        "NASCAR® and its marks are trademarks",
        "Links for this race (some data may be unavailable):",
    ]

    # Skip the table if it contains any of the specific text
    if any(condition in table.text for condition in skip_conditions):
        return []

    table_data = []
    rows = table.find_all("tr")
    for row in rows:
        cells = row.find_all(["td", "th"])
        row_data = []

        for cell in cells:
            # Check if the cell contains an image (img tag)
            imgs = cell.find_all("img")
            a_tags = cell.find_all("a")

            # Check if there are multiple images and anchor tags
            if imgs and a_tags:
                cell_data = []
                for img, a_tag in zip(imgs, a_tags):
                    image_url = img.get("src").lstrip("//") if img and img.get("src") else ""
                    a_text = a_tag.text.strip() if a_tag and a_tag.text else ""
                    rider_name_img = [image_url, a_text]
                    cell_data.append(rider_name_img)

                row_data.append(cell_data)
            else:
                # Process the cell as before if there is only one image and one anchor tag or none
                img = imgs[0] if imgs else None
                a_tag = a_tags[0] if a_tags else None

                if img:
                    image_url = img.get("src").lstrip("//") if img and img.get("src") else ""
                    if a_tag:
                        a_text = a_tag.text.strip() if a_tag and a_tag.text else ""
                        rider_name_img = [image_url, a_text]
                        row_data.append(rider_name_img)
                    else:
                        row_data.append(image_url)
                else:
                    my_cell_text= cell.text.strip() if cell and cell.text else ""
                    row_data.append(my_cell_text)

        table_data.append(row_data)

    return table_data


# def extract_table_data(table):
#     """
#     2nd table
#         Caution flag breakdown:
#         Lap leader breakdown:

#     This method is for the below tables
#     ( heading plus + table data)
#     in this table The table heading is mentioned in the table in td tag with class name "newhead"
#     fetch table name seprately and render the table and and dataframe of the table
#     """

#     skip_conditions = [
#         "NASCAR® and its marks are trademarks",
#         "Links for this race (some data may be unavailable):",
#     ]

#     # Skip the table if it contains any of the specific text
#     if any(condition in table.text for condition in skip_conditions):
#         return []

#     table_data = []
#     rows = table.find_all("tr")

#     # Check if the table has a heading row with class "newhead"
#     heading_row = rows[0]
#     heading_cell = heading_row.find("td", class_="newhead")
#     heading = heading_cell.text.strip() if heading_cell else ""
#     if heading:
#         for row in rows[1:]:
#             cells = row.find_all(["td", "th"])
#             row_data = []

#             for cell in cells:
#                 # Check if the cell contains an image (img tag)
#                 img = cell.find("img")
#                 text_content = cell.text.strip() if cell else ""
#                 if img:
#                     # If an image is present, fetch the URL from the 'src' attribute
#                     image_url = img.get("src").lstrip("//")
#                     row_data.append(f"{image_url}, {text_content}")
#                 else:
#                     # If no image, fetch the text content
#                     row_data.append(text_content)

#             table_data.append(row_data)

#     other_table_data = pd.DataFrame(table_data)
#     other_table_data.reset_index(drop=True, inplace=True)

#     return heading, other_table_data

def extract_table_data(table):
    """
    2nd table
        Caution flag breakdown:
        Lap leader breakdown:

    This method is for the below tables
    ( heading plus + table data)
    in this table The table heading is mentioned in the table in td tag with class name "newhead"
    fetch table name seprately and render the table and and dataframe of the table
    """

    skip_conditions = [
        "NASCAR® and its marks are trademarks",
        "Links for this race (some data may be unavailable):",
    ]

    # Skip the table if it contains any of the specific text
    if any(condition in table.text for condition in skip_conditions):
        return []

    table_data = []
    other_table_data = []
    rows = table.find_all("tr")

    # Check if the table has a heading row with class "newhead"
    heading_row = rows[0]
    heading_cell = heading_row.find("td", class_="newhead")
    heading = heading_cell.text.strip() if heading_cell and heading_cell.text else ""
    if heading:
        for row in rows[1:]:
            cells = row.find_all(["td", "th"])
            row_data = []
            if cells:
                for cell in cells:
                    # Check if the cell contains an image (img tag)
                    img = cell.find("img")
                    text_content = cell.text.strip() if cell and cell.text else ""
                    if img:
                        # If an image is present, fetch the URL from the 'src' attribute
                        image_url = img.get("src").lstrip("//") if img and img.get("src") else ""
                        row_data.append(f"{image_url}, {text_content}")
                    else:
                        # If no image, fetch the text content
                        row_data.append(text_content)

                table_data.append(row_data)
            other_table_data = pd.DataFrame(table_data)
            other_table_data.reset_index(drop=True, inplace=True)

    else:
        table_data = []
        heading_cell = heading_row.find("th", class_="newhead")
        heading = heading_cell.text.strip() if heading_cell and heading_cell.text else ""
        if heading:
            heading= heading.replace(":","")
            row_data = []
            table_data.append(heading)
            for row in rows:
                cells = row.find_all("td")
                if cells:
                  for cell in cells:
                      # Check if the cell contains an image (img tag)
                      text_content = cell.text.strip() if cell and cell.text else ""
                      print(text_content)
                      row_data.append(text_content)
                  table_data.extend(row_data)

            other_table_data = pd.DataFrame(table_data)
            other_table_data.reset_index(drop=True, inplace=True)
            heading = "Fastest Lap" if heading == "Race notes" else heading

    print("\nHEADING IS ", heading)
    print("\n DF is\n", other_table_data)
    return heading, other_table_data



"""
race results table

2020_24_Hours_of_Le_Mans_Pos

https://www.racing-reference.info/race-results/2021-02/Q/

"""


def save_races_results(html_content, driver_name, table_name, race_name):
    """
    This is main method, the html content and driver name and race name are requiredfield
    these helps to make the seprate folder for each and every deivers with his perticular table name and race name

    in this method 2 child method is calling to handle 2 seprate senarios of the table on that page
    and making seprate folder and seprate csv after getting dataframe from both methods
    """

    table_name = hp.remove_regix(table_name)
    race_name = hp.remove_regix(race_name)

    current_directory = os.getcwd()
    mydirectory = f"{current_directory}/Automation_Script/third_inner_page_main/Races/"
    my_newdata_directory = (
        f"{current_directory}/Automation_Script/third_inner_page_main/new_Races/"
    )

    driver_name = driver_name.replace(".", "")

    #  for record in Races Folder
    driver_folder_path = os.path.join(mydirectory, f"{driver_name}")
    hp.make_new_folder(driver_folder_path, folder_name=driver_name)

    table_folder_path = os.path.join(f"{mydirectory}/{driver_name}", f"{table_name}")
    hp.make_new_folder(table_folder_path, folder_name=table_name)

    race_folder_path = os.path.join(
        f"{mydirectory}/{driver_name}/{table_name}", f"{race_name}"
    )
    hp.make_new_folder(race_folder_path, folder_name=table_name)

    #  For new record in new_Races Folder
    new_races_folder_path = os.path.join(my_newdata_directory, f"{driver_name}")
    hp.make_new_folder(new_races_folder_path, folder_name=driver_name)

    new_races_folder_path = os.path.join(
        f"{my_newdata_directory}/{driver_name}", f"{table_name}"
    )
    hp.make_new_folder(new_races_folder_path, folder_name=table_name)

    new_races_folder_path = os.path.join(
        f"{my_newdata_directory}/{driver_name}/{table_name}", f"{race_name}"
    )
    hp.make_new_folder(new_races_folder_path, folder_name=race_name)

    soup = BeautifulSoup(html_content, "html.parser")

    # Extract tables with class "tb race-results-tbl"
    race_results_table = soup.find("table", class_="tb race-results-tbl")
    unuse_table = soup.find_all("table", cellpadding=["4", "5"])
    # Extract tables with class "tb"
    other_tables = soup.find_all("table", class_="tb")
    other_tables = [
        table
        for table in other_tables
        if table not in (race_results_table, *unuse_table)
    ]

    # Create an empty DataFrame
    df = pd.DataFrame()

    if race_results_table:
        # table called for main table
        race_results_data = extract_race_results_table_data(race_results_table)
        df = pd.DataFrame(race_results_data)
        # print("__________  tb race-results-tbl ___________")
        # print(df)

        # For Record in Race Folder "mydirectory"
        df.to_csv(
            f"{mydirectory}/{driver_name}/{table_name}/{race_name}/{race_name}.csv",
            index=False,
        )
        print(f"\n\trace file save in folder {mydirectory} {race_name} successfully")

        # For Record in new_Race Folder     "my_newdata_directory"
        df.to_csv(
            f"{my_newdata_directory}/{driver_name}/{table_name}/{race_name}/{race_name}.csv",
            index=False,
        )
        print(
            f"\n\trace file save in folder  {my_newdata_directory} {race_name} successfully"
        )

    # Extract and append data from the other tables to the DataFrame
    for table in other_tables:
        result = extract_table_data(table)
        if result:
            heading, other_table_data = result

            heading = hp.remove_regix(heading)

            # For Record in Race Folder "mydirectory"
            other_table_data.to_csv(
                f"{mydirectory}/{driver_name}/{table_name}/{race_name}/{heading}.csv",
                index=False,
            )
            print(
                f"race file {heading} save in folder {mydirectory} {race_name} successfully"
            )

            # For Record in new_Race Folder     "my_newdata_directory"
            other_table_data.to_csv(
                f"{my_newdata_directory}/{driver_name}/{table_name}/{race_name}/{heading}.csv",
                index=False,
            )
            print(
                f"race file {heading} save in folder {my_newdata_directory} {race_name} successfully"
            )
