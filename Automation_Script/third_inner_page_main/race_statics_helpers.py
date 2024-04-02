"""
Races with horizontal and vertical columns
completed and working 

Race Statics 

1 of 13

e.g
https://www.racing-reference.info/driver-season-stats/adamjo01/2016/TU/

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


def extract_stats_meta_info(html_content):
    """
    This method is to fetch the additional data from above link
    and return in dataframe
    example :
            Win:0 ( 0.0%)       Average start:6.0       Total Winnings:$0
            Podium:0 ( 0.0%)   Average finish:6.0       (excluding bonuses)
            DNF:0
    """
    soup = BeautifulSoup(html_content, "html.parser")
    additional_table = soup.find("table", class_="statsMetaInfo")

    if additional_table:
        # Extract data from div class="row1"
        row1_data = [
            item.text.strip() for item in additional_table.select(".row1 span")
        ]

        # Extract data from div class="row2" to end
        row_data = {}
        for div in additional_table.select('.statsMetaInfo div[class^="row"]'):
            label_span = div.find("span", class_="label")
            value_span = div.find("span", class_="value")

            if label_span and value_span:
                label_text = label_span.text.strip().replace(
                    ":", ""
                )  # Remove colons from column names
                value_text = value_span.text.strip()
                row_data[label_text] = value_text

        # Extract specific values not covered by span class
        average_start = soup.select_one(
            '.row2 span.label:contains("Average start:") + span.value'
        ).text.strip()
        average_finish = soup.select_one(
            '.row3 span.label:contains("Average finish:") + span.value'
        ).text.strip()
        total_winnings = soup.select_one(
            '.row2 span.label:contains("Total Winnings:") + span.value'
        ).text.strip()

        # Check if the element exists before accessing its text attribute
        excluding_bonuses_elem = soup.select_one(
            '.row3 span.label:contains("(excluding bonuses)") + span.value'
        )
        excluding_bonuses = (
            excluding_bonuses_elem.text.strip() if excluding_bonuses_elem else None
        )

        # Add these values to the row_data dictionary
        row_data["Average start"] = average_start
        row_data["Average finish"] = average_finish
        row_data["Total Winnings"] = total_winnings
        row_data["(excluding bonuses)"] = excluding_bonuses

        # Create a DataFrame for row1_data
        df_row1 = pd.DataFrame([row1_data], columns=["Column 1", "Column 2"])

        # Create a DataFrame for row_data
        df_row2 = pd.DataFrame([row_data])
        # Concatenate the two DataFrames
        additional_df = pd.concat([df_row1, df_row2], axis=1)
        return additional_df
    return None


def save_race_statics(html_content, driver_name, table_name, race_name):
    """
    This method is help to fetch the tabes data and handling the colspan (html) condition for exact columns
    example in this link:-
        https://www.racing-reference.info/driver-season-stats/allmea.01/2006/W/


    This method also called the extract_stats_meta_info for fetching additional data
    and get dataframe from this method's response
    """

    # first making directories according to tables

    table_name = hp.remove_regix(table_name)
    race_name = hp.remove_regix(race_name)
    driver_name = driver_name.replace(".", "")

    current_directory = os.getcwd()
    mydirectory = f"{current_directory}/Automation_Script/third_inner_page_main/Races/"
    my_newdata_directory = (
        f"{current_directory}/Automation_Script/third_inner_page_main/new_Races/"
    )

    #  for record in Races Folder
    new_folder_path = os.path.join(mydirectory, f"{driver_name}")
    hp.make_new_folder(new_folder_path, folder_name=driver_name)

    new_folder_path = os.path.join(f"{mydirectory}/{driver_name}", f"{table_name}")
    hp.make_new_folder(new_folder_path, folder_name=table_name)

    new_folder_path = os.path.join(
        f"{mydirectory}/{driver_name}/{table_name}", f"{race_name}"
    )
    hp.make_new_folder(new_folder_path, folder_name=race_name)

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
    tables = soup.find_all("table", class_="tb")

    if not tables:
        print("No Tables")

    # Initialize an empty DataFrame for each iteration
    all_dfs = []

    for i, table in enumerate(tables):
        rows = table.find_all("tr")
        headers = [th.text.strip() for th in rows[0].find_all("th")]
        data = []

        for row in rows[1:]:
            values = []
            cells = row.find_all("td")
            for cell in cells:
                # Check if the cell has colspan="3"
                colspan = cell.get("colspan", "1")
                # Add the text content of the cell
                values.append(cell.text.strip())

                # Add empty values based on colspan
                for _ in range(int(colspan) - 1):
                    values.append("")

            data.append(dict(zip(headers, values)))

        # Create a DataFrame for the current table
        df = pd.DataFrame(data)

        # Add a prefix to the columns based on the table number and race name
        df.columns = [f"{col}" for col in df.columns]

        # Append the current table's DataFrame to the current DataFrame
        all_dfs.append(df)
    final_df = pd.concat(all_dfs, ignore_index=True)

    # For Record in Race Folder "mydirectory"
    final_df.to_csv(
        f"{mydirectory}/{driver_name}/{table_name}/{race_name}/{race_name}.csv",
        index=False,
    )
    print(
        f"\n\trace file save in folder {mydirectory} {driver_name}/{table_name}/{race_name} successfully {race_name}.csv"
    )

    # For Record in new_Race Folder     "my_newdata_directory"
    final_df.to_csv(
        f"{my_newdata_directory}/{driver_name}/{table_name}/{race_name}/{race_name}.csv",
        index=False,
    )
    print(
        f"\n\trace file save in folder  {my_newdata_directory} ____ {driver_name}/{table_name}/{race_name} successfully {race_name}.csv"
    )

    # Fetch the additional table
    additional_df = extract_stats_meta_info(html_content)
    if additional_df is not None and not additional_df.empty:
        # For Record in Race Folder "mydirectory"
        additional_df.to_csv(
            f"{mydirectory}/{driver_name}/{table_name}/{race_name}/Addtional_statics.csv",
            index=False,
        )
        print(
            f"\n\tAddtional data statics file save in folder {mydirectory} ____ {driver_name}/{table_name}/{race_name} successfully Additional_data.csv"
        )

        # For Record in new_Race Folder     "my_newdata_directory"
        additional_df.to_csv(
            f"{my_newdata_directory}/{driver_name}/{table_name}/{race_name}/Addtional_statics.csv",
            index=False,
        )
        print(
            f"\n\tAddtional data statics file save in folder {my_newdata_directory} ___ {driver_name}/{table_name}/{race_name} successfully Additional_data.csv"
        )
