RIDER_NAMES_PROCESSED = False


import pandas as pd
import csv
import os
import numpy as np
from pymongo import MongoClient
import uuid

from baseurl import get_mongo_connection, get_database

db = get_mongo_connection()

rider_collection = db["rider_races"]
riders_collections = db["driver_table_page_2"]

RIDER_NAMES_PROCESSED = False


def handle_nan_inf(value):
    """
    Handle NaN (Not a Number) and infinity values.

    Args:
        value: The value to be checked.

    Returns:
        None: If the value is NaN, positive infinity, or negative infinity.
        value: Otherwise, the original value is returned.
    """
    if pd.isna(value) or value == np.inf or value == -np.inf:
        return None
    return value


def clean_csv_data(csv_data, csv_file_name):
    """
    Clean CSV data by handling NaN and Infinity values.

    Args:
        csv_data (list): List of dictionaries representing CSV data.
        csv_file_name (str): Name of the CSV file.

    Returns:
        list: List of cleaned dictionaries with NaN and Infinity values handled.
    """
    cleaned_data = []
    # Remove letters before the first occurrence of '_' and remove '.csv' from the end
    modified_file_name = csv_file_name.split("___", 2)[-1].rsplit(".", 1)[0]
    for row in csv_data:
        cleaned_row = {"table_name": modified_file_name}
        for key, value in row.items():
            cleaned_row[key] = handle_nan_inf(value)
        cleaned_data.append(cleaned_row)
    return cleaned_data


def process_rider_race_names(db, rider_info_list):
    global RIDER_NAMES_PROCESSED
    response_data = []  # Initialize with an empty list

    if not RIDER_NAMES_PROCESSED:
        FOLDER_PATH = (
            "MY_ALL_3_Tables_folder/merged/3rd_inner_page_Statics_Script/Races/"
        )

        for name in os.listdir(FOLDER_PATH):
            rider_folder = os.path.join(FOLDER_PATH, name)
            if os.path.isdir(rider_folder):
                rider_name = name.split("___")[0]
                matching_rider_info = [
                    info
                    for info in rider_info_list
                    if info.get("UUID").replace("-", "") == rider_name
                ]

                if matching_rider_info:
                    rider_data = {
                        "uuid": matching_rider_info[0].get("UUID"),
                        "rider_name": matching_rider_info[0].get("Driver", ""),
                        "tables": {},
                    }

                    for rider_info in matching_rider_info:
                        cleaned_csv_data = clean_csv_data(
                            rider_info["CSVData"], rider_info["CSVFileName"]
                        )
                        table_name = cleaned_csv_data[0]["table_name"].split("___")[1]

                        for root, dirs, files in os.walk(rider_folder):
                            files = [file for file in files if file.endswith(".csv")]

                            for file in files:
                                try:
                                    formatted_file_name = file.replace(
                                        "_", " "
                                    ).replace(".csv", "")
                                    matching_race_entry = next(
                                        (
                                            entry
                                            for entry in cleaned_csv_data
                                            if entry.get("Race") == formatted_file_name
                                        ),
                                        None,
                                    )

                                    if matching_race_entry:
                                        race_uuid = matching_race_entry.get("race_uuid")
                                        folder_name = file.split(".")[0]
                                        csv_file_path = os.path.join(
                                            rider_folder, table_name, folder_name, file
                                        )

                                        if os.path.exists(csv_file_path):
                                            with open(
                                                csv_file_path, newline=""
                                            ) as csvfile:
                                                next(csvfile)
                                                csv_data_list = list(
                                                    csv.DictReader(csvfile)
                                                )

                                                # Generate a new UUID for each entry in csv_data_list
                                                for entry in csv_data_list:
                                                    entry["3rdpageuuid"] = str(
                                                        uuid.uuid4()
                                                    )

                                                update_result = (
                                                    riders_collections.insert_one(
                                                        {
                                                            "uuid": rider_data["uuid"],
                                                            "csv_data": csv_data_list,
                                                            "race_uuid": race_uuid,
                                                        }
                                                    )
                                                )

                                                response_data.append(
                                                    {
                                                        "rider_name": name,
                                                        "csv_file": formatted_file_name,
                                                        "race_uuid": race_uuid,
                                                        "csv_data": csv_data_list,
                                                    }
                                                )

                                except ValueError:
                                    print(f"Invalid year format in CSV File: {file}")

                    RIDER_NAMES_PROCESSED = True

    return response_data

rider_page3 = db["driver_table_page_2"]
def automation_process_rider_race_names(db, rider_info_list):
    print("\n\n\t ********** automation_process_rider **********\n\n\n")
    print("\n\n\t ********** rider_info_list **********\n\n\n", rider_info_list)

    global RIDER_NAMES_PROCESSED
    response_data = []  # New list to store response information

    if not RIDER_NAMES_PROCESSED:
        FOLDER_PATH = "Automation_Script/third_inner_page_main/new_Races/"

        for name in os.listdir(FOLDER_PATH):
            rider_folder = os.path.join(FOLDER_PATH, name)
            if os.path.isdir(rider_folder):
                rider_name = name.split("___")[0]
                matching_rider_info = [
                    info
                    for info in rider_info_list
                    if info.get("UUID").replace("-", "") == rider_name
                ]

                if matching_rider_info:
                    rider_data = {
                        "uuid": matching_rider_info[0].get("UUID"),
                        "rider_name": matching_rider_info[0].get("Driver", ""),
                        "tables": {},
                    }

                    for rider_info in matching_rider_info:
                        cleaned_csv_data = clean_csv_data(
                            rider_info["CSVData"], rider_info["CSVFileName"]
                        )
                        table_name = cleaned_csv_data[0]["table_name"].split("___")[1]

                        for root, dirs, files in os.walk(rider_folder):
                            files = [file for file in files if file.endswith(".csv")]

                            for file in files:
                                try:
                                    formatted_file_name = file.replace(
                                        "_", " "
                                    ).replace(".csv", "")
                                    matching_race_entry = next(
                                        (
                                            entry
                                            for entry in cleaned_csv_data
                                            if entry.get("Race") == formatted_file_name
                                        ),
                                        None,
                                    )

                                    if matching_race_entry:
                                        race_uuid = matching_race_entry.get("race_uuid")
                                        folder_name = file.split(".")[0]
                                        csv_file_path = os.path.join(
                                            rider_folder, table_name, folder_name, file
                                        )

                                        if os.path.exists(csv_file_path):
                                            with open(
                                                csv_file_path, newline=""
                                            ) as csvfile:
                                                next(csvfile)
                                                csv_data_list = list(
                                                    csv.DictReader(csvfile)
                                                )
                                                update_result = (
                                                    rider_page3.insert_one(
                                                        {
                                                            "uuid": rider_data["uuid"],
                                                            "csv_data": csv_data_list,
                                                            "race_uuid": race_uuid,
                                                        }
                                                    )
                                                )

                                                response_data.append(
                                                    {
                                                        "rider_name": name,
                                                        "csv_file": formatted_file_name,
                                                        "race_uuid": race_uuid,
                                                        "csv_data": csv_data_list,
                                                    }
                                                )

                                except ValueError:
                                    print(f"Invalid year format in CSV File: {file}")

                RIDER_NAMES_PROCESSED = True

    return response_data
