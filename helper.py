import pandas as pd
import csv
import os
import numpy as np
from pymongo import MongoClient
import re
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


def process_rider_names_year(db, rider_info_list):
    global RIDER_NAMES_PROCESSED
    response_data = []  # New list to store response information

    if not RIDER_NAMES_PROCESSED:
        FOLDER_PATH = (
            "MY_ALL_3_Tables_folder/merged/3rd_inner_page_Statics_Script/Races/"
        )

        for name in os.listdir(FOLDER_PATH):
            rider_folder = os.path.join(FOLDER_PATH, name)
            if os.path.isdir(rider_folder):
                rider_name = name.split("___")[0]
                matching_rider_info = [
                    rider_info
                    for rider_info in rider_info_list
                    if rider_info.get("UUID").replace("-", "") == rider_name
                ]

                if matching_rider_info:
                    rider_data = {
                        "uuid": matching_rider_info[0].get("UUID"),
                        "rider_name": matching_rider_info[0].get("Driver", ""),
                        "tables": {},
                    }

                    for rider_info in matching_rider_info:
                        csv_file_name = rider_info.get("CSVFileName", "")
                        csv_data = rider_info.get("CSVData", [])

                        # Clean CSV data and table name
                        cleaned_csv_data = clean_csv_data(csv_data, csv_file_name)
                        table_name = cleaned_csv_data[0]["table_name"]

                        rider_data["tables"][table_name] = cleaned_csv_data

                    # Traverse subdirectories and compare table names with subfolder names
                    for root, dirs, files in os.walk(rider_folder):
                        # Filter out directories, only process files
                        files = [file for file in files if file.endswith(".csv")]
                        for file in files:
                            try:
                                # Check if the file name contains 'year' or 'years'
                                if "year" in file.lower() or "years" in file.lower():
                                    slice_length = 7
                                else:
                                    continue

                                # Extract the slice from the file name, remove trailing underscores, and replace spaces
                                sliced_file_name = (
                                    file[:slice_length]
                                    .replace("_", " ")
                                    .rstrip()
                                    .replace("_", " ")
                                )

                                # Print processing information
                                print(f"Processing CSV file: {file}")
                                print("Sliced file name:", sliced_file_name)

                                # Print potential year and CSV data for debugging
                                # print("Potential Year:", potential_year)
                                print("CSV Data:")
                                for entry in csv_data:
                                    print(entry)

                                matching_entry = next(
                                    (
                                        entry
                                        for entry in csv_data
                                        if entry.get("Year") == sliced_file_name
                                    ),
                                    None,
                                )

                                if matching_entry:
                                    races_uuid = matching_entry.get("races_uuid")

                                    # Construct the full path to the CSV file
                                    table_name = cleaned_csv_data[0][
                                        "table_name"
                                    ].split("___")[1]
                                    folder_name = file.split(".")[0]
                                    csv_file_path = os.path.join(
                                        rider_folder, table_name, folder_name, file
                                    )

                                    # Check if the file exists before trying to open it
                                    if os.path.exists(csv_file_path):
                                        with open(csv_file_path, newline="") as csvfile:
                                            csv_reader = csv.DictReader(csvfile)
                                            csv_data_list = list(csv_reader)

                                            # Print sliced CSV Data
                                            print(
                                                f"Sliced CSV Data for {file}: {sliced_file_name}"
                                            )
                                            print(csv_data_list)

                                            # Store the CSV data in the MongoDB database
                                            riders_collections = db["testing"]
                                            update_result = (
                                                riders_collections.insert_one(
                                                    {
                                                        "uuid": rider_data["uuid"],
                                                        "csv_data": csv_data_list,
                                                        "races_uuid": races_uuid,
                                                    }
                                                )
                                            )

                                            # Append response information to the list
                                            response_data.append(
                                                {
                                                    "rider_name": rider_name,
                                                    "csv_file": file,
                                                    "races_uuid": races_uuid,
                                                    "csv_data": csv_data_list,
                                                }
                                            )
                                    else:
                                        print(f"CSV File not found: {csv_file_path}")

                                else:
                                    print(
                                        f"No matching races found for {rider_name}, CSV File: {file}"
                                    )
                            except ValueError:
                                print(f"Invalid year format in CSV File: {file}")

                else:
                    print(f"No matching rider info found for {rider_name}")

        RIDER_NAMES_PROCESSED = True

    return response_data  # Return the response_data at the end of the function


import uuid


def process_rider_names(db, rider_info_list):
    global RIDER_NAMES_PROCESSED
    response_data = []  # New list to store response information

    if not RIDER_NAMES_PROCESSED:
        FOLDER_PATH = (
            "MY_ALL_3_Tables_folder/merged/3rd_inner_page_Statics_Script/Races/"
        )

        for name in os.listdir(FOLDER_PATH):
            rider_folder = os.path.join(FOLDER_PATH, name)

            if os.path.isdir(rider_folder):
                rider_name = name.split("___")[0]
                matching_rider_info = [
                    rider_info
                    for rider_info in rider_info_list
                    if rider_info.get("UUID").replace("-", "") == rider_name
                ]

                if matching_rider_info:
                    rider_data = {
                        "uuid": matching_rider_info[0].get("UUID"),
                        "rider_name": matching_rider_info[0].get("Driver", ""),
                        "tables": {},
                    }

                    for rider_info in matching_rider_info:
                        csv_file_name = rider_info.get("CSVFileName", "")
                        csv_data = rider_info.get("CSVData", [])

                        # Clean CSV data and table name
                        cleaned_csv_data = clean_csv_data(csv_data, csv_file_name)
                        table_name = cleaned_csv_data[0]["table_name"]

                        rider_data["tables"][table_name] = cleaned_csv_data

                    # Traverse subdirectories and compare table names with subfolder names
                    for root, dirs, files in os.walk(rider_folder):
                        # Filter out directories, only process files
                        files = [file for file in files if file.endswith(".csv")]
                        for file in files:
                            try:
                                # Assuming the year is the first four words
                                if "years" not in file:
                                    # Extract the first four words from the file name (excluding the extension)
                                    potential_year = "_".join(file.split("_")[:4])[:4]
                                    # Check if the potential year matches any entry in CSV data
                                    matching_entry = next(
                                        (
                                            entry
                                            for entry in csv_data
                                            if entry.get("Year") == potential_year
                                        ),
                                        None,
                                    )
                                    if matching_entry:
                                        races_uuid = matching_entry.get("races_uuid")
                                        year_uuid = matching_entry.get("year_uuid")

                                        # Construct the full path to the CSV file
                                        table_name = cleaned_csv_data[0][
                                            "table_name"
                                        ].split("___")[1]
                                        folder_name = file.split(".")[0]
                                        csv_file_path = os.path.join(
                                            rider_folder, table_name, folder_name, file
                                        )
                                        print("csv_file_path--------", csv_file_path)
                                        # Check if the file exists before trying to open it
                                        if os.path.exists(csv_file_path):
                                            with open(
                                                csv_file_path, newline=""
                                            ) as csvfile:
                                                csv_reader = csv.DictReader(csvfile)
                                                csv_data_list = list(csv_reader)

                                                # Generate a new UUID for each entry in csv_data_list
                                                for entry in csv_data_list:
                                                    entry["3rdpageuuid"] = str(
                                                        uuid.uuid4()
                                                    )

                                                # Store the CSV data in the MongoDB database
                                                riders_collections = db[
                                                    "driver_table_page_2"
                                                ]
                                                update_result = (
                                                    riders_collections.insert_one(
                                                        {
                                                            "uuid": rider_data["uuid"],
                                                            "csv_data": csv_data_list,
                                                            "races_uuid": races_uuid,
                                                        }
                                                    )
                                                )

                                                # Append response information to the list
                                                response_data.append(
                                                    {
                                                        "rider_name": name,
                                                        "csv_file": file,
                                                        "races_uuid": races_uuid,
                                                        "csv_data": csv_data_list,
                                                    }
                                                )
                                        else:
                                            print(
                                                f"CSV File not found: {csv_file_path}"
                                            )

                                    else:
                                        print(
                                            f"No matching races found for {name}, CSV File: {file}"
                                        )
                                else:
                                    print(
                                        f"Ignoring CSV with 'years' in the name: {file}"
                                    )
                            except ValueError:
                                print(f"Invalid year format in CSV File: {file}")
                else:
                    print(f"No matching rider info found for {name}")

        RIDER_NAMES_PROCESSED = True

    return response_data  # Return the response_data at the end of the function

rider_page3 = db["driver_table_page_2"]

def automation_process_rider_names(db, rider_info_list):
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
                    rider_info
                    for rider_info in rider_info_list
                    if rider_info.get("UUID").replace("-", "") == rider_name
                ]

                if matching_rider_info:
                    rider_data = {
                        "uuid": matching_rider_info[0].get("UUID"),
                        "rider_name": matching_rider_info[0].get("Driver", ""),
                        "tables": {},
                    }

                    for rider_info in matching_rider_info:
                        csv_file_name = rider_info.get("CSVFileName", "")
                        csv_data = rider_info.get("CSVData", [])

                        # Clean CSV data and table name
                        cleaned_csv_data = clean_csv_data(csv_data, csv_file_name)
                        table_name = cleaned_csv_data[0]["table_name"]

                        rider_data["tables"][table_name] = cleaned_csv_data

                    # Traverse subdirectories and compare table names with subfolder names
                    for root, dirs, files in os.walk(rider_folder):
                        # Filter out directories, only process files
                        files = [file for file in files if file.endswith(".csv")]
                        for file in files:
                            try:
                                # Assuming the year is the first four words
                                if "years" not in file:
                                    # Extract the first four words from the file name (excluding the extension)
                                    potential_year = "_".join(file.split("_")[:4])[:4]
                                    # Check if the potential year matches any entry in CSV data
                                    matching_entry = next(
                                        (
                                            entry
                                            for entry in csv_data
                                            if entry.get("Year") == potential_year
                                        ),
                                        None,
                                    )
                                    print("Processing rider:", rider_name)
                                    print("Matching rider info:", matching_rider_info)
                                    print("CSV file:", file)

                                    if matching_entry:
                                        races_uuid = matching_entry.get("races_uuid")
                                        year_uuid = matching_entry.get("year_uuid")

                                        # Construct the full path to the CSV file
                                        table_name = cleaned_csv_data[0][
                                            "table_name"
                                        ].split("___")[1]
                                        folder_name = file.split(".")[0]
                                        csv_file_path = os.path.join(
                                            rider_folder, table_name, folder_name, file
                                        )
                                        print("csv_file_path--------", csv_file_path)
                                        # Check if the file exists before trying to open it
                                        if os.path.exists(csv_file_path):
                                            with open(
                                                csv_file_path, newline=""
                                            ) as csvfile:
                                                csv_reader = csv.DictReader(csvfile)
                                                csv_data_list = list(csv_reader)

                                                # Store the CSV data in the MongoDB database
                                                update_result = (
                                                    rider_page3.insert_one(
                                                        {
                                                            "uuid": rider_data["uuid"],
                                                            "csv_data": csv_data_list,
                                                            "races_uuid": races_uuid,
                                                        }
                                                    )
                                                )

                                                # Append response information to the list
                                                response_data.append(
                                                    {
                                                        "rider_name": name,
                                                        "csv_file": file,
                                                        "races_uuid": races_uuid,
                                                        "csv_data": csv_data_list,
                                                    }
                                                )
                                        else:
                                            print(
                                                f"CSV File not found: {csv_file_path}"
                                            )

                                    else:
                                        print(
                                            f"No matching races found for {name}, CSV File: {file}"
                                        )
                                else:
                                    print(
                                        f"Ignoring CSV with 'years' in the name: {file}"
                                    )
                            except ValueError:
                                print(f"Invalid year format in CSV File: {file}")
                else:
                    print(f"No matching rider info found for {name}")

        RIDER_NAMES_PROCESSED = True

    return response_data
