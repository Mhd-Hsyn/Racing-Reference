# import os

# folder_path = 'drives_data/'  # Replace with your actual folder path

# # List all folders in the specified path
# folders = os.listdir(folder_path)

# for folder in folders:
#     # Replace underscores with spaces and add a space at the beginning
#     new_name = '  ' + folder.replace('_', ' ')

#     # Create the full path for the old and new names
#     old_path = os.path.join(folder_path, folder)
#     new_path = os.path.join(folder_path, new_name)

#     # Rename the folder
#     os.rename(old_path, new_path)
# from pymongo import MongoClient

# # Connect to MongoDB
# client = MongoClient("mongodb://localhost:27017/")

# # Select the database and collection
# db = client["racing-reference"]
# collection_name = "test"
# collection = db[collection_name]

# # Drop the collection
# collection.drop()

# # Close the MongoDB connection
# client.close()

# from pymongo import MongoClient

# # Connect to MongoDB
# client = MongoClient("mongodb://localhost:27017/")

# # Select the database
# db = client["racing-reference"]

# # List all collections in the database
# collections = db.list_collection_names()

# # Drop each collection
# for collection in collections:
#     db[collection].drop()

# print("Folders renamed successfully.")
# import os

# folder_path = 'drives_data (3)/'  # Replace with your actual folder path

# # List all folders in the specified path
# folders = os.listdir(folder_path)

# for folder in folders:
#     # Add an additional space before the existing name
#     new_name = ' ' + folder

#     # Create the full path for the old and new names
#     old_path = os.path.join(folder_path, folder)
#     new_path = os.path.join(folder_path, new_name)

#     # Rename the folder
#     os.rename(old_path, new_path)

# print("Folders renamed successfully.")

# import os

# folder_path = 'drives_data/'  # Aap apne folders ke path ko ismein daalain

# # List all folders in the specified path
# folders = os.listdir(folder_path)

# for folder in folders:
#     # Convert folder name to lowercase
#     new_name = folder

#     # Create the full path for the old and new names
#     old_path = os.path.join(folder_path, folder)
#     new_path = os.path.join(folder_path, new_name)

#     # Rename the folder
#     os.rename(old_path, new_path)

# print("Folders renamed to lowercase successfully.")

# import requests

# # Replace 'your_uuid_here' with the actual UUID you want to retrieve information for
# uuid_to_get = 'f30e632c-ef7f-4709-b972-8c1cda353fb5'
# url = f'http://localhost:8000/get-rider-data/{uuid_to_get}'

# response = requests.get(url)

# if response.status_code == 200:
#     rider_data = response.json()
#     print(f"Rider Name: {rider_data['rider_name']}")
#     print("CSV Data:")
#     for row in rider_data['csv_data']:
#         print(row)
# else:
#     print(f"Failed to retrieve rider data. Status code: {response.status_code}, Detail: {response.text}")

# import os
# import pandas as pd

# def convert_xlsx_to_csv(input_folder):
#     # Loop through each file in the input folder
#     for file_name in os.listdir(input_folder):
#         if file_name.endswith(".xlsx"):
#             # Construct the input and output file paths
#             input_file_path = os.path.join(input_folder, file_name)
#             output_file_path = os.path.join(input_folder, os.path.splitext(file_name)[0] + ".csv")

#             # Read the Excel file and save as CSV, overwriting the original file
#             df = pd.read_excel(input_file_path, engine="openpyxl")
#             df.to_csv(output_file_path, index=False)

#             # Remove the original Excel file
#             os.remove(input_file_path)

#             print(f"Conversion and overwrite complete: {file_name}")

# if __name__ == "__main__":
#     # Replace this path with your actual folder path
#     folder_path = "Races/Scott_Drnek/Exhibition_or_Unclassified_race_results/2012_Rolex_Monterey_Motorsports_Reunion"

#     convert_xlsx_to_csv(folder_path)

# Import Rider_Country_Img
# import pandas as pd

# # CSV file ka path
# csv_path = 'MY_ALL_3_Tables_folder/1st_page_a_to_z/merged_all.csv'

# # CSV file ko pandas DataFrame mein load karein
# df = pd.read_csv(csv_path)

# # "Rider_Country_Img" field se unique values extract karein
# unique_links = df['Rider_Country_Img'].unique()

# # Flags.txt file ko write mode mein open karein
# with open('flags.txt', 'w') as flags_file:
#     # Unique links ko flags.txt mein likhein
#     for link in unique_links:
#         flags_file.write(link + '\n')

# print("Flags.txt file has been generated.")
# Country flags dictionary ko print karein
# import json
#
# country_flags = {
# 'finland': 'www.racing-reference.info/wp-content/themes/ndms/images/racing-reference/flags/finland.png',
# 'usa': 'www.racing-reference.info/wp-content/themes/ndms/images/racing-reference/flags/usa.png',
# 'norway': 'www.racing-reference.info/wp-content/themes/ndms/images/racing-reference/flags/norway.png',
# 'spain': 'www.racing-reference.info/wp-content/themes/ndms/images/racing-reference/flags/spain.png',
# 'mexico': 'www.racing-reference.info/wp-content/themes/ndms/images/racing-reference/flags/mexico.png',
# 'italy': 'www.racing-reference.info/wp-content/themes/ndms/images/racing-reference/flags/italy.png',
# 'germany': 'www.racing-reference.info/wp-content/themes/ndms/images/racing-reference/flags/germany.png',
# 'australia': 'www.racing-reference.info/wp-content/themes/ndms/images/racing-reference/flags/australia.png',
# 'japan': 'www.racing-reference.info/wp-content/themes/ndms/images/racing-reference/flags/japan.png',
# 'uk': 'www.racing-reference.info/wp-content/themes/ndms/images/racing-reference/flags/uk.png',
# 'canada': 'www.racing-reference.info/wp-content/themes/ndms/images/racing-reference/flags/canada.png',
# 'brazil': 'www.racing-reference.info/wp-content/themes/ndms/images/racing-reference/flags/brazil.png',
# 'france': 'www.racing-reference.info/wp-content/themes/ndms/images/racing-reference/flags/france.png',
# 'belgium': 'www.racing-reference.info/wp-content/themes/ndms/images/racing-reference/flags/belgium.png',
# 'switzerland': 'www.racing-reference.info/wp-content/themes/ndms/images/racing-reference/flags/switzerland.png',
# 'russia': 'www.racing-reference.info/wp-content/themes/ndms/images/racing-reference/flags/russia.png',
# 'portugal': 'www.racing-reference.info/wp-content/themes/ndms/images/racing-reference/flags/portugal.png',
# 'argentina': 'www.racing-reference.info/wp-content/themes/ndms/images/racing-reference/flags/argentina.png',
# 'algeria': 'www.racing-reference.info/wp-content/themes/ndms/images/racing-reference/flags/algeria.png',
# 'syria': 'www.racing-reference.info/wp-content/themes/ndms/images/racing-reference/flags/syria.png',
# 'saudi_arabia': 'www.racing-reference.info/wp-content/themes/ndms/images/racing-reference/flags/saudi_arabia.png',
# 'uae': 'www.racing-reference.info/wp-content/themes/ndms/images/racing-reference/flags/uae.png',
# 'bahrain': 'www.racing-reference.info/wp-content/themes/ndms/images/racing-reference/flags/bahrain.png',
# 'colombia': 'www.racing-reference.info/wp-content/themes/ndms/images/racing-reference/flags/colombia.png',
# 'netherlands': 'www.racing-reference.info/wp-content/themes/ndms/images/racing-reference/flags/netherlands.png',
# 'austria': 'www.racing-reference.info/wp-content/themes/ndms/images/racing-reference/flags/austria.png',
# 'thailand': 'www.racing-reference.info/wp-content/themes/ndms/images/racing-reference/flags/thailand.png',
# 'none': 'www.racing-reference.info/wp-content/themes/ndms/images/racing-reference/flags/none.png',
# 'pakistan': 'www.racing-reference.info/wp-content/themes/ndms/images/racing-reference/flags/pakistan.png',
# 'indonesia': 'www.racing-reference.info/wp-content/themes/ndms/images/racing-reference/flags/indonesia.png',
# 'puerto_rico': 'www.racing-reference.info/wp-content/themes/ndms/images/racing-reference/flags/puerto_rico.png',
# 'new_zealand': 'www.racing-reference.info/wp-content/themes/ndms/images/racing-reference/flags/new_zealand.png',
# 'sweden': 'www.racing-reference.info/wp-content/themes/ndms/images/racing-reference/flags/sweden.png',
# 'denmark': 'www.racing-reference.info/wp-content/themes/ndms/images/racing-reference/flags/denmark.png',
# 'paraguay': 'www.racing-reference.info/wp-content/themes/ndms/images/racing-reference/flags/paraguay.png',
# 'singapore': 'www.racing-reference.info/wp-content/themes/ndms/images/racing-reference/flags/singapore.png',
# 'scotland': 'www.racing-reference.info/wp-content/themes/ndms/images/racing-reference/flags/scotland.png',
# 'venezuela': 'www.racing-reference.info/wp-content/themes/ndms/images/racing-reference/flags/venezuela.png',
# '': 'www.racing-reference.info/wp-content/themes/ndms/images/racing-reference/flags/.png',
# 'estonia': 'www.racing-reference.info/wp-content/themes/ndms/images/racing-reference/flags/estonia.png',
# 'south_africa': 'www.racing-reference.info/wp-content/themes/ndms/images/racing-reference/flags/south_africa.png',
# 'egypt': 'www.racing-reference.info/wp-content/themes/ndms/images/racing-reference/flags/egypt.png',
# 'greece': 'www.racing-reference.info/wp-content/themes/ndms/images/racing-reference/flags/greece.png',
# 'philippines': 'www.racing-reference.info/wp-content/themes/ndms/images/racing-reference/flags/philippines.png',
# 'hungary': 'www.racing-reference.info/wp-content/themes/ndms/images/racing-reference/flags/hungary.png',
# 'monaco': 'www.racing-reference.info/wp-content/themes/ndms/images/racing-reference/flags/monaco.png',
# 'lebanon': 'www.racing-reference.info/wp-content/themes/ndms/images/racing-reference/flags/lebanon.png',
# 'hong_kong': 'www.racing-reference.info/wp-content/themes/ndms/images/racing-reference/flags/hong_kong.png',
# 'ireland': 'www.racing-reference.info/wp-content/themes/ndms/images/racing-reference/flags/ireland.png',
# 'india': 'www.racing-reference.info/wp-content/themes/ndms/images/racing-reference/flags/india.png',
# 'uruguay': 'www.racing-reference.info/wp-content/themes/ndms/images/racing-reference/flags/uruguay.png',
# 'latvia': 'www.racing-reference.info/wp-content/themes/ndms/images/racing-reference/flags/latvia.png',
# 'dominican_republic': 'www.racing-reference.info/wp-content/themes/ndms/images/racing-reference/flags/dominican_republic.png',
# 'china': 'www.racing-reference.info/wp-content/themes/ndms/images/racing-reference/flags/china.png',
# 'chile': 'www.racing-reference.info/wp-content/themes/ndms/images/racing-reference/flags/chile.png',
# 'guatemala': 'www.racing-reference.info/wp-content/themes/ndms/images/racing-reference/flags/guatemala.png',
# 'macau': 'www.racing-reference.info/wp-content/themes/ndms/images/racing-reference/flags/macau.png',
# 'czechoslovakia': 'www.racing-reference.info/wp-content/themes/ndms/images/racing-reference/flags/czechoslovakia.png',
# 'taiwan': 'www.racing-reference.info/wp-content/themes/ndms/images/racing-reference/flags/taiwan.png',
# 'ecuador': 'www.racing-reference.info/wp-content/themes/ndms/images/racing-reference/flags/ecuador.png',
# 'south_korea': 'www.racing-reference.info/wp-content/themes/ndms/images/racing-reference/flags/south_korea.png',
# 'peru': 'www.racing-reference.info/wp-content/themes/ndms/images/racing-reference/flags/peru.png',
# 'morocco': 'www.racing-reference.info/wp-content/themes/ndms/images/racing-reference/flags/morocco.png',
# 'virgin_islands': 'www.racing-reference.info/wp-content/themes/ndms/images/racing-reference/flags/virgin_islands.png',
# 'israel': 'www.racing-reference.info/wp-content/themes/ndms/images/racing-reference/flags/israel.png',
# 'angola': 'www.racing-reference.info/wp-content/themes/ndms/images/racing-reference/flags/angola.png',
# 'slovenia': 'www.racing-reference.info/wp-content/themes/ndms/images/racing-reference/flags/slovenia.png',
# 'slovakia': 'www.racing-reference.info/wp-content/themes/ndms/images/racing-reference/flags/slovakia.png',
# 'poland': 'www.racing-reference.info/wp-content/themes/ndms/images/racing-reference/flags/poland.png',
# 'malaysia': 'www.racing-reference.info/wp-content/themes/ndms/images/racing-reference/flags/malaysia.png',
# 'luxembourg': 'www.racing-reference.info/wp-content/themes/ndms/images/racing-reference/flags/luxembourg.png',
# 'romania': 'www.racing-reference.info/wp-content/themes/ndms/images/racing-reference/flags/romania.png',
# 'bahamas': 'www.racing-reference.info/wp-content/themes/ndms/images/racing-reference/flags/bahamas.png',
# 'rhodesia': 'www.racing-reference.info/wp-content/themes/ndms/images/racing-reference/flags/rhodesia.png',
# 'benin': 'www.racing-reference.info/wp-content/themes/ndms/images/racing-reference/flags/benin.png',
# 'zimbabwe': 'www.racing-reference.info/wp-content/themes/ndms/images/racing-reference/flags/zimbabwe.png',
# 'papua': 'www.racing-reference.info/wp-content/themes/ndms/images/racing-reference/flags/papua.png',
# 'iran': 'www.racing-reference.info/wp-content/themes/ndms/images/racing-reference/flags/iran.png',
# 'croatia': 'www.racing-reference.info/wp-content/themes/ndms/images/racing-reference/flags/croatia.png',
# 'bulgaria': 'www.racing-reference.info/wp-content/themes/ndms/images/racing-reference/flags/bulgaria.png',
# 'san_marino': 'www.racing-reference.info/wp-content/themes/ndms/images/racing-reference/flags/san_marino.png',
# 'isle_of_man': 'www.racing-reference.info/wp-content/themes/ndms/images/racing-reference/flags/isle_of_man.png',
# 'sri_lanka': 'www.racing-reference.info/wp-content/themes/ndms/images/racing-reference/flags/sri_lanka.png',
# 'honduras': 'www.racing-reference.info/wp-content/themes/ndms/images/racing-reference/flags/honduras.png',
# 'el_salvador': 'www.racing-reference.info/wp-content/themes/ndms/images/racing-reference/flags/el_salvador.png',
# 'serbia': 'www.racing-reference.info/wp-content/themes/ndms/images/racing-reference/flags/serbia.png',
# 'costa_rica': 'www.racing-reference.info/wp-content/themes/ndms/images/racing-reference/flags/costa_rica.png',
# 'tanzania': 'www.racing-reference.info/wp-content/themes/ndms/images/racing-reference/flags/tanzania.png',
# 'liechtenstein': 'www.racing-reference.info/wp-content/themes/ndms/images/racing-reference/flags/liechtenstein.png',
# 'barbados': 'www.racing-reference.info/wp-content/themes/ndms/images/racing-reference/flags/barbados.png',
# 'panama': 'www.racing-reference.info/wp-content/themes/ndms/images/racing-reference/flags/panama.png',
# 'lithuania': 'www.racing-reference.info/wp-content/themes/ndms/images/racing-reference/flags/lithuania.png',
# 'turkey': 'www.racing-reference.info/wp-content/themes/ndms/images/racing-reference/flags/turkey.png'
# }
#
# Save country flags dictionary to a file in the same format as printed
# with open('country_flags.json', 'w') as json_file:
# json.dump(country_flags, json_file, indent=4)
#
# print("Country flags data has been saved to country_flags.json.")
#
from pymongo import MongoClient

# Replace the connection string and database name with your actual values
client = MongoClient(
    "mongodb+srv://usman:shoaibbilal@fastapi.momn8bp.mongodb.net/?retryWrites=true&w=majority"
)
database_name = "racing-reference"
collection_name_to_drop = "driver_table_page_1_test"

# Access the specified database
db = client[database_name]

# Drop the specified collection
db[collection_name_to_drop].drop()

# Close the MongoDB connection
client.close()
