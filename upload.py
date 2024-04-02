import requests

payload = {}
files = {}
headers = {}
host = "localhost"


def upload_driver_data():
    url = f"http://{host}:9005/api/upload-drivers-data/"
    response = requests.request("POST", url, headers=headers, data=payload, files=files)
    print(response.json())


def upload_driver_table_page_1():
    url = f"http://{host}:9005/upload_driver_table_page_1"
    response = requests.request("POST", url, headers=headers, data=payload, files=files)
    print(response.json())


def upload_driver_table_page_2_races():
    url = f"http://{host}:9005/api/upload_driver_table_page_2/"
    response = requests.request("POST", url, headers=headers, data=payload, files=files)
    print(response.json())


def upload_driver_table_page_2_race():
    url = f"http://{host}:9005/api/upload_driver_table_page_2_race/"
    response = requests.request("POST", url, headers=headers, data=payload, files=files)
    print(response.json())


def upload_series_data():
    url = f"http://{host}:9005/upload-csv/"
    response = requests.request("POST", url, headers=headers, data=payload, files=files)
    print(response.json())


upload_driver_data_status = upload_driver_data()
print("upload_driver_data_status", upload_driver_data_status)
if upload_driver_data_status:
    print("upload drivers data successfully")
    upload_driver_table_page_1_status = upload_driver_table_page_1()
    print("upload_driver_table_page_1_status", upload_driver_table_page_1_status)
    if upload_driver_table_page_1_status:
        print("upload drivers data page 1 successfully")
        upload_driver_table_page_2_races_status = upload_driver_table_page_2_races()
        print(
            "upload_driver_table_page_2_races_status",
            upload_driver_table_page_2_races_status,
        )
        if upload_driver_table_page_2_races_status:
            print("upload drivers data races page 2 successfully")
            upload_series_data_status = upload_series_data()
            print("upload_series_data_status", upload_series_data_status)
            if upload_series_data_status:
                print("upload series successfully")
                upload_driver_table_page_2_race_status = (
                    upload_driver_table_page_2_race()
                )
                print(
                    "upload_driver_table_page_2_race_status",
                    upload_driver_table_page_2_race_status,
                )
                if upload_driver_table_page_2_race_status:
                    print("upload drivers data race page 2 successfully")
                    print("all uploaded successfully")
