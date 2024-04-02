from typing import List, Dict, Any, Optional
from bson import ObjectId
from pymongo import MongoClient
from ast import literal_eval
from uuid import uuid4
from pydantic import BaseModel
from passlib.hash import django_pbkdf2_sha256 as handler
from dotenv import load_dotenv
from json import JSONEncoder
from motor.motor_asyncio import AsyncIOMotorDatabase
from pymongo.collection import ReturnDocument
from datetime import datetime
from datetime import timedelta
from Automation_Script.Cups_Automation.script import scrape_cups
from Automation_Script.Owners_Automation.script import scrape_owners
from Automation_Script.Imsa_Automation.script import scrape_imsa_series
from Automation_Script.Indy_Car_Automation.script import scrape_indy_car
from Automation_Script.f1__Automation.script import scrape_f1_series
from Automation_Script.all_tracks.first_page import script as track_script
from fastapi import (
    FastAPI,
    File,
    UploadFile,
    HTTPException,
    Depends,
    status,
    Body,
    Path,
    Request,
    Query,
)
from fastapi.security import OAuth2PasswordBearer
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from rider_processing import (
    RIDER_NAMES_PROCESSED,
    process_rider_race_names,
    clean_csv_data,
    automation_process_rider_race_names,
)
from helper import (
    process_rider_names,
    process_rider_names_year,
    automation_process_rider_names,
)
import Automation_Script.xfinity.first_page.script as xfinity_script
import Automation_Script.trucks.first_page.script as trucks_script
import Automation_Script.arca.first_page.script as arca_script
import Automation_Script.modified.first_page.script as modified_script
import Automation_Script.first_page_a_to_z.script_automation as fist_page_script
import Automation_Script.second_page_driver_tables_Script.script_automation as second_page_script
import Automation_Script.third_inner_page_main.script_automation as third_page_script
import Automation_Script.all_tracks.second_page.script as track_script_page2
import time
import jwt
import json
import math
import logging
import uuid
import subprocess
import numpy as np
import pandas as pd
import os
import uvicorn
import ast
from selenium import webdriver
from bs4 import BeautifulSoup
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager
import schedule
import asyncio
from baseurl import get_mongo_connection, get_database

db = get_mongo_connection()
chrome_options = Options()
chrome_options.add_argument("--headless")

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

try:
    # Attempt to list collections
    collections = db.list_collection_names()

    if collections:
        print(f"Connection to MongoDB successful! Collections: {collections}")
    else:
        print("No collections found in the 'fastapi' database.")
except Exception as e:
    print(f"Failed to connect to MongoDB. Error: {e}")

user_collection = db["user"]
driver_collection = db["drivers"]
rider_collection = db["rider_races"]
additional_data = db["additional_data"]
admin_collection = db["admin"]
additional_data = db["additional_data"]
riders_collections = db["driver_table_page_2"]
cup_collection = db["cups"]
owner_collection = db["owners"]
Imsa_collection = db["Imsa"]
Indy_Car_collection = db["Indy_Car"]
xfinity_firstpage_collection = db["xfinity_page_1"]
track_firstpage_collection = db["track_page_1"]
trucks_firstpage_collection = db["trucks_page_1"]
f1_series_collection = db["f1"]
araca_firstpage_collection = db["araca_page_1"]
modified_firstpage_collection = db["modified_page_1"]
owner_statistics_collection = db["owner_statistics"]

RIDER_NAMES_PROCESSED = False
processed_rider_data = []

oauth2_scheme = OAuth2PasswordBearer(tokenUrl="token")


class User(BaseModel):
    username: str
    password: str


class AdditionalData(BaseModel):
    SERIESKEY: str
    SERIES: str


class UserInDB(User):
    _id: ObjectId


class DriverInfo(BaseModel):
    Driver: str
    Born: str
    Died: str
    Home: str
    Active: str


class OwnerData(BaseModel):
    Owner: str


class CupData(BaseModel):
    Year: str
    Races: str
    Champion: str
    Car_Owner: List[str]
    Champions_Crew_Chief: List[str]
    Rookie_of_the_Year: str
    Most_Popular_Driver: str
    Manufacturers_Championship: str


class ImsaData(BaseModel):
    Year: int
    Races: str
    Prototype_GTP: List[str]
    Prototype_Challenge_LMP2: List[str]
    LMP3: List[str]
    GT_LeMans_GTD_Pro: List[str]
    GT_Daytona: List[str]


class IndyCar(BaseModel):
    Year: str
    Races: str
    Champion: List[str]
    Rookie_of_the_Year: List[str]
    Most_Popular_Driver: List[str]


class f1_Series_Data(BaseModel):
    Year: str
    Races: str
    Driver_Champion: str
    Driver_Points: str
    Constructor_Champion: str
    Constructor_Points: str
    Engine: str


class XfinityData(BaseModel):
    Year: Optional[str] = None
    Races: Optional[str] = None
    Champion: Optional[str] = None
    Car_Owner: Optional[str] = None
    Champions_Crew_Chief: Optional[str] = None
    Rookie_of_the_Year: Optional[str] = None
    Most_Popular_Driver: Optional[str] = None
    Manufacturers_Championship: Optional[str] = None
    champion_status: Optional[str] = None


class TruckData(BaseModel):
    Year: str
    Races: str
    Champion: str
    Truck_Owner: str
    Champions_Crew_Chief: str
    Rookie_of_the_Year: str
    Most_Popular_Driver: str
    Manufacturers_Championship: str


class ModifiedData(BaseModel):
    Year: str
    Champion: str
    Car_Owner: str
    Rookie_of_the_Year: str
    Most_Popular_Driver: str


class ArcaData(BaseModel):
    Year: str
    Races: str
    Champion: str
    Rookie_of_the_Year: str


class TracksData(BaseModel):
    Track: List[str]
    Location: str
    Country: str
    First_race: str
    Last_race: str
    Races: str


def get_current_user(token: str = Depends(oauth2_scheme)):
    user = decode_token(token)
    if user is None:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid token"
        )
    return user


def decode_token(token: str):
    return {"username": "testuser"}


def serialize_record(record):
    serialized_record = {}
    for key, value in record.items():
        if isinstance(value, ObjectId):
            serialized_record[key] = str(value)
        elif isinstance(value, datetime):
            serialized_record[key] = value.strftime("%Y-%m-%d %H:%M:%S")
        elif isinstance(value, float):
            # Handle NaN values by converting them to None
            serialized_record[key] = value if value == value else None
        else:
            serialized_record[key] = value
    return serialized_record


@app.post("/api/signup/", response_model=UserInDB)
async def signup(user: User):
    existing_user = user_collection.find_one({"username": user.username})
    if existing_user:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Username already taken",
        )

    user_dict = user.dict()
    user_id = user_collection.insert_one(user_dict).inserted_id

    return {"_id": user_id, **user.dict()}


@app.post("/api/login/", response_model=UserInDB)
async def login(user: User):
    existing_user = user_collection.find_one(
        {"username": user.username, "password": user.password}
    )
    if not existing_user:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid credentials",
        )

    return existing_user


@app.get("/api/users/me/", response_model=UserInDB)
async def read_users_me(current_user: User = Depends(get_current_user)):
    return current_user


def add_created_at(records):
    current_time = datetime.utcnow()
    for record in records:
        record["created_at"] = current_time
    return records


def process_uploaded_file(file_path: str):
    try:
        df = pd.read_csv(file_path)
        data = df.to_dict(orient="records")

        data_with_created_at = add_created_at(data)

        driver_collection.insert_many(data_with_created_at)

        return JSONResponse(
            content={"message": "Driver data uploaded successfully"},
            status_code=200,
        )
    except Exception as e:
        raise HTTPException(
            status_code=500, detail=f"Error processing file: {str(e)}"
        ) from e


@app.post("/api/upload-drivers-data/")
async def create_upload_file():
    file_path = "MY_ALL_3_Tables_folder/1st_page_a_to_z/merged_all.csv"
    return process_uploaded_file(file_path)


class CustomJSONEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, ObjectId):
            return str(obj)
        return super().default(obj)


@app.get("/api/get-drivers-data-all/", response_class=JSONResponse)
async def get_drivers_data():
    records = list(driver_collection.find().sort([("_id", -1)]))
    records_json = json.dumps({"records": records}, cls=CustomJSONEncoder, indent=2)
    drivers_data = json.loads(records_json)
    return drivers_data


@app.get("/api/get_rider/{uuid}")
async def get_rider_by_uuid(uuid: str):
    try:
        records = driver_collection.find_one({"UUID": uuid})
        records_json = json.dumps({"records": records}, cls=CustomJSONEncoder, indent=2)
        drivers_data = json.loads(records_json)
        if drivers_data:
            return drivers_data
        raise HTTPException(status_code=404, detail="Rider not found")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Internal Server Error: {str(e)}")


FOLDER_PATH = "MY_ALL_3_Tables_folder/merged/2nd_page_driver_tables_Script/drives_data/"


@app.post("/upload_driver_table_page_1")
async def match_names():
    cleaned_data = list(db["drivers"].find())
    collection_data = [
        (record.get("Driver", ""), record.get("UUID", "")) for record in cleaned_data
    ]

    folders = os.listdir(FOLDER_PATH)
    folders = [name.split("___")[0] for name in folders]

    matching_names_uuid = [
        (driver_name, uuid)
        for driver_name, uuid in collection_data
        if uuid.replace("-", "") in folders
    ]

    for driver_name, UUID in matching_names_uuid:
        print(
            f"Found matching driver: '{driver_name}', Length: {len(driver_name)}, UUID: {UUID}"
        )

        my_UUID = UUID.replace("-", "")
        name = driver_name.replace(" ", "_").replace(".", "_").replace(",", "")
        folder_name = f"{my_UUID}___{name}"
        print("folder_name-------", folder_name)
        folder_to_open = os.path.join(FOLDER_PATH, folder_name)
        subprocess.run(["explorer", folder_to_open], shell=True)  # For Windows
        print("folder_to_open-------", folder_to_open)

        csv_files = [
            file for file in os.listdir(folder_to_open) if file.lower().endswith(".csv")
        ]

        for csv_file in csv_files:
            csv_path = os.path.join(folder_to_open, csv_file)
            df = pd.read_csv(csv_path)

            rider_collection = db["driver_table_page_1"]
            data_to_insert = {
                "UUID": UUID,
                "Driver": driver_name,
                "CSVFileName": csv_file,
                "CSVData": df.to_dict(orient="records"),
            }
            for i in range(len(data_to_insert["CSVData"])):
                if data_to_insert["CSVData"][i]:
                    if data_to_insert["CSVData"][i].get("Race"):
                        generated_uuid = str(uuid.uuid4())
                        data_to_insert["CSVData"][i]["race_uuid"] = generated_uuid

                    if data_to_insert["CSVData"][i].get("Races"):
                        generated_uuid1 = str(uuid.uuid4())
                        generated_uuid2 = str(uuid.uuid4())
                        data_to_insert["CSVData"][i]["year_uuid"] = generated_uuid1
                        data_to_insert["CSVData"][i]["races_uuid"] = generated_uuid2
                    else:
                        None

            rider_collection.insert_one(data_to_insert)

    print("csv_files----------------------", csv_files)

    return {"matching_names_uuid": matching_names_uuid, "csv_files": csv_files}


@app.get("/get-rider-data/{UUID}")
async def get_rider_data(UUID: str, db: MongoClient = Depends(get_database)):
    try:
        print(f"Requested UUID: {UUID}")

        rider_info_cursor = db["driver_table_page_1"].find({"UUID": UUID})

        rider_info_list = list(rider_info_cursor)
        print(f"Retrieved rider_info_list: {rider_info_list}")

        if rider_info_list:
            rider_data_list = []
            for rider_info in rider_info_list:
                rider_name = rider_info.get("Driver", "")
                csv_file_name = rider_info.get("CSVFileName", "")
                csv_data = rider_info.get("CSVData", [])
                cleaned_csv_data = clean_csv_data(csv_data, csv_file_name)

                table_name = cleaned_csv_data[0]["table_name"]

                cleaned_csv_data_no_table_name = [
                    {key: value for key, value in record.items() if key != "table_name"}
                    for record in cleaned_csv_data
                ]

                rider_data_list.append(
                    {
                        "rider_name": rider_name,
                        "table_name": table_name,
                        "csv_data": cleaned_csv_data_no_table_name,
                    }
                )

            return rider_data_list
        else:
            raise HTTPException(
                status_code=404, detail="Rider not found with the given UUID"
            )

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


def convert_to_json_compliant(value):
    if isinstance(value, float) and (
        value == float("inf") or value == float("-inf") or value != value
    ):
        return str(value)
    elif isinstance(value, ObjectId):
        return str(value)
    else:
        return value


@app.get("/api/get-drivers-data", response_class=JSONResponse)
async def get_driver_names(db: MongoClient = Depends(get_database)):
    try:
        driver_names_cursor = db["driver_table_page_1"].distinct("UUID")
        driver_names = [
            {"Driver": driver_name.strip()} for driver_name in driver_names_cursor
        ]

        detailed_driver_info = []
        for driver_info in driver_names:
            driver_name = driver_info["Driver"]
            print(f"Processing driver: {driver_name}")
            driver_details = db["drivers"].find_one({"UUID": driver_name})

            if driver_details:
                for key, value in driver_details.items():
                    driver_details[key] = convert_to_json_compliant(value)

                detailed_driver_info.append(driver_details)
                print(f"Details found for driver: {driver_name}")
            else:
                print(f"No details found for driver: {driver_name}")

        file_path = "MY_ALL_3_Tables_folder/1st_page_a_to_z/merged_all.csv"
        df = pd.read_csv(file_path)
        scrap_record = df["Driver"][:227]
        print("scrap_record-------", scrap_record)

        scrap_list = []

        for driver_name in scrap_record:
            for driver_info in detailed_driver_info:
                if driver_info["Driver"] == driver_name:
                    driver_info_cleaned = {
                        k: "" if v == "nan" else v for k, v in driver_info.items()
                    }
                    # Replace actual numpy.nan values with an empty string
                    driver_info_cleaned = {
                        k: "" if pd.isna(v) else v
                        for k, v in driver_info_cleaned.items()
                    }
                    scrap_list.append(driver_info_cleaned)
        records_json = json.dumps(
            {"records": scrap_list}, default=convert_to_json_compliant
        )
        drivers_data = json.loads(records_json)
        return drivers_data

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Internal Server Error: {str(e)}")


@app.get("/api/get-drivers-data", response_class=JSONResponse)
async def get_driver_names(db: MongoClient = Depends(get_database)):
    try:
        driver_names_cursor = db["driver_table_page_1"].distinct("UUID")
        driver_names = [
            {
                "Driver": driver_name.strip()  # Assuming you want to remove leading/trailing whitespaces
            }
            for driver_name in driver_names_cursor
        ]

        detailed_driver_info = []
        for driver_info in driver_names:
            driver_name = driver_info["Driver"]
            print(f"Processing driver: {driver_name}")
            driver_details = db["drivers"].find_one({"UUID": driver_name})

            if driver_details:
                for key, value in driver_details.items():
                    driver_details[key] = convert_to_json_compliant(value)

                detailed_driver_info.append(driver_details)
                print(f"Details found for driver: {driver_name}")
            else:
                print(f"No details found for driver: {driver_name}")

        records_json = json.dumps(
            {"records": detailed_driver_info},
            default=convert_to_json_compliant,
        )
        drivers_data = json.loads(records_json)
        print("-------------------", type(drivers_data))
        return drivers_data

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Internal Server Error: {str(e)}")


@app.post("/api/upload_driver_table_page_2_race/")
async def upload_rider_data(db: MongoClient = Depends(get_database)):
    try:
        rider_info_list = list(db["driver_table_page_1"].find())
        if not rider_info_list:
            raise HTTPException(status_code=404, detail="Rider not found")

        if not RIDER_NAMES_PROCESSED:
            response_data = process_rider_race_names(db, rider_info_list)
            print("response_data---------", response_data)
        else:
            response_data = []

        return response_data

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


RIDER_NAMES_PROCESSED = False
processed_rider_data = []


def create_rider_race_relationship(rider_uuid, races_uuid):
    riders_collections.update_one(
        {"uuid": rider_uuid}, {"$addToSet": {"races": races_uuid}}
    )


@app.post("/api/upload_driver_table_page_2/")
async def get_rider_data(db: MongoClient = Depends(get_database)):
    try:
        rider_info_cursor = db["driver_table_page_1"].find()

        rider_info_list = list(rider_info_cursor)
        if not rider_info_list:
            raise HTTPException(status_code=404, detail="Rider not found")

        global RIDER_NAMES_PROCESSED
        if not RIDER_NAMES_PROCESSED:
            response_data = process_rider_names(db, rider_info_list)
            RIDER_NAMES_PROCESSED = True

            process_rider_names_year(db, rider_info_list)

        return response_data

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/get-race-data/{race_uuid}")
async def get_race_data(race_uuid: str, db: MongoClient = Depends(get_database)):
    try:
        riders_collections = db["driver_table_page_2"]

        if not race_uuid:
            raise HTTPException(status_code=400, detail="Race UUID not provided")

        result = riders_collections.find_one(
            {"$or": [{"race_uuid": race_uuid}, {"races_uuid": race_uuid}]}
        )

        if not result:
            raise HTTPException(
                status_code=404,
                detail="Race data not found for the given race_uuid",
            )

        result["_id"] = str(result["_id"])

        if "csv_data" in result and isinstance(result["csv_data"], list):
            for item in result["csv_data"]:
                if "Driver" in item:
                    item["Driver"] = literal_eval(item["Driver"])

        return result

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/upload-csv/")
async def upload_csv():
    try:
        file_path = "MY_ALL_3_Tables_folder/LznPlO.csv"

        df = pd.read_csv(file_path)

        data_to_insert = df.to_dict(orient="records")

        result = additional_data.insert_many(data_to_insert)

        return JSONResponse(
            content={
                "message": f"CSV data uploaded successfully. {len(result.inserted_ids)} records inserted."
            },
            status_code=200,
        )

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An error occurred: {str(e)}")


class CustomsJSONEncoder(JSONEncoder):
    def default(self, obj):
        if isinstance(obj, ObjectId):
            return str(obj)
        return super().default(obj)


app.json_encoder = CustomsJSONEncoder


def convert_to_serializable(data):
    if isinstance(data, list):
        return [convert_to_serializable(item) for item in data]
    elif isinstance(data, dict):
        return {key: convert_to_serializable(value) for key, value in data.items()}
    elif isinstance(data, ObjectId):
        return str(data)
    else:
        return data


@app.get("/get-data/")
async def get_data():
    try:
        data = list(additional_data.find())

        data_serializable = convert_to_serializable(data)

        return JSONResponse(content=data_serializable)

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An error occurred: {str(e)}")


####################################################### ADMIN PANEL APIS ####################################################
class UserCreate(BaseModel):
    firstname: str
    lastname: str
    email: str
    password: str
    username: str


class UserResponse(BaseModel):
    firstname: str
    lastname: str
    email: str
    username: str


@app.post("/signup", response_model=UserResponse)
async def create_user(user: UserCreate):
    try:
        hashed_password = handler.using(rounds=80000).hash(user.password)

        user_dict = {
            "firstname": user.firstname,
            "lastname": user.lastname,
            "email": user.email,
            "password": hashed_password,
            "username": user.username,
        }

        print("User Dictionary:", user_dict)

        result = admin_collection.insert_one(user_dict)

        if result.inserted_id:
            return user

        raise HTTPException(status_code=500, detail="Error creating user")
    except Exception as e:
        print("Exception:", e)
        raise


load_dotenv()
user_jwt_token = os.getenv("User_jwt_token")
jwtkeys = {"user": user_jwt_token}


class LoginRequest(BaseModel):
    email: str
    password: str


class LoginResponse(BaseModel):
    status: bool
    message: str
    token: str
    user: UserResponse


async def generatedToken(fetchuser, authKey, totaldays, request: Request):
    try:
        access_token_payload = {
            "sub": str(fetchuser["_id"]),
            "email": fetchuser["email"],
            "exp": datetime.utcnow() + timedelta(days=totaldays),
            "iat": datetime.utcnow(),
        }

        access_token = jwt.encode(access_token_payload, authKey, algorithm="HS256")

        userpayload = {
            "id": str(fetchuser["_id"]),
            "email": fetchuser["email"],
        }
        return {"status": True, "token": access_token, "payload": userpayload}

    except Exception as e:
        return {
            "status": False,
            "message": "Something went wrong in token creation",
            "details": str(e),
        }


@app.post("/login", response_model=LoginResponse)
async def login(request: Request, user_info: LoginRequest):
    fetchuser = admin_collection.find_one({"email": user_info.email})

    if fetchuser and handler.verify(user_info.password, fetchuser["password"]):
        generate_auth_result = await generatedToken(
            fetchuser, jwtkeys["user"], 1, request
        )

        if generate_auth_result["status"]:
            user_response = UserResponse(
                firstname=fetchuser.get("firstname", ""),
                lastname=fetchuser.get("lastname", ""),
                email=fetchuser["email"],
                username=fetchuser.get("username", ""),
            )

            return {
                "status": True,
                "message": "Login Successfully",
                "token": generate_auth_result["token"],
                "user": user_response,
            }
        else:
            raise HTTPException(status_code=500, detail="Error generating token")
    else:
        raise HTTPException(status_code=401, detail="Invalid credentials")


@app.post("/upload-additional-data/")
async def upload_payload(payload: AdditionalData):
    try:
        data_to_insert = {
            "SERIESKEY": payload.SERIESKEY,
            "SERIES": payload.SERIES,
        }

        result = additional_data.insert_one(data_to_insert)

        return JSONResponse(
            content={
                "status": True,
                "message": "Additional data uploaded successfully.",
            },
            status_code=201,
        )

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An error occurred: {str(e)}")


@app.get("/get-additional-data/")
async def get_additional_data():
    try:
        data = list(additional_data.find().sort("_id", -1))

        data_serializable = convert_to_serializable(data)

        return JSONResponse(content=data_serializable)

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An error occurred: {str(e)}")


@app.put("/update-additional-data/{record_id}")
async def update_additional_data(record_id: str, payload: AdditionalData):
    try:
        data_to_update = {
            "SERIESKEY": payload.SERIESKEY,
            "SERIES": payload.SERIES,
        }

        result = additional_data.update_one(
            {"_id": ObjectId(record_id)}, {"$set": data_to_update}
        )

        if result.modified_count == 1:
            return JSONResponse(
                content={
                    "status": True,
                    "message": "Additional data update successfully.",
                },
                status_code=200,
            )
        else:
            raise HTTPException(
                status_code=404,
                detail=f"Record with ID {record_id} not found.",
            )

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An error occurred: {str(e)}")


@app.delete("/delete-additional-data/{record_id}")
async def delete_additional_data(record_id: str):
    try:
        result = additional_data.delete_one({"_id": ObjectId(record_id)})

        if result.deleted_count == 1:
            return JSONResponse(
                content={
                    "status": True,
                    "message": f"Additional data deleted successfully.",
                },
                status_code=200,
            )
        else:
            raise HTTPException(
                status_code=404,
                detail=f"Record with ID {record_id} not found.",
            )

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An error occurred: {str(e)}")


async def process_uploaded_data(driver_info: dict = Body(...)):
    try:
        driver_id = str(uuid4())

        rider_country = driver_info.get("Rider_Country_Img", "")

        with open("country_flags.json", "r") as flags_file:
            country_flags = json.load(flags_file)

        rider_country_img = country_flags.get(rider_country, "")

        # Get current datetime
        created_at = datetime.utcnow()

        data = {
            "UUID": driver_id,
            "Driver": driver_info.get("Driver", ""),
            "Born": driver_info.get("Born", ""),
            "Died": driver_info.get("Died", ""),
            "Home": driver_info.get("Home", ""),
            "Rider_Country_Img": rider_country_img,
            "created_at": created_at,
        }

        driver_collection.insert_one(data)

        # Convert datetime objects to strings in the data dictionary
        data_serializable = json.loads(json.dumps(data, default=str))

        return JSONResponse(
            content={
                "status": True,
                "message": "Driver data Add successfully",
                "data": data_serializable,
            },
            status_code=200,
        )
    except Exception as e:
        raise HTTPException(
            status_code=500, detail=f"Error processing data: {str(e)}"
        ) from e


@app.post("/upload-driver-data/")
async def create_upload_driver_data(
    driver_info: dict = Body(...),  # Use dict for request payload
    processed_data: dict = Depends(process_uploaded_data),
):
    return processed_data


class CustomJSONssssEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, ObjectId):
            return str(o)
        elif isinstance(o, float) and math.isnan(o):
            return "NaN"
        return super().default(o)


def convert_to_json_compliants(value, seen=None):
    if seen is None:
        seen = set()

    if id(value) in seen:
        print(f"Circular reference detected for value: {value}")
        return "CIRCULAR_REFERENCE"

    seen.add(id(value))

    if isinstance(value, dict):
        return {k: convert_to_json_compliants(v, seen) for k, v in value.items()}
    elif isinstance(value, list):
        return [convert_to_json_compliants(item, seen) for item in value]
    elif isinstance(value, datetime):  # Handle datetime objects
        return value.isoformat()
    else:
        return value


@app.get("/get-drivers-data-all/", response_class=JSONResponse)
async def get_driver_data():
    driver_names_cursor = db["driver_table_page_1"].distinct("UUID")
    driver_names = [
        {"Driver": driver_name.strip()} for driver_name in driver_names_cursor
    ]

    detailed_driver_info = []
    for driver_info in driver_names:
        driver_name = driver_info["Driver"]
        driver_details = db["drivers"].find_one({"UUID": driver_name})

        if driver_details:
            print(f"Details found for driver: {driver_name}")
            for key, value in driver_details.items():
                driver_details[key] = convert_to_json_compliants(value)

            detailed_driver_info.append(driver_details)
        else:
            print(f"No details found for driver: {driver_name}")

    print("Detailed Driver Info:", detailed_driver_info)

    detailed_driver_info_sorted = sorted(
        detailed_driver_info,
        key=lambda x: x.get("created_at", datetime.min),
        reverse=True,
    )

    print("Sorted Detailed Driver Info:", detailed_driver_info_sorted)

    file_path = "MY_ALL_3_Tables_folder/1st_page_a_to_z/merged_all.csv"
    df = pd.read_csv(file_path)
    scrap_record = df["Driver"][:80]

    scrap_list = []

    for driver_name in scrap_record:
        for driver_info in detailed_driver_info_sorted:
            if driver_info["Driver"] == driver_name:
                driver_info_cleaned = {
                    k: "" if v == "nan" else v for k, v in driver_info.items()
                }
                driver_info_cleaned = {
                    k: "" if pd.isna(v) else v for k, v in driver_info_cleaned.items()
                }
                scrap_list.append(driver_info_cleaned)

    scrap_list = scrap_list[::-1]

    records_json = json.dumps({"records": scrap_list}, cls=CustomJSONssssEncoder)

    drivers_data = json.loads(records_json)
    return drivers_data


def get_object_id(driver_id: str = Path(...)):
    try:
        return ObjectId(driver_id)
    except Exception:
        try:
            # Try parsing as UUID
            return uuid.UUID(driver_id)
        except ValueError:
            raise HTTPException(
                status_code=400, detail="Invalid ObjectId or UUID format"
            )


from bson import ObjectId, json_util


def update_driver(
    driver_id: str = Path(..., title="The ID of the driver to update"),
    update_payload: dict = Body(...),
):
    try:
        driver_id = get_object_id(driver_id)

        existing_driver = driver_collection.find_one({"_id": driver_id})
        print(existing_driver)

        if existing_driver:
            if isinstance(update_payload["Born"], datetime):
                update_payload["Born"] = update_payload["Born"].strftime("%Y-%m-%d")
            if isinstance(update_payload["Died"], datetime):
                update_payload["Died"] = update_payload["Died"].strftime("%Y-%m-%d")

            update_dict = {"$set": update_payload}
            print("Payload", update_payload)
            print("Dict", update_dict)
            driver_collection.update_one({"_id": driver_id}, update_dict)

            updated_driver = driver_collection.find_one({"_id": driver_id})

            serialized_driver = serialize_record(updated_driver)

            return JSONResponse(
                content={
                    "status": True,
                    "message": "Driver updated successfully",
                    "updated_driver": serialized_driver,
                }
            )
        else:
            raise HTTPException(status_code=404, detail="Driver not found")
    except Exception as e:
        logging.error(f"Error updating driver: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Error updating driver: {str(e)}")


@app.put("/update-driver/{driver_id}")
async def update_driver_api(
    driver_id: str = Path(..., title="The ID of the driver to update"),
    update_payload: dict = Body(...),
):
    return update_driver(driver_id, update_payload)


def delete_driver(driver_id: ObjectId = Depends(get_object_id)):
    try:
        result = driver_collection.delete_one({"_id": driver_id})
        print(result)
        if result.deleted_count > 0:
            print(result.deleted_count)
            return {"status": True, "message": "Driver deleted successfully"}
        else:
            raise HTTPException(status_code=404, detail="Driver not found")
    except Exception as e:
        logging.error(f"Error deleting driver: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Error deleting driver: {str(e)}")


@app.delete("/delete-driver/{driver_id}", response_model=None)
async def delete_driver_api(
    driver_id: str = Path(..., title="The ID of the driver to delete")
):
    try:
        object_id = ObjectId(driver_id)
        delete_driver(object_id)

        return JSONResponse(
            content={"status": True, "message": "Driver deleted successfully"}
        )
    except HTTPException as e:
        raise e  # Re-raise HTTPException to keep the original status code and detail message
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error deleting driver: {str(e)}")


@app.get("/get-driver-data/{uuid}")
async def get_rider_data(uuid: str, db: MongoClient = Depends(get_database)):
    try:
        rider_info_cursor = db["driver_table_page_1"].find({"UUID": uuid})

        rider_info_list = list(rider_info_cursor)

        if rider_info_list:
            rider_data_list = []
            all_keys = set()

            for rider_info in rider_info_list:
                rider_name = rider_info.get("Driver", "")
                csv_file_name = rider_info.get("CSVFileName", "")
                csv_data = rider_info.get("CSVData", [])

                cleaned_csv_data = clean_csv_data(csv_data, csv_file_name)

                raw_table_name = cleaned_csv_data[0]["table_name"]
                table_name = raw_table_name.replace("TABLE_NAME___", "").replace(
                    "_", " "
                )

                cleaned_csv_data_no_table_name = [
                    {key: value for key, value in record.items() if key != "table_name"}
                    for record in cleaned_csv_data
                ]
                all_list_keys = []

                for record in cleaned_csv_data:
                    record_keys = [key for key in record.keys() if key != "table_name"]
                    print("record --", record_keys)
                    all_list_keys.append(record_keys)
                print("\n=========", all_list_keys)

                all_keys_list = list(all_keys)

                for record in cleaned_csv_data_no_table_name:
                    all_keys.update(record.keys())

                rider_data_list.append(
                    {
                        "rider_name": rider_name,
                        "table_name": table_name,
                        "csv_data": cleaned_csv_data_no_table_name,
                        "keys": all_list_keys[0],
                    }
                )

            return rider_data_list
        else:
            raise HTTPException(
                status_code=404, detail="Rider not found with the given UUID"
            )

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.put("/update-driver-data/{id}")
async def update_driver_data(
    id: str = Path(..., title="The ID of the driver data to update"),
    races_uuid: Optional[str] = Body(None, title="The races_uuid for the driver data"),
    update_payload: Dict[str, Any] = Body(..., title="The dynamic fields to update"),
    db: AsyncIOMotorDatabase = Depends(get_database),
):
    print("Received ID:", id)
    print("Received races_uuid:", races_uuid)

    existing_driver = db["driver_table_page_1_test"].find_one(
        {
            "UUID": id,
            "$or": [
                {"CSVData.races_uuid": races_uuid},
                {"CSVData.race_uuid": races_uuid},
            ],
        }
    )

    print("Existing Driver:", existing_driver)

    if not existing_driver:
        raise HTTPException(status_code=404, detail="Driver not found")

    update_dict = {
        "CSVData.$[elem]." + key: value for key, value in update_payload.items()
    }

    array_filter_key = "races_uuid" if races_uuid else "race_uuid"
    array_filters = [{"elem." + array_filter_key: {"$eq": races_uuid}}]

    result = db["driver_table_page_1_test"].find_one_and_update(
        {
            "UUID": id,
            "$or": [
                {"CSVData.races_uuid": races_uuid},
                {"CSVData.race_uuid": races_uuid},
            ],
        },
        {"$set": update_dict},
        return_document=ReturnDocument.AFTER,
        upsert=False,
        array_filters=array_filters,
    )

    print("Result after update:", result)

    return {"status": True, "message": "Driver data updated successfully"}


@app.put("/update-driver-race-data/{id}")
async def update_driver_data(
    id: str = Path(..., title="The ID of the driver data to update"),
    race_uuid: Optional[str] = Body(None, title="The races_uuid for the driver data"),
    update_payload: Dict[str, Any] = Body(..., title="The dynamic fields to update"),
    db: AsyncIOMotorDatabase = Depends(get_database),
):
    print("Received ID:", id)
    print("Received race_uuid:", race_uuid)

    existing_driver = db["driver_table_page_1_test"].find_one(
        {
            "UUID": id,
            "CSVData.race_uuid": race_uuid,
        }
    )

    print("Existing Driver:", existing_driver)

    if not existing_driver:
        raise HTTPException(status_code=404, detail="Driver not found")

    update_dict = {
        "CSVData.$[elem]." + key: value for key, value in update_payload.items()
    }

    array_filters = [{"elem.race_uuid": {"$eq": race_uuid}}]

    result = db["driver_table_page_1_test"].find_one_and_update(
        {
            "UUID": id,
            "CSVData.race_uuid": race_uuid,
        },
        {"$set": update_dict},
        return_document=ReturnDocument.AFTER,
        upsert=False,
        array_filters=array_filters,
    )

    print("Result after update:", result)

    return {"status": True, "message": "Driver data updated successfully"}


from fastapi import HTTPException


@app.put("/insert-row-driver-data/{id}")
async def insert_row_driver_data(
    id: str = Path(..., title="The ID of the driver data to update"),
    request_data: Dict[str, Any] = Body(
        ..., title="Request data including table_name and update_payload"
    ),
    db: AsyncIOMotorDatabase = Depends(get_database),
):
    table_name = request_data.get("table_name")
    update_payload = request_data.get("update_payload", {})

    if not table_name:
        raise HTTPException(
            status_code=400,
            detail="Table name is required in the request body",
        )

    print("Received ID:", id)
    print("Received table_name:", table_name)
    csv_file_name = existing_driver.get("CSVFileName", "")
    csv_file_name = csv_file_name.replace("TABLE_NAME___", f"{table_name}___")

    print("\nUpdated CSVFileName:", csv_file_name)
    try:
        existing_driver = db["driver_table_page_1_test"].find_one({"UUID": id})

        if existing_driver:
            print("Existing Driver:", existing_driver)

            existing_csv_data = existing_driver.get("CSVData", [])

            year_uuid = str(uuid.uuid4()) if "Year" in update_payload else None
            race_uuid = str(uuid.uuid4()) if "Race" in update_payload else None
            races_uuid = str(uuid.uuid4()) if "Year" in update_payload else None

            if year_uuid is not None:
                update_payload["year_uuid"] = year_uuid
                print("Generated year_uuid:", year_uuid)

            if race_uuid is not None:
                update_payload["race_uuid"] = race_uuid
                print("Generated race_uuid:", race_uuid)

            if races_uuid is not None:
                update_payload["races_uuid"] = races_uuid
                print("Generated races_uuid:", races_uuid)

            for item in existing_csv_data:
                if year_uuid is not None:
                    item["year_uuid"] = year_uuid

                if race_uuid is not None:
                    item["race_uuid"] = race_uuid

            existing_csv_data.append(update_payload)

            # Assuming 'Year' is the unique identifier for the CSVData items
            array_filters = [{"elem.Year": update_payload.get("Year")}]

            csv_file_name = existing_driver.get("CSVFileName", "")
            csv_file_name = csv_file_name.replace("TABLE_NAME___", f"{table_name}___")

            print("\nUpdated CSVFileName:", csv_file_name)

            update_dict = {"CSVData": existing_csv_data, "CSVFileName": csv_file_name}

            update_dict["CSVFileName"] = csv_file_name

            result = db["driver_table_page_1_test"].find_one(
                {"UUID": id},
                {"$set": update_dict},
            )

            print("\nUpdate operation result:", result)

            db["driver_table_page_1_test"].update_one(
                {"UUID": id}, {"$set": {"CSVFileName": csv_file_name}}
            )

            return {"status": True, "message": "Row Inserted successfully."}

        else:
            return {"status": False, "message": "Driver not found."}

    except Exception as e:
        print(f"Error: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")


def handle_nan_inf(value):
    if pd.isna(value) or value == np.inf or value == -np.inf:
        return None
    return value


def second_page_clean_csv_data(row):
    cleaned_row = {}
    for key, value in row.items():
        if key == "Driver":
            try:
                driver_info = ast.literal_eval(value)
                cleaned_row[key] = (
                    driver_info[0] if driver_info and len(driver_info) > 0 else None
                )
            except (ValueError, IndexError):
                cleaned_row[key] = None
        else:
            cleaned_row[key] = handle_nan_inf(value)
    return cleaned_row


def clean_csv_datas(csv_data):
    all_keys = set()
    cleaned_data_list = []

    for row in csv_data:
        cleaned_row = second_page_clean_csv_data(row)
        all_keys.update(
            cleaned_row.keys()
        )  # Update the set with keys from the current row
        cleaned_data_list.append(cleaned_row)

    unique_keys_array = list(all_keys)  # Convert the set to a list
    return cleaned_data_list, unique_keys_array


def extract_unique_keys(csv_data):
    all_keys = set()
    for row in csv_data:
        all_keys.update(row.keys())
    return list(all_keys)


@app.get("/get-driver-second-page-data/{uuid}", response_model=List[dict])
async def get_rider_data(
    uuid: str,
    race_uuid: str = None,
    races_uuid: str = None,
    db: MongoClient = Depends(get_database),
):
    try:
        # Define the filter criteria
        filter_criteria = {"uuid": uuid}
        if race_uuid:
            filter_criteria["race_uuid"] = race_uuid
        elif races_uuid:
            filter_criteria["races_uuid"] = races_uuid

        rider_info_cursor = db["driver_table_page_2"].find(filter_criteria)
        rider_info_list = list(rider_info_cursor)

        if not rider_info_list:
            raise HTTPException(
                status_code=404, detail="Rider not found with the given uuid"
            )

        rider_data_list = []
        unique_keys_array = []

        for rider_info in rider_info_list:
            cleaned_csv_data, unique_keys = clean_csv_datas(rider_info["csv_data"])
            rider_data = {
                "csv_data": cleaned_csv_data,
                "races_uuid": rider_info.get("races_uuid", ""),
                "race_uuid": rider_info.get("race_uuid", ""),
                "keys": unique_keys,  # Add the keys array to the result
            }
            rider_data_list.append(rider_data)
            unique_keys_array.extend(unique_keys)

        unique_keys_array = list(set(unique_keys_array))

        return {
            "rider_data_list": rider_data_list,
            "unique_keys_array": unique_keys_array,
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.put("/update-driver-second-page-data")
async def update_driver_data(
    third_page_uuid: Optional[str] = Body(
        None, title="The 3rdpageuuid for the driver data"
    ),
    update_payload: Dict[str, Any] = Body(..., title="The dynamic fields to update"),
    db: AsyncIOMotorDatabase = Depends(get_database),
):
    print("Received 3rdpageuuid:", third_page_uuid)
    print("Update Payload:", update_payload)

    existing_driver = db["driver_table_page_2"].find_one(
        {
            "csv_data.3rdpageuuid": third_page_uuid,
        }
    )

    print("Query to Find Existing Driver:", existing_driver)

    if not existing_driver:
        raise HTTPException(status_code=404, detail="Driver not found")

    update_dict = {
        "csv_data.$[elem]." + key: value for key, value in update_payload.items()
    }

    result = db["driver_table_page_2"].find_one_and_update(
        {
            "csv_data.3rdpageuuid": third_page_uuid,
        },
        {"$set": update_dict},
        return_document=ReturnDocument.AFTER,
        upsert=False,
        array_filters=[{"elem.3rdpageuuid": third_page_uuid}],
    )

    print("Result after update:", result)

    return {"status": True, "message": "Driver data updated successfully"}


@app.put("/insert-row-driver-page-2-data/{race_uuid}")
async def insert_row_driver_data(
    race_uuid: str = Path(..., title="The race_uuid of the driver data to update"),
    request_data: Dict[str, Any] = Body(
        ..., title="Request data including update_payload"
    ),
    db: AsyncIOMotorDatabase = Depends(get_database),
):
    print("Received race_uuid:", race_uuid)

    try:
        existing_driver = db["driver_table_page_2"].find_one(
            {"$or": [{"race_uuid": race_uuid}, {"races_uuid": race_uuid}]}
        )

        if existing_driver:
            updated_driver_data = existing_driver.copy()

            csv_data = updated_driver_data.get("csv_data", [])

            dynamic_keys = set(request_data.get("update_payload", {}).keys())

            third_page_uuid = str(uuid.uuid4())

            relevant_data = {
                k: v
                for k, v in request_data["update_payload"].items()
                if k in dynamic_keys
            }

            relevant_data["3rdpageuuid"] = third_page_uuid

            csv_data.append(relevant_data)

            db["driver_table_page_2"].update_one(
                {"$or": [{"race_uuid": race_uuid}, {"races_uuid": race_uuid}]},
                {"$set": {"csv_data": csv_data}},
            )

            return {"status": True, "message": "Row Inserted successfully."}

        else:
            raise HTTPException(status_code=404, detail="Driver not found")

    except Exception as e:
        print(f"Error: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")


# async def run_automation():
#     print("Running automation...")
#     try:
#         await automation(db)
#     except Exception as e:
#         print(f"Error running automation: {e}")


# async def scheduled_job(interval_minutes):
#     while True:
#         schedule.run_pending()
#         await asyncio.sleep(interval_minutes * 60)


# @app.post("/api/set-schedule-interval/")
# async def set_schedule_interval(
#     scheduled_time: str, db: MongoClient = Depends(get_database)
# ):
#     global scheduled_job_task

#     try:
#         scheduled_datetime = datetime.strptime(scheduled_time, "%Y-%m-%d %H:%M:%S")

#         time_difference = scheduled_datetime - datetime.now()

#         if "scheduled_job_task" in globals():
#             scheduled_job_task.cancel()

#         schedule.every(time_difference.total_seconds() / 60).minutes.do(
#             lambda: asyncio.create_task(run_automation())
#         )

#         scheduled_job_task = asyncio.create_task(
#             scheduled_job(time_difference.total_seconds() / 60)
#         )

#         return JSONResponse(
#             content={"message": "Scheduler interval set to "},
#             status_code=200,
#         )
#     except ValueError as e:
#         return JSONResponse(
#             content={
#                 "error": "Invalid datetime format. Please use the format '%Y-%m-%d %H:%M:%S'"
#             },
#             status_code=400,
#         )


# @app.on_event("startup")
# async def on_startup():
#     global scheduled_job_task
#     scheduled_job_task = asyncio.create_task(scheduled_job(10))
from fastapi import BackgroundTasks, FastAPI, Depends

async def automation():
    print("Automation running in background...")

async def run_automation(db):
    print("Running automation...")
    try:
        await automation(db)
        print("Automation executed successfully")
    except Exception as e:
        print(f"Error running automation: {e}")

@app.post("/api/set-schedule-interval/")
async def run_automation_endpoint(background_tasks: BackgroundTasks, db: MongoClient = Depends(get_database)):
    background_tasks.add_task(run_automation, db)
    return JSONResponse(content={"message": "Script Run Successfully..."}, status_code=200)


drivers_collection = db["drivers"]

def automation_process_uploaded_file(file_path: str):
    try:
        df = pd.read_csv(file_path)
        data = df.to_dict(orient="records")
        if not data:
            return JSONResponse(
                content={
                    "message": f"EMPTY CSV No new record in csv _________",
                },
                status_code=404,
            )

        current_datetime = datetime.utcnow()

        existing_uuids = set(drivers_collection.distinct("UUID"))
        data_to_insert = [
            {**record, "created_at": current_datetime}
            for record in data
            if record["UUID"] not in existing_uuids
        ]

        if data_to_insert:
            drivers_collection.insert_many(data_to_insert)
            return JSONResponse(
                content={
                    "message": f"{len(data_to_insert)} driver(s) data uploaded successfully",
                    "existing_drivers": f"{len(existing_uuids)} driver(s) exists in drivers_collection",
                },
                status_code=200,
            )
        else:
            return JSONResponse(
                content={"message": "No new data to insert"}, status_code=400
            )
    except Exception as e:
        raise HTTPException(
            status_code=500, detail=f"Error processing file: {str(e)}"
        ) from e


def automation_insert_rider_second_page_data(FOLDER_PATH):
    cleaned_data = list(db["drivers"].find())
    collection_data = [
        (record.get("Driver", ""), record.get("UUID", "")) for record in cleaned_data
    ]

    folders = os.listdir(FOLDER_PATH)
    folders = [name.split("___")[0] for name in folders]

    matching_names_uuid = [
        (driver_name, uuid)
        for driver_name, uuid in collection_data
        if uuid.replace("-", "") in folders
    ]

    for driver_name, UUID in matching_names_uuid:
        print(
            f"Found matching driver: '{driver_name}', Length: {len(driver_name)}, UUID: {UUID}"
        )
        my_UUID = UUID.replace("-", "")
        name = driver_name.replace(" ", "_").replace(".", "_").replace(",", "")
        folder_name = f"{my_UUID}___{name}"
        print("folder_name-------", folder_name)
        folder_to_open = os.path.join(FOLDER_PATH, folder_name)
        subprocess.run(["explorer", folder_to_open], shell=True)  # For Windows
        print("folder_to_open-------", folder_to_open)

        csv_files = [
            file for file in os.listdir(folder_to_open) if file.lower().endswith(".csv")
        ]

        for csv_file in csv_files:
            csv_path = os.path.join(folder_to_open, csv_file)
            df = pd.read_csv(csv_path)

            riderss_collection = db["driver_table_page_1"]
            data_to_insert = {
                "UUID": UUID,
                "Driver": driver_name,
                "CSVFileName": csv_file,
                "CSVData": df.to_dict(orient="records"),
            }
            for i in range(len(data_to_insert["CSVData"])):
                if data_to_insert["CSVData"][i]:
                    if data_to_insert["CSVData"][i].get("Race"):
                        generated_uuid = str(uuid.uuid4())
                        data_to_insert["CSVData"][i]["race_uuid"] = generated_uuid

                    if data_to_insert["CSVData"][i].get("Races"):
                        generated_uuid1 = str(uuid.uuid4())
                        generated_uuid2 = str(uuid.uuid4())
                        data_to_insert["CSVData"][i]["year_uuid"] = generated_uuid1
                        data_to_insert["CSVData"][i]["races_uuid"] = generated_uuid2
                    else:
                        None

            riderss_collection.insert_one(data_to_insert)


@app.post("/api/auto-upload-drivers-data/")
async def automation(db: MongoClient = Depends(get_database)):
    fist_page_script.sysInit()
    file_path = "Automation_Script/first_page_a_to_z/new_data_first_page.csv"
    response = automation_process_uploaded_file(file_path)

    if (
        response.status_code == 400
        or response.status_code == 404
        or response.status_code == 500
    ):
        return response

    elif response.status_code == 200:
        second_page_script.sysInit()
        FOLDER_PATH = (
            "Automation_Script/second_page_driver_tables_Script/new_drivers_data/"
        )
        automation_insert_rider_second_page_data(FOLDER_PATH=FOLDER_PATH)

        # For 3rd Page
        third_page_script.sysInit()  # call scrapping script
        rider_info_list = list(db["driver_table_page_1"].find())
        # Process rider names if not already processed
        if not RIDER_NAMES_PROCESSED:
            automation_process_rider_race_names(db, rider_info_list)
            time.sleep(5)
            automation_process_rider_names(db, rider_info_list)

        return JSONResponse(
            content={
                "status": True,
                "message": "Data of 1st, 2nd and 3rd page inserted successfully",
            },
            status_code=201,
        )




########################################################## OWNER #######################################################
@app.post("/scrape-owners-stats")
async def read_item():
    result = scrape_owners("https://www.racing-reference.info/owners-report/")
    print(result)
    return JSONResponse(content=result)


@app.get("/get-owners")
async def get_owners():
    try:
        cursor = owner_collection.find().sort("created_at", -1)

        data_list = json.loads(
            json_util.dumps(list(cursor), cls=CustomJSONOwnerEncoder)
        )

        formatted_data_list = [
            {
                "Owner": data.get("Owner", ""),
                "UUID": data.get("UUID", ""),
                "_id": str(data.get("_id", "")["$oid"]),
                "created_at": data.get("created_at", ""),
            }
            for data in data_list
        ]

        return JSONResponse(
            content={
                "message": "Data Retrieved Successfully",
                "data": formatted_data_list,
            },
            status_code=200,
        )

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Internal Server Error: {str(e)}")


@app.post("/get_first_5_headings_with_table")
async def get_first_5_headings_with_table():
    all_data = []

    try:
        for document in owner_collection.find().limit(10):
            link = document.get("Link")
            UUID = document.get("UUID")

            if link:
                driver = None

                try:
                    driver = webdriver.Chrome(ChromeDriverManager().install())
                    driver.get(link)

                    time.sleep(5)

                    page_source = driver.page_source

                    soup = BeautifulSoup(page_source, "html.parser")

                    heading_tag = soup.find("h2")

                    if heading_tag:
                        headings = [tag.text.strip() for tag in heading_tag]
                        data = {"UUID": str(document["UUID"]), "headings": headings}
                        print(data)

                    else:
                        data = {
                            "UUID": str(document["UUID"]),
                            "headings": ["No h2 tags found"],
                        }

                    table_data = []
                    table_rows = soup.select("table.tb tr")

                    if len(table_rows) > 3:
                        table_data = [
                            row_data.find_all(["th", "td"])
                            for row_data in table_rows[3:-1]
                            if row_data
                        ]

                        # Check if there is any data before extracting
                        if table_data:
                            table_data = [
                                [td.get_text(strip=True) for td in row_data]
                                for row_data in table_data
                            ]

                            # Exclude specific rows
                            exclude_rows = [
                                "Links for this owner:",
                                "",
                                "Video Links (0)",
                            ]
                            table_data = [
                                row_data
                                for row_data in table_data
                                if row_data not in exclude_rows
                            ]

                            # Remove the first array from table_data
                            table_data = table_data

                            data["table_data"] = table_data
                        else:
                            data["table_data"] = "No table found"
                    else:
                        data["table_data"] = "Not enough rows to extract table data"

                    all_data.append(data)
                    print(all_data)
                except Exception as e:
                    all_data.append(
                        {
                            "UUID": str(document["UUID"]),
                            "error": f"Error fetching data from {link}: {str(e)}",
                        }
                    )
                finally:
                    if driver:
                        driver.quit()

            else:
                all_data.append(
                    {
                        "UUID": str(document["UUID"]),
                        "error": "Link not found in the document",
                    }
                )

        formatted_data = reformat_data(all_data)

        owner_statistics_collection.insert_many(formatted_data)

        success_message = "Data uploaded successfully."
        return JSONResponse(
            content={"message": success_message, "data": formatted_data},
            status_code=200,
        )

    except Exception as e:
        error_message = "Data uploaded successfully."
        return JSONResponse(content={"message": error_message}, status_code=500)

    finally:
        pass


def reformat_data(all_data):
    reformatted_data = []

    for entry in all_data:
        formatted_entry = {
            "UUID": str(entry["UUID"]),
            "heading": entry["heading"],
            "table_data": [],
        }

        if "table_data" in entry:
            table_data = entry["table_data"][1:]

            for row_data in table_data:
                row_dict = {}  # Add the UUID to each row_dict
                for i in range(len(row_data)):
                    header = entry["table_data"][0][i]
                    value = row_data[i]

                    if isinstance(value, ObjectId):
                        value = str(value)

                    row_dict[header] = value

                formatted_entry["table_data"].append(row_dict)

        reformatted_data.append(formatted_entry)

    return reformatted_data



@app.get("/get_data_by_uuid/{uuid}")
async def get_data_by_uuid(uuid: str):
    try:
        result = owner_statistics_collection.find_one({"UUID": uuid})

        if result:
            result["_id"] = str(result["_id"])

            return result
        else:
            raise HTTPException(
                status_code=404, detail="Data not found for the provided UUID"
            )

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Internal Server Error: {str(e)}")


@app.post("/insert_owner_data")
async def post_owner_data(owner_data: OwnerData):
    try:
        data_dict = owner_data.dict()

        data_dict["UUID"] = str(uuid.uuid4())

        data_dict["created_at"] = datetime.utcnow().isoformat()

        inserted_data = owner_collection.insert_one(data_dict)

        inserted_document = owner_collection.find_one(
            {"_id": inserted_data.inserted_id}
        )

        inserted_document.pop("_id", None)

        return JSONResponse(
            content={
                "message": "Data Inserted Successfully",
                "data": inserted_document,
            },
            status_code=200,
        )

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Internal Server Error: {str(e)}")


class CustomJSONOwnerEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, ObjectId):
            return str(o)
        elif isinstance(o, datetime):
            return o.isoformat()
        return super().default(o)


@app.get("/get_owner_data")
async def get_owner_data():
    try:
        cursor = owner_collection.find().sort("created_at", -1)

        data_list = json.loads(
            json_util.dumps(list(cursor), cls=CustomJSONOwnerEncoder)
        )

        formatted_data_list = [
            {
                "Owner": data.get("Owner", ""),
                "UUID": data.get("UUID", ""),
                "_id": str(data.get("_id", "")["$oid"]),
                "created_at": data.get("created_at", ""),
            }
            for data in data_list
        ]

        return JSONResponse(
            content={
                "message": "Data Retrieved Successfully",
                "data": formatted_data_list,
            },
            status_code=200,
        )

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Internal Server Error: {str(e)}")


@app.put("/update_owner_data/{uuid}")
async def update_owner_data(uuid: str, updated_data: OwnerData):
    try:
        existing_document = owner_collection.find_one({"UUID": uuid})

        if existing_document:
            owner_collection.update_one(
                {"UUID": uuid}, {"$set": {"Owner": updated_data.Owner}}
            )

            # Retrieve the updated document
            updated_document = owner_collection.find_one({"UUID": uuid})

            updated_document.pop("_id", None)

            return JSONResponse(
                content={
                    "message": "Data Updated Successfully",
                    "data": updated_document,
                },
                status_code=200,
            )
        else:
            raise HTTPException(
                status_code=404, detail="Data not found for the provided UUID"
            )

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Internal Server Error: {str(e)}")


@app.delete("/delete_owner_data/{uuid}")
async def delete_owner_data(uuid: str):
    try:
        existing_document = owner_collection.find_one({"UUID": uuid})

        if existing_document:
            existing_document["_id"] = str(existing_document["_id"])

            owner_collection.delete_one({"UUID": uuid})

            return JSONResponse(
                content={"message": "Data Deleted Successfully"}, status_code=200
            )
        else:
            raise HTTPException(
                status_code=404, detail="Data not found for the provided UUID"
            )

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Internal Server Error: {str(e)}")


########################################################## CUP #######################################################
@app.post("/scrape-nascar-stats")
async def read_item():
    result = scrape_cups("https://www.racing-reference.info/nascar-cup-series-stats/")
    print(result)
    return JSONResponse(content=result)


@app.get("/get-nascar-stats")
async def get_nascar_stats():
    try:
        cursor = cup_collection.find().sort("created_at", -1)

        data_list = json.loads(
            json_util.dumps(list(cursor), cls=CustomJSONOwnerEncoder)
        )

        formatted_data_list = [
            {
                "Year": data.get("Year", ""),
                "Races": data.get("Races", ""),
                "Champion": data.get("Champion", ""),
                "Car_Owner": data.get("Car_Owner", ""),
                "Champions_Crew_Chief": data.get("Champions_Crew_Chief", ""),
                "Rookie_of_the_Year": data.get("Rookie_of_the_Year", ""),
                "Most_Popular_Driver": data.get("Most_Popular_Driver", ""),
                "Manufacturers_Championship": data.get(
                    "Manufacturers_Championship", ""
                ),
                "UUID": data.get("UUID", ""),
                "_id": str(data.get("_id", "")["$oid"]),
                "created_at": data.get("created_at", ""),
            }
            for data in data_list
        ]

        return JSONResponse(
            content={
                "message": "Data Retrieved Successfully",
                "data": formatted_data_list,
            },
            status_code=200,
        )

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Internal Server Error: {str(e)}")


@app.delete("/delete_cup_data/{uuid}")
async def delete_cup_data(uuid: str):
    try:
        existing_document = cup_collection.find_one({"UUID": uuid})

        if existing_document:
            existing_document["_id"] = str(existing_document["_id"])

            cup_collection.delete_one({"UUID": uuid})

            return JSONResponse(
                content={"message": "Data Deleted Successfully"}, status_code=200
            )
        else:
            raise HTTPException(
                status_code=404, detail="Data not found for the provided UUID"
            )

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Internal Server Error: {str(e)}")


@app.post("/insert_cup_data")
async def post_cup_data(cup_data: CupData):
    try:
        data_dict = cup_data.dict()

        data_dict["UUID"] = str(uuid.uuid4())

        data_dict["created_at"] = datetime.utcnow().isoformat()

        inserted_data = cup_collection.insert_one(data_dict)

        inserted_document = cup_collection.find_one({"_id": inserted_data.inserted_id})

        inserted_document.pop("_id", None)

        return JSONResponse(
            content={
                "message": "Data Inserted Successfully",
                "data": inserted_document,
            },
            status_code=200,
        )

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Internal Server Error: {str(e)}")


@app.put("/update_cup_data/{uuid}")
async def update_cup_data(uuid: str, cup_data: CupData):
    try:
        existing_document = cup_collection.find_one({"UUID": uuid})

        if existing_document:
            updated_data = {
                "$set": cup_data.dict(),
                "$currentDate": {"lastModified": True},
            }
            cup_collection.update_one({"UUID": uuid}, updated_data)

            updated_document = cup_collection.find_one({"UUID": uuid})
            updated_document.pop("_id", None)

            serialized_document = json.loads(
                json_util.dumps(updated_document, default=json_util.default)
            )

            return JSONResponse(
                content={
                    "message": "Data Updated Successfully",
                    "data": serialized_document,
                },
                status_code=200,
            )
        else:
            raise HTTPException(status_code=404, detail="Document not found")

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Internal Server Error: {str(e)}")


########################################################## IMSA #######################################################
@app.post("/scrape-imsa-stats")
async def read_item():
    result = scrape_imsa_series("https://www.racing-reference.info/imsa-series/")
    print(result)
    return JSONResponse(content=result)


@app.get("/get-imsa-stats")
async def get_imsa_stats():
    try:
        cursor = Imsa_collection.find().sort("created_at", -1)

        data_list = json.loads(
            json_util.dumps(list(cursor), cls=CustomJSONOwnerEncoder)
        )

        if data_list:
            formatted_data_list = [
                {
                    key: str(value["$oid"])
                    if key == "_id" and "$oid" in value
                    else value
                    for key, value in data.items()
                }
                for data in data_list
            ]

            return JSONResponse(
                content={
                    "message": "Data Retrieved Successfully",
                    "data": formatted_data_list,
                },
                status_code=200,
            )
        else:
            return JSONResponse(
                content={
                    "message": "No data available",
                    "data": [],
                },
                status_code=200,
            )

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Internal Server Error: {str(e)}")


@app.delete("/delete_imsa_data/{uuid}")
async def delete_imsa_data(uuid: str):
    try:
        existing_document = Imsa_collection.find_one({"UUID": uuid})

        if existing_document:
            existing_document["_id"] = str(existing_document["_id"])

            Imsa_collection.delete_one({"UUID": uuid})

            return JSONResponse(
                content={"message": "Data Deleted Successfully"}, status_code=200
            )
        else:
            raise HTTPException(
                status_code=404, detail="Data not found for the provided UUID"
            )

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Internal Server Error: {str(e)}")


@app.post("/insert_imsa_data")
async def post_imsa_data(imsa_data: ImsaData):
    try:
        data_dict = imsa_data.dict()

        data_dict["UUID"] = str(uuid.uuid4())
        data_dict["created_at"] = datetime.utcnow().isoformat()

        inserted_data = Imsa_collection.insert_one(data_dict)

        inserted_document = Imsa_collection.find_one({"_id": inserted_data.inserted_id})
        inserted_document.pop("_id", None)

        return JSONResponse(
            content={
                "message": "Data Inserted Successfully",
                "data": inserted_document,
            },
            status_code=200,
        )
    except Exception as e:
        return JSONResponse(
            content={
                "message": f"Error inserting data: {str(e)}",
            },
            status_code=500,
        )


########################################################## INDY CAR #######################################################
@app.post("/scrape-Indy-Car")
async def read_item():
    result = scrape_indy_car("https://www.racing-reference.info/indycar-series/")
    print(result)
    return JSONResponse(content=result)


@app.get("/get-Indy-Car")
async def get_Indy_Car():
    try:
        cursor = Indy_Car_collection.find().sort("created_at", -1)

        data_list = json.loads(json_util.dumps(list(cursor), default=json_util.default))

        formatted_data_list = [
            {
                "UUID": data.get("UUID", ""),
                "Year": data.get("Year", ""),
                "Races": data.get("Races", ""),
                "Champion": data.get("Champion", []),
                "Rookie_of_the_Year": data.get("Rookie_of_the_Year", []),
                "Most_Popular_Driver": data.get("Most_Popular_Driver", []),
            }
            for data in data_list
        ]

        return JSONResponse(
            content={
                "message": "Data Retrieved Successfully",
                "data": formatted_data_list,
            },
            status_code=200,
        )

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Internal Server Error: {str(e)}")


@app.delete("/delete_indy_data/{uuid}")
async def delete_indy_data(uuid: str):
    try:
        existing_document = Indy_Car_collection.find_one({"UUID": uuid})

        if existing_document:
            existing_document["_id"] = str(existing_document["_id"])

            Indy_Car_collection.delete_one({"UUID": uuid})

            return JSONResponse(
                content={"message": "Data Deleted Successfully"}, status_code=200
            )
        else:
            raise HTTPException(
                status_code=404, detail="Data not found for the provided UUID"
            )

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Internal Server Error: {str(e)}")


@app.post("/insert_indy_car_data")
async def post_indy_car_data(indy_car_data: IndyCar):
    try:
        data_dict = indy_car_data.dict()

        data_dict["UUID"] = str(uuid.uuid4())
        data_dict["created_at"] = datetime.utcnow().isoformat()

        inserted_data = Indy_Car_collection.insert_one(data_dict)

        inserted_document = Indy_Car_collection.find_one(
            {"_id": inserted_data.inserted_id}
        )
        inserted_document.pop("_id", None)

        return JSONResponse(
            content={
                "message": "Data Inserted Successfully",
                "data": inserted_document,
            },
            status_code=200,
        )
    except Exception as e:
        return JSONResponse(
            content={
                "message": f"Error inserting data: {str(e)}",
            },
            status_code=500,
        )


@app.put("/edit_indy_car_data/{uuid}")
async def update_indy_data(uuid: str, indy_car_data: IndyCar):
    try:
        existing_document = Indy_Car_collection.find_one({"UUID": uuid})

        if existing_document:
            updated_data = {
                "$set": indy_car_data.dict(),
                "$currentDate": {"lastModified": True},
            }
            Indy_Car_collection.update_one({"UUID": uuid}, updated_data)

            updated_document = Indy_Car_collection.find_one({"UUID": uuid})
            updated_document.pop("_id", None)

            serialized_document = json.loads(
                json_util.dumps(updated_document, default=json_util.default)
            )

            return JSONResponse(
                content={
                    "message": "Data Updated Successfully",
                    "data": serialized_document,
                },
                status_code=200,
            )
        else:
            raise HTTPException(status_code=404, detail="Document not found")

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Internal Server Error: {str(e)}")


########################################################## F1 SERIES #######################################################
@app.post("/scrape-f1-series")
async def read_item():
    result = scrape_f1_series("https://www.racing-reference.info/f1-series/")
    print(result)
    return JSONResponse(content=result)


@app.get("/get-f1-series")
async def get_f1_series():
    try:
        cursor = f1_series_collection.find().sort("created_at", -1)

        data_list = json.loads(
            json_util.dumps(list(cursor), cls=CustomJSONOwnerEncoder)
        )

        formatted_data_list = [
            {
                "Year": data.get("Year", ""),
                "Races": data.get("Races", ""),
                "Driver_Champion": data.get("Driver_Champion", ""),
                "Driver_Points": data.get("Driver_Points", ""),
                "Constructor_Champion": data.get("Constructor_Champion", ""),
                "Constructor_Points": data.get("Constructor_Points", ""),
                "Engine": data.get("Engine", ""),
                "UUID": data.get("UUID", ""),
                "created_at": data.get("created_at", ""),
            }
            for data in data_list
        ]

        return JSONResponse(
            content={
                "message": "Data Retrieved Successfully",
                "data": formatted_data_list,
            },
            status_code=200,
        )

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Internal Server Error: {str(e)}")


@app.delete("/delete_f1_data/{uuid}")
async def delete_f1_data(uuid: str):
    try:
        existing_document = f1_series_collection.find_one({"UUID": uuid})

        if existing_document:
            f1_series_collection.delete_one({"UUID": uuid})

            return {"message": "Data Deleted Successfully"}
        else:
            raise HTTPException(status_code=404, detail="Document not found")

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Internal Server Error: {str(e)}")


@app.post("/insert_f1_data")
async def post_f1_data(f1_data: f1_Series_Data):
    try:
        data_dict = f1_data.dict()

        data_dict["UUID"] = str(uuid.uuid4())
        data_dict["created_at"] = datetime.utcnow().isoformat()

        inserted_data = f1_series_collection.insert_one(data_dict)

        inserted_document = f1_series_collection.find_one(
            {"_id": inserted_data.inserted_id}
        )

        inserted_document.pop("_id", None)

        return {
            "message": "Data Inserted Successfully",
            "data": inserted_document,
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Internal Server Error: {str(e)}")


class CustomJSONf1Encoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, datetime):
            return o.isoformat()
        return super().default(o)


@app.put("/update_f1_data/{uuid}")
async def update_f1_data(uuid: str, updated_data: f1_Series_Data = Body(...)):
    try:
        existing_document = f1_series_collection.find_one({"UUID": uuid})

        if existing_document:
            updated_data_dict = updated_data.dict(exclude_unset=True)
            f1_series_collection.update_one({"UUID": uuid}, {"$set": updated_data_dict})

            updated_document = f1_series_collection.find_one({"UUID": uuid})

            updated_document_str = json_util.dumps(
                updated_document, cls=CustomJSONf1Encoder
            )
            updated_document = json.loads(updated_document_str)

            updated_document.pop("_id", None)

            return JSONResponse(
                content={
                    "message": "Data Updated Successfully",
                    "data": updated_document,
                },
                status_code=200,
            )
        else:
            raise HTTPException(status_code=404, detail="Document not found")

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Internal Server Error: {str(e)}")


########################################################## XFINITY #######################################################
@app.post("/auto-add-xfinity")
async def add_xfinity(db: MongoClient = Depends(get_database)):
    xfinity_script.sysInit()
    return JSONResponse(
        content={"status": True, "message": "data of xfinity inserted successfully"},
        status_code=201,
    )


@app.get("/get-xfinity/")
async def get_tracks_data(
    series_name: str = Query(None, title="Series Name"),
    db: MongoClient = Depends(get_database),
):
    try:
        projection = {"_id": 0, "champion_status": 0, "year_link": 0}
        query_new_champion = {"champion_status": "new_champions"}
        new_champion_data = list(
            xfinity_firstpage_collection.find(query_new_champion, projection)
        )

        query_old_champion = {"champion_status": "old_champions"}
        old_champion_data = list(
            xfinity_firstpage_collection.find(query_old_champion, projection)
        )
        return JSONResponse(
            content={
                "status": True,
                "new_champion_data": new_champion_data,
                "old_champion_data": old_champion_data,
            },
            status_code=200,
        )
    except Exception as e:
        return JSONResponse(
            content={"error": f"Error retrieving data: {str(e)}"}, status_code=500
        )


@app.delete("/delete_xfinity_data/{uuid}")
async def delete_xfinity_data(uuid: str):
    try:
        existing_document = xfinity_firstpage_collection.find_one({"year_uuid": uuid})

        if existing_document:
            xfinity_firstpage_collection.delete_one({"year_uuid": uuid})

            return {"message": "Data Deleted Successfully"}
        else:
            raise HTTPException(status_code=404, detail="Document not found")

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Internal Server Error: {str(e)}")


@app.post("/add_xfinity_data")
async def add_xfinity_data(xfinity_data: XfinityData = Body(...)):
    try:
        # Filter out None values
        filtered_data = {
            key: value
            for key, value in xfinity_data.dict().items()
            if value is not None
        }

        # Add UUID and created_at
        filtered_data["year_uuid"] = str(uuid.uuid4())
        filtered_data["created_at"] = datetime.utcnow().isoformat()

        inserted_data = xfinity_firstpage_collection.insert_one(filtered_data)

        # Fetch the inserted document
        inserted_document = xfinity_firstpage_collection.find_one(
            {"_id": inserted_data.inserted_id}
        )

        # Remove MongoDB ObjectId and return the result
        inserted_document.pop("_id", None)

        return {
            "message": "Data Inserted Successfully",
            "data": inserted_document,
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Internal Server Error: {str(e)}")


@app.put("/update_xfinity_data/{year_uuid}")
async def update_xfinity_data(year_uuid: str, updated_data: XfinityData):
    try:
        # Filter out None values from the updated data
        filtered_data = {
            key: value
            for key, value in updated_data.dict().items()
            if value is not None
        }

        # Update the document with the provided fields
        update_result = xfinity_firstpage_collection.update_one(
            {"year_uuid": year_uuid}, {"$set": filtered_data}
        )

        if update_result.modified_count == 1:
            # Fetch the updated document
            updated_document = xfinity_firstpage_collection.find_one(
                {"year_uuid": year_uuid}
            )
            # Remove MongoDB ObjectId and return the result
            updated_document.pop("_id", None)

            return {
                "message": "Data Updated Successfully",
                "data": updated_document,
            }
        elif update_result.matched_count == 0:
            raise HTTPException(
                status_code=404, detail="Data not found for the provided year_uuid"
            )
        else:
            raise HTTPException(status_code=500, detail="Failed to update data")

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Internal Server Error: {str(e)}")


########################################################## TRUCK #######################################################
@app.post("/auto-add-trucks")
async def add_xfinity(db: MongoClient = Depends(get_database)):
    trucks_script.sysInit()
    return JSONResponse(
        content={"status": True, "message": "data of turcks inserted successfully"},
        status_code=201,
    )


@app.get("/get-trucks/")
async def get_tracks_data(
    series_name: str = Query(None, title="Series Name"),
    db: MongoClient = Depends(get_database),
):
    try:
        projection = {"_id": 0, "year_link": 0}
        trucks_data = list(trucks_firstpage_collection.find({}, projection))
        return JSONResponse(
            content={
                "status": True,
                "trucks_data": trucks_data,
            },
            status_code=200,
        )
    except Exception as e:
        return JSONResponse(
            content={"error": f"Error retrieving data: {str(e)}"}, status_code=500
        )


@app.delete("/delete_truck_data/{uuid}")
async def delete_truck_data(uuid: str):
    try:
        existing_document = trucks_firstpage_collection.find_one({"year_uuid": uuid})

        if existing_document:
            trucks_firstpage_collection.delete_one({"year_uuid": uuid})

            return {"message": "Data Deleted Successfully"}
        else:
            raise HTTPException(status_code=404, detail="Document not found")

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Internal Server Error: {str(e)}")


@app.post("/add_truck_data")
async def add_truck_data(truck_data: TruckData = Body(...)):
    try:
        data_dict = truck_data.dict()

        data_dict["year_uuid"] = str(uuid.uuid4())
        data_dict["created_at"] = datetime.utcnow().isoformat()

        inserted_data = trucks_firstpage_collection.insert_one(data_dict)

        inserted_document = trucks_firstpage_collection.find_one(
            {"_id": inserted_data.inserted_id}
        )

        inserted_document.pop("_id", None)

        return {
            "message": "Data Inserted Successfully",
            "data": inserted_document,
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Internal Server Error: {str(e)}")


@app.put("/update_truck_data/{year_uuid}")
async def update_truck_data(year_uuid: str, truck_data: TruckData = Body(...)):
    try:
        data_dict = truck_data.dict()

        data_dict["year_uuid"] = year_uuid

        data_dict["created_at"] = datetime.utcnow().isoformat()

        result = trucks_firstpage_collection.update_one(
            {"year_uuid": year_uuid}, {"$set": data_dict}, upsert=True
        )

        if result.modified_count > 0 or result.upserted_id is not None:
            updated_document = trucks_firstpage_collection.find_one(
                {"year_uuid": year_uuid}
            )

            updated_document.pop("_id", None)

            return JSONResponse(
                content={
                    "message": "Data Updated Successfully",
                    "data": updated_document,
                },
                status_code=200,
            )
        else:
            raise HTTPException(status_code=404, detail="Document not found")

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Internal Server Error: {str(e)}")


########################################################## TRACK #######################################################
@app.post("/auto-upload-alltrack/")
async def auto_all_tracks_data(db: MongoClient = Depends(get_database)):
    track_script.sysInit()  # call script
    return JSONResponse(
        content={
            "status": True,
            "message": "Data of All Tracks 1st Page Inserted Successfully",
        },
        status_code=201,
    )


@app.get("/get-tracks/")
async def get_tracks_data(
    series_name: str = Query(None, title="Series Name"),
    db: MongoClient = Depends(get_database),
):
    try:
        projection = {"_id": 0, "link": 0}
        if series_name:
            query = {"series_name": series_name}
            tracks_data = list(track_firstpage_collection.find(query, projection))
        else:
            query = {}
            total_count = track_firstpage_collection.count_documents(query)
            limit = min(200, total_count)
            tracks_data = list(
                track_firstpage_collection.aggregate(
                    [
                        {"$match": query},
                        {"$sample": {"size": limit}},
                        {"$project": projection},
                    ]
                )
            )

        return JSONResponse(content={"tracks_data": tracks_data}, status_code=200)
    except Exception as e:
        return JSONResponse(
            content={"error": f"Error retrieving data: {str(e)}"}, status_code=500
        )


@app.delete("/delete_track_data/{uuid}")
async def delete_track_data(uuid: str):
    try:
        existing_document = track_firstpage_collection.find_one({"UUID": uuid})

        if existing_document:
            track_firstpage_collection.delete_one({"UUID": uuid})

            return {"message": "Data Deleted Successfully"}
        else:
            raise HTTPException(status_code=404, detail="Document not found")

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Internal Server Error: {str(e)}")


@app.post("/insert_tracks_data")
async def post_tracks_data(tracks_data: TracksData):
    try:
        data_dict = tracks_data.dict()

        data_dict["UUID"] = str(uuid.uuid4())

        data_dict["created_at"] = datetime.utcnow().isoformat()

        inserted_data = track_firstpage_collection.insert_one(data_dict)

        inserted_document = track_firstpage_collection.find_one(
            {"_id": inserted_data.inserted_id}
        )

        inserted_document.pop("_id", None)

        return JSONResponse(
            content={
                "message": "Data Inserted Successfully",
                "data": inserted_document,
            },
            status_code=200,
        )

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Internal Server Error: {str(e)}")


@app.put("/update_tracks_data/{UUID}")
async def update_tracks_data(UUID: str, tracks_data: TracksData):
    try:
        existing_document = track_firstpage_collection.find_one({"UUID": UUID})

        if existing_document:
            # Convert the provided data to a dictionary
            data_dict = tracks_data.dict()

            # Update the document with the provided data
            track_firstpage_collection.update_one({"UUID": UUID}, {"$set": data_dict})

            # Fetch the updated document
            updated_document = track_firstpage_collection.find_one({"UUID": UUID})

            # Remove MongoDB ObjectId and return the result
            updated_document.pop("_id", None)

            return JSONResponse(
                content={
                    "message": "Data Updated Successfully",
                    "data": updated_document,
                },
                status_code=200,
            )
        else:
            raise HTTPException(
                status_code=404, detail="Data not found for the provided UUID"
            )

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Internal Server Error: {str(e)}")


########################################################## ARCA #######################################################
@app.post("/api/auto-add-arca")
async def add_arca(db: MongoClient = Depends(get_database)):
    arca_script.sysInit()
    return JSONResponse(
        content={"status": True, "message": "data of ARCA inserted successfully"},
        status_code=201,
    )


@app.get("/api/get-arca/")
async def get_arca_data(
    series_name: str = Query(None, title="Series Name"),
    db: MongoClient = Depends(get_database),
):
    try:
        projection = {"_id": 0, "year_link": 0}
        arca_data = list(araca_firstpage_collection.find({}, projection))
        return JSONResponse(
            content={
                "status": True,
                "arca_data": arca_data,
            },
            status_code=200,
        )
    except Exception as e:
        return JSONResponse(
            content={"error": f"Error retrieving data: {str(e)}"}, status_code=500
        )


@app.delete("/delete_arca_data/{uuid}")
async def delete_arca_data(uuid: str):
    try:
        existing_document = araca_firstpage_collection.find_one({"year_uuid": uuid})

        if existing_document:
            araca_firstpage_collection.delete_one({"year_uuid": uuid})

            return {"message": "Data Deleted Successfully"}
        else:
            raise HTTPException(status_code=404, detail="Document not found")

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Internal Server Error: {str(e)}")


@app.post("/add_arca_data")
async def add_arca_data(data: ArcaData):
    try:
        # Convert the provided data to a dictionary
        data_dict = data.dict()

        # Add UUID and created_at
        data_dict["year_uuid"] = str(uuid.uuid4())
        data_dict["created_at"] = datetime.utcnow().isoformat()

        # Insert the data into the database
        inserted_data = araca_firstpage_collection.insert_one(data_dict)

        # Fetch the inserted document
        inserted_document = araca_firstpage_collection.find_one(
            {"_id": inserted_data.inserted_id}
        )

        # Remove MongoDB ObjectId and return the result
        inserted_document.pop("_id", None)

        return {
            "message": "Data Inserted Successfully",
            "data": inserted_document,
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Internal Server Error: {str(e)}")


@app.put("/update_data/{year_uuid}")
async def update_data(year_uuid: str, data: ArcaData):
    try:
        # Find the document with the given year_uuid
        existing_document = araca_firstpage_collection.find_one(
            {"year_uuid": year_uuid}
        )

        if existing_document:
            # Extract the fields from the updated data
            fields_to_update = data.dict(exclude_unset=True)

            # Update the document with the provided fields
            araca_firstpage_collection.update_one(
                {"year_uuid": year_uuid}, {"$set": fields_to_update}
            )

            # Fetch the updated document
            updated_document = araca_firstpage_collection.find_one(
                {"year_uuid": year_uuid}
            )

            # Remove MongoDB ObjectId and return the result
            updated_document.pop("_id", None)

            return {
                "message": "Data Updated Successfully",
                "data": updated_document,
            }
        else:
            raise HTTPException(
                status_code=404, detail="Data not found for the provided year_uuid"
            )

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Internal Server Error: {str(e)}")


########################################################## MODIFIED ##########################################################
@app.post("/api/auto-add-modified")
async def add_modified_data(db: MongoClient = Depends(get_database)):
    modified_script.sysInit()
    return JSONResponse(
        content={"status": True, "message": "data of Modified inserted successfully"},
        status_code=201,
    )


@app.get("/api/get-modified/")
async def get_modified_data(
    series_name: str = Query(None, title="Series Name"),
    db: MongoClient = Depends(get_database),
):
    try:
        projection = {"_id": 0, "year_link": 0}
        arca_data = list(modified_firstpage_collection.find({}, projection))
        return JSONResponse(
            content={
                "status": True,
                "arca_data": arca_data,
            },
            status_code=200,
        )
    except Exception as e:
        return JSONResponse(
            content={"error": f"Error retrieving data: {str(e)}"}, status_code=500
        )


@app.delete("/delete_modified_data/{uuid}")
async def delete_modified_data(uuid: str):
    try:
        existing_document = modified_firstpage_collection.find_one({"year_uuid": uuid})

        if existing_document:
            modified_firstpage_collection.delete_one({"year_uuid": uuid})

            return {"message": "Data Deleted Successfully"}
        else:
            raise HTTPException(status_code=404, detail="Document not found")

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Internal Server Error: {str(e)}")


@app.post("/add_modified_data")
async def add_modified_data(modified_data: ModifiedData):
    try:
        # Convert the provided data to a dictionary
        data_dict = modified_data.dict()

        # Add UUID and created_at
        data_dict["year_uuid"] = str(uuid.uuid4())
        data_dict["created_at"] = datetime.utcnow().isoformat()

        inserted_data = modified_firstpage_collection.insert_one(data_dict)

        # Fetch the inserted document
        inserted_document = modified_firstpage_collection.find_one(
            {"_id": inserted_data.inserted_id}
        )

        # Remove MongoDB ObjectId and return the result
        inserted_document.pop("_id", None)

        return {
            "message": "Data Inserted Successfully",
            "data": inserted_document,
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Internal Server Error: {str(e)}")


@app.put("/update_modified_data/{year_uuid}")
async def update_modified_data(year_uuid: str, updated_data: ModifiedData):
    try:
        # Find the document with the given year_uuid
        existing_document = modified_firstpage_collection.find_one(
            {"year_uuid": year_uuid}
        )

        if existing_document:
            # Update the existing document with the new data
            updated_data_dict = updated_data.dict(exclude_unset=True)
            modified_firstpage_collection.update_one(
                {"year_uuid": year_uuid}, {"$set": updated_data_dict}
            )

            # Fetch the updated document
            updated_document = modified_firstpage_collection.find_one(
                {"year_uuid": year_uuid}
            )

            # Remove MongoDB ObjectId and return the result
            updated_document.pop("_id", None)

            return {
                "message": "Data Updated Successfully",
                "data": updated_document,
            }
        else:
            raise HTTPException(
                status_code=404, detail="Data not found for the provided year_uuid"
            )

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Internal Server Error: {str(e)}")


@app.post("/add-tracks-page2/")
async def add_tracks_page2(db: MongoClient = Depends(get_database)):
    try:
        track_script_page2.sysInit()

        return JSONResponse(
            content={
                "status": True,
                "message": "Data Inserted Successfully . . . ",
            },
            status_code=200,
        )
    except Exception as e:
        return JSONResponse(
            content={"error": f"Error retrieving data: {str(e)}"}, status_code=500
        )


@app.get("/get-tracks-innerdata/{track_uuid}")
async def get_track_innerdata(track_uuid: str, db: MongoClient = Depends(get_database)):
    try:
        # projection = {"_id": 0, "track_page_link": 0, "track_uuid":0}
        projection = {"_id": 0, "track_page_link": 0}

        track_inner_page_alldata = db["track_page_2"].find(
            {"track_uuid": track_uuid}, projection=projection
        )
        return list(track_inner_page_alldata)

    except Exception as e:
        return JSONResponse(
            content={"error": f"Error retrieving data: {str(e)}"}, status_code=500
        )


@app.get("/get-multiple-rider-data/")
async def get_rider_data(
    driver_name: str = Query(..., alias="driver_name"),
    db: MongoClient = Depends(get_database),
):
    try:
        print(f"Requested Driver Name: {driver_name}")

        # Modify the query to match the driver's name with a case-insensitive regex
        rider_info_cursor = db["driver_table_page_1"].find(
            {"Driver": {"$regex": f"^{driver_name}$", "$options": "i"}}
        )

        # Additional logging
        print(f"Actual MongoDB query: {str(rider_info_cursor)}")

        rider_info_list = list(rider_info_cursor)
        print(f"Retrieved rider_info_list: {rider_info_list}")

        if rider_info_list:
            rider_data_list = []
            for rider_info in rider_info_list:
                rider_name = rider_info.get("Driver", "")
                csv_file_name = rider_info.get("CSVFileName", "")
                csv_data = rider_info.get("CSVData", [])
                cleaned_csv_data = clean_csv_data(csv_data, csv_file_name)

                # Remove "TABLE_NAME___" prefix from table_name
                table_name = cleaned_csv_data[0]["table_name"].replace(
                    "TABLE_NAME___", ""
                )

                cleaned_csv_data_no_table_name = [
                    {key: value for key, value in record.items() if key != "table_name"}
                    for record in cleaned_csv_data
                ]

                rider_data_list.append(
                    {
                        "rider_name": rider_name,
                        "table_name": table_name,
                        "csv_data": cleaned_csv_data_no_table_name,
                    }
                )

            return rider_data_list
        else:
            raise HTTPException(
                status_code=404,
                detail="COMMING SOON!",
            )

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


if __name__ == "__main__":
    uvicorn.run("main:app", host="0.0.0.0", port=9002, reload=True)