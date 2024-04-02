from pymongo import MongoClient

#  mongodb+srv://usman:shoaibbilal@fastapi.momn8bp.mongodb.net/?retryWrites=true&w=majority

def get_mongo_connection():
    PASSWORD = "shoaibbilal"
    URI = f"mongodb+srv://usman:{PASSWORD}@fastapi.momn8bp.mongodb.net/?retryWrites=true&w=majority"
    client = MongoClient(URI)
    db = client["racing-reference"]
    return db



def get_database():
    PASSWORD = "shoaibbilal"
    URI = f"mongodb+srv://usman:{PASSWORD}@fastapi.momn8bp.mongodb.net/?retryWrites=true&w=majority"
    client = MongoClient(URI)
    db = client["racing-reference"]
    return db


#### CONNECT LIVE MONGO DATABASE
# PASSWORD = "shoaibbilal"
# URI = f"mongodb+srv://usman:{PASSWORD}@fastapi.momn8bp.mongodb.net/?retryWrites=true&w=majority"
# client = MongoClient(URI)
# db = client["racing-reference"]

#### CONNECT LOCAL MONGO DATABASE
# CONNECT_TIMEOUT_MS = 900000
#     SOCKET_TIMEOUT_MS = 900000
#     client = MongoClient(
#         "mongodb://localhost:27017/",
#         connectTimeoutMS=CONNECT_TIMEOUT_MS,
#         socketTimeoutMS=SOCKET_TIMEOUT_MS,
#     )
#     db = client["racing-reference"]
