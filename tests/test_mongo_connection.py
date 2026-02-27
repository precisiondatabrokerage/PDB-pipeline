from pymongo import MongoClient
import os
from dotenv import load_dotenv

load_dotenv()

MONGODB_URI = os.getenv("MONGODB_URI")
MONGO_DB = os.getenv("MONGO_DB_NAME") or os.getenv("MONGODB_DB")
MONGO_COLLECTION = os.getenv("MONGODB_COMPANY_COLLECTION")

print("URI:", MONGODB_URI)
print("DB:", MONGO_DB)
print("Collection:", MONGO_COLLECTION)

client = MongoClient(MONGODB_URI)
db = client[MONGO_DB]
collection = db[MONGO_COLLECTION]

doc = {
    "test_entry": True,
    "message": "If you see this in MongoDB, the connection works."
}

result = collection.insert_one(doc)
print("Inserted ID:", result.inserted_id)
print("Done.")
