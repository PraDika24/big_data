import os
from pymongo import MongoClient
from dotenv import load_dotenv
from pathlib import Path

# Ambil path root project, lalu cari .env di sana
dotenv_path = Path(__file__).resolve().parents[1] / ".env"
load_dotenv(dotenv_path)

# Ambil URI dari .env
MONGO_URI = os.getenv("MONGO_URI")

def get_database():
    if not MONGO_URI:
        raise Exception("MONGO_URI tidak ditemukan di file .env")

    client = MongoClient(MONGO_URI)
    db_name = MONGO_URI.split("/")[-1].split("?")[0]
    return client[db_name]
