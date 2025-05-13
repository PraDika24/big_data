# connection.py

import os
from pymongo import MongoClient
from dotenv import load_dotenv
from pathlib import Path

# Ambil path .env dari parent folder (sesuaikan dengan struktur proyekmu)
dotenv_path = Path(__file__).resolve().parents[1] / ".env"
load_dotenv(dotenv_path)

def get_db():
    mongo_uri = os.getenv("MONGO_URI")
    client = MongoClient(mongo_uri)
    db = client["youtube_sentiment"]  # Ganti sesuai kebutuhan
    return db
