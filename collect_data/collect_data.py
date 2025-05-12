import os
from pathlib import Path
import googleapiclient.discovery
from dotenv import load_dotenv
from connection import get_db
from datetime import datetime

# Ambil path root project, lalu cari .env di sana
dotenv_path = Path(__file__).resolve().parents[1] / ".env"
load_dotenv(dotenv_path)

# API setup
api_key = os.getenv("YOUTUBE_API_KEY")
youtube = googleapiclient.discovery.build("youtube", "v3", developerKey=api_key)

# Video IDs yang ingin kamu ambil
VIDEO_IDS = [
    "video_id_1",
    "video_id_2",
    "video_id_3",
    "video_id_4",
    "video_id_5",
    "video_id_6",
    "video_id_7",
    "video_id_8"
]

def fetch_comments(video_id, db):
    collection = db[f"video_{video_id}"]  # Koleksi per video

    next_page_token = None
    while True:
        request = youtube.commentThreads().list(
            part="snippet",
            videoId=video_id,
            maxResults=100,
            pageToken=next_page_token,
            textFormat="plainText"
        )
        response = request.execute()

        for item in response["items"]:
            snippet = item["snippet"]
            top_comment = snippet["topLevelComment"]["snippet"]

            comment_data = {
                "video_id": video_id,
                "comment_id": item["id"],
                "text": top_comment["textOriginal"],
                "author": top_comment["authorDisplayName"],
                "like_count": top_comment["likeCount"],
                "published_at": top_comment["publishedAt"],
                "retrieved_at": datetime.utcnow().isoformat(),
                "raw": item  # simpan semua data mentah juga
            }

            collection.insert_one(comment_data)

        next_page_token = response.get("nextPageToken")
        if not next_page_token:
            break

if __name__ == "__main__":
    db = get_db()
    for vid in VIDEO_IDS:
        print(f"Mengambil komentar untuk video {vid}...")
        fetch_comments(vid, db)
    print("Selesai mengambil komentar.")
