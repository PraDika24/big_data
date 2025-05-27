import os
from pathlib import Path
from dotenv import load_dotenv
from connection import get_db
import googleapiclient.discovery
from pymongo.errors import DuplicateKeyError
import uuid

# Load .env
dotenv_path = Path(__file__).resolve().parents[0] / ".env"
load_dotenv(dotenv_path)

# Setup YouTube API
api_key = os.getenv("YOUTUBE_API_KEY")
youtube = googleapiclient.discovery.build("youtube", "v3", developerKey=api_key)

# Daftar ID Video
VIDEO_IDS = [
    "fK85SQzm0Z0",
    "K8d4n9GhBrg",
    "zBL_5DkiXCk",
    "g4TDjwPRArg",
    "QtIlD6uxwwk",
    "oNS8PHxWdp8",
    "zTW20UFOVbo",
    "7cAzQjnEqXs"
]

def fetch_comments(video_id, collection_name, db, max_comments=15000):
    collection = db[collection_name]
    collection.create_index("id", unique=True)  # Indeks untuk cek duplikasi

    next_page_token = None
    total_fetched = 0

    while total_fetched < max_comments:
        request = youtube.commentThreads().list(
            part="snippet,replies",  # Ambil snippet dan replies
            videoId=video_id,
            maxResults=100,
            pageToken=next_page_token,
            textFormat="plainText"
        )
        response = request.execute()

        for item in response.get("items", []):
            # Siapkan dokumen top-level comment
            comment_thread = item
            top_comment_id = item["snippet"]["topLevelComment"]["id"]
            
            # Inisialisasi field replies jika ada sub-komentar
            total_reply = item["snippet"].get("totalReplyCount", 0)
            if total_reply > 0:
                reply_token = None
                all_replies = []
                
                # Ambil semua sub-komentar
                while True:
                    reply_request = youtube.comments().list(
                        part="snippet",
                        parentId=top_comment_id,
                        maxResults=100,
                        pageToken=reply_token,
                        textFormat="plainText"
                    )
                    reply_response = reply_request.execute()

                    for reply in reply_response.get("items", []):
                        all_replies.append(reply)

                    reply_token = reply_response.get("nextPageToken")
                    if not reply_token:
                        break

                # Tambahkan semua sub-komentar ke field replies
                comment_thread["replies"] = {"comments": all_replies}
            else:
                # Jika tidak ada sub-komentar, hapus field replies atau biarkan kosong
                comment_thread.pop("replies", None)

            # Simpan dokumen lengkap ke MongoDB
            try:
                collection.insert_one(comment_thread)
                total_fetched += 1
            except DuplicateKeyError:
                pass

        next_page_token = response.get("nextPageToken")
        if not next_page_token:
            break

        print(f"✅ Fetched {total_fetched} raw comment threads so far for {collection_name}")

    print(f"🎉 DONE: {total_fetched} total raw comment threads for {collection_name}")

# Main Eksekusi
if __name__ == "__main__":
    db = get_db("db_data_kotor")
    for idx, vid in enumerate(VIDEO_IDS, start=1):
        collection_name = f"video_{idx}"
        print(f"\n🚀 Mulai mengambil komentar dari video urutan ke-{idx}...")
        fetch_comments(vid, collection_name, db)
    print("\n✅ Semua video selesai diproses.")