import os
from pathlib import Path
from dotenv import load_dotenv
from datetime import datetime
from connection import get_db
import googleapiclient.discovery

# Load .env
dotenv_path = Path(__file__).resolve().parents[0] / ".env"
load_dotenv(dotenv_path)

# Setup API
api_key = os.getenv("YOUTUBE_API_KEY")
youtube = googleapiclient.discovery.build("youtube", "v3", developerKey=api_key)

# List Video IDs (8 video)
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

# Fungsi ambil komentar (hingga 3000 per video)
def fetch_comments(video_id, db, max_comments=3000):
    collection = db[f"video_{video_id}"]
    next_page_token = None
    total_fetched = 0

    while total_fetched < max_comments:
        request = youtube.commentThreads().list(
            part="snippet",
            videoId=video_id,
            maxResults=100,
            pageToken=next_page_token,
            textFormat="plainText"
        )
        response = request.execute()

        comments = []
        for item in response.get("items", []):
            snippet = item["snippet"]
            top_comment = snippet["topLevelComment"]["snippet"]

            comment_data = {
                "video_id": video_id,
                "comment_id": item["id"],
                "text": top_comment.get("textOriginal", ""),
                "author": top_comment.get("authorDisplayName", ""),
                "like_count": top_comment.get("likeCount", 0),
                "published_at": top_comment.get("publishedAt", ""),
                "retrieved_at": datetime.utcnow().isoformat(),
            }

            comments.append(comment_data)

        if comments:
            collection.insert_many(comments)
            total_fetched += len(comments)
            print(f"✅ Fetched {total_fetched} comments for video {video_id}")

        next_page_token = response.get("nextPageToken")
        if not next_page_token:
            break

    print(f"🎉 Selesai ambil {total_fetched} komentar untuk video {video_id}")

# Main execution
if __name__ == "__main__":
    db = get_db()
    for vid in VIDEO_IDS:
        print(f"🚀 Mengambil komentar dari video {vid}...")
        fetch_comments(vid, db)
    print("✅ Semua video selesai diproses.")

