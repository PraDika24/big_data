from langdetect import detect, DetectorFactory
from collect_data.connection import get_db

DetectorFactory.seed = 0

def is_english(text):
    try:
        return detect(text) == "en"
    except:
        return False

def filter_english():
    """Fungsi utama untuk membersihkan komentar-komentar YouTube dari DB kotor ke DB bersih."""
    db_source = get_db("db_data_kotor")
    db_target = get_db("db_filter_english")
    collections = [f"video_{i}" for i in range(1, 9)]

    for collection_name in collections:
        print(f"\n🚀 Memproses koleksi {collection_name}...")
        source_collection = db_source[collection_name]
        target_collection = db_target[collection_name]

        documents = source_collection.find()


        for doc in documents:
            try:
                snippet = doc["snippet"]["topLevelComment"]["snippet"]
                video_id = doc["snippet"]["videoId"]
                author_name = snippet["authorDisplayName"]
                comment_text = snippet["textDisplay"]
                published_at = snippet["publishedAt"]
                updated_at = snippet["updatedAt"]

               
                if is_english(comment_text):
                    cleaned_doc = {
                        "video_id": video_id,
                        "author_name": author_name,
                        "comment_text": comment_text,
                        "published_at": published_at,
                        "updated_at": updated_at
                    }

                    target_collection.insert_one(cleaned_doc)

            except KeyError as e:
                print(f"⚠️ Dokumen tidak lengkap di {collection_name}: {e}")
                continue

        print(f"✅ Selesai memproses {collection_name}:")

if __name__ == "__main__":
    filter_english()
    print("\n🎉 Semua koleksi selesai diproses.")