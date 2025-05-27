import string
import emoji
from langdetect import detect, DetectorFactory
from collect_data.connection import get_db

# Untuk memastikan hasil deteksi bahasa konsisten
DetectorFactory.seed = 0

def clean_text(text):
    """Menghilangkan tanda baca dan emotikon dari teks."""
    # Hilangkan emotikon
    text = emoji.replace_emoji(text, replace="")
    # Hilangkan tanda baca
    text = text.translate(str.maketrans("", "", string.punctuation))
    return text.strip()

def is_english(text):
    """Cek apakah teks berbahasa Inggris."""
    try:
        return detect(text) == "en"
    except:
        return False

def clean_comments():
    # Koneksi ke database
    db_source = get_db("db_data_kotor")  # Database sumber
    db_target = get_db("db_data_bersih")  # Database tujuan untuk data bersih

    # Daftar koleksi (video_1, video_2, ..., video_8)
    collections = [f"video_{i}" for i in range(1, 9)]

    for collection_name in collections:
        print(f"\n🚀 Memproses koleksi {collection_name}...")
        source_collection = db_source[collection_name]
        target_collection = db_target[collection_name]

        # Ambil semua dokumen dari koleksi sumber
        documents = source_collection.find()

        total_processed = 0
        total_english = 0

        for doc in documents:
            # Ambil data yang dibutuhkan
            try:
                video_id = doc["snippet"]["videoId"]
                author_name = doc["snippet"]["topLevelComment"]["snippet"]["authorDisplayName"]
                comment_text = doc["snippet"]["topLevelComment"]["snippet"]["textDisplay"]
                published_at = doc["snippet"]["topLevelComment"]["snippet"]["publishedAt"]
                updated_at = doc["snippet"]["topLevelComment"]["snippet"]["updatedAt"]

                # Bersihkan komentar
                cleaned_comment = clean_text(comment_text)

                # Cek apakah komentar berbahasa Inggris
                if cleaned_comment and is_english(cleaned_comment):
                    # Siapkan dokumen baru untuk disimpan
                    cleaned_doc = {
                        "video_id": video_id,
                        "author_name": author_name,
                        "comment_text": cleaned_comment,
                        "published_at": published_at,
                        "updated_at": updated_at
                    }

                    # Simpan ke koleksi tujuan
                    target_collection.insert_one(cleaned_doc)
                    total_english += 1

                total_processed += 1

            except KeyError as e:
                print(f"⚠️ Dokumen tidak lengkap di {collection_name}: {e}")
                continue

        print(f"✅ Selesai memproses {collection_name}: {total_english}/{total_processed} komentar berbahasa Inggris disimpan.")

if __name__ == "__main__":
    clean_comments()
    print("\n🎉 Semua koleksi selesai diproses.")