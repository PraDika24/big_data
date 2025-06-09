import string
import emoji
import re
import pymongo
import time
from langdetect import detect, DetectorFactory
from multiprocessing import Pool, cpu_count
from collect_data.connection import get_db

DetectorFactory.seed = 0

CUSTOM_EMOJI_PATTERNS = [
    r'\b(?:smiling|face|eyes|tears|lol|clap|fire|heart|laugh|rolling|cry|joy|wow|omg|cool|grin|blush|wink|sad|angry|love|shock|sleep|zzz|party|thinking|Thicc|BenNo|BenYes|Gyatt|WellWell|Sewey2|Demon|GodisGood|Banana|Box|football|Gay|harold|LLL|pikatchu|sus|monkey|uno|TIMEOUT)[a-z]*\b'
]

def remove_custom_youtube_emoji(text):
    for pattern in CUSTOM_EMOJI_PATTERNS:
        text = re.sub(pattern, '', text, flags=re.IGNORECASE)
    return text

def clean_text(text):
    text = text.lower()
    text = emoji.replace_emoji(text, replace='')
    text = re.sub(r'http\S+|www\S+|https\S+', '', text)
    text = re.sub(r'@\w+|#\w+', '', text)
    text = text.translate(str.maketrans("", "", string.punctuation))
    text = remove_custom_youtube_emoji(text)
    text = ' '.join(text.split())
    return text.strip()

def is_english(text):
    try:
        return detect(text) == "en"
    except:
        return False

def is_spam(text):
    spam_keywords = {
        "subscribe", "visit my channel", "check my channel", "follow me", "watch my video",
        "support me", "free money", "win prize", "get rich", "earn cash", "free gift",
        "click here", "claim now", "link in bio", "see below", "more info", "check link",
        "visit link", "pls like", "spam alert", "bot comment", "auto comment", "buy now",
        "shop here", "deal today", "limited offer", "exclusive content", "giveaway", "promo code",
        "fast cash", "big win", "you won", "click this", "don't miss", "make money", "cash app",
        "bitcoin giveaway", "100% free", "dm me", "message me", "contact me", "act now",
        "urgent offer", "join now", "download now", "hot girls", "xxx", "onlyfans", "telegram group",
        "vip access", "earn bitcoin", "credit card", "loan approval", "investment opportunity",
        "referral code", "deal", "100% legit", "check bio", "cheap price",
        "lowest price", "guaranteed", "earn daily", "instantly rich",
        "get followers", "boost followers", "click the link", "like back", "real", "legit",
        "don't skip", "no scam", "real account", "free followers", "click below", "amazing offer",
        "too good to miss", "sponsored post", "giveaway now", "investment tips", "buy crypto",
        "free promo", "vip group", "early access", "tap the link", "unlock content",
        "sign up now", "get verified", "sfs", "f4f", "l4l"
    }

    words = text.split()
    if len(words) < 2:
        return True
    if re.match(r'^(?:[\s\w]*(?:\W)\s*)+$', text):
        return True
    if any(keyword in text for keyword in spam_keywords):
        return True
    return False

def process_collection(collection_name, batch_size=1000):
    db_source = get_db("db_data_kotor")
    db_target = get_db("db_data_bersih_2")
    source_collection = db_source[collection_name]
    target_collection = db_target[collection_name]

    print(f"\n⚙️  Memproses koleksi: {collection_name}")
    start_time = time.time()

    try:
        target_collection.create_index("comment_text", unique=True)
    except Exception as e:
        print(f"⚠️  Gagal membuat indeks: {e}")

    documents = source_collection.find()
    batch = []

    total_processed = 0
    total_english = 0
    total_spam = 0
    total_duplicates = 0
    total_saved = 0

    for doc in documents:
        try:
            video_id = doc["snippet"]["videoId"]
            author_name = doc["snippet"]["topLevelComment"]["snippet"]["authorDisplayName"]
            comment_text = doc["snippet"]["topLevelComment"]["snippet"]["textDisplay"]
            published_at = doc["snippet"]["topLevelComment"]["snippet"]["publishedAt"]
            updated_at = doc["snippet"]["topLevelComment"]["snippet"]["updatedAt"]

            cleaned_comment = clean_text(comment_text)

            if not cleaned_comment or is_spam(cleaned_comment):
                total_spam += 1
                total_processed += 1
                continue

            if not is_english(cleaned_comment):
                total_processed += 1
                continue

            cleaned_doc = {
                "video_id": video_id,
                "author_name": author_name,
                "comment_text": cleaned_comment,
                "published_at": published_at,
                "updated_at": updated_at
            }

            batch.append(cleaned_doc)

            if len(batch) >= batch_size:
                try:
                    target_collection.insert_many(batch, ordered=False)
                    total_saved += len(batch)
                except pymongo.errors.BulkWriteError as e:
                    total_duplicates += len(e.details.get("writeErrors", []))
                    total_saved += len(batch) - total_duplicates
                batch = []

            total_processed += 1

        except KeyError as e:
            continue

    # insert sisa batch terakhir
    if batch:
        try:
            target_collection.insert_many(batch, ordered=False)
            total_saved += len(batch)
        except pymongo.errors.BulkWriteError as e:
            total_duplicates += len(e.details.get("writeErrors", []))
            total_saved += len(batch) - total_duplicates

    duration = time.time() - start_time
    print(f"✅ {collection_name} selesai: {total_saved}/{total_processed} komentar disimpan.")
    print(f"   Spam: {total_spam}, Duplikat: {total_duplicates}, Waktu: {duration:.2f} detik")

def clean_comments_parallel():
    collections = [f"video_{i}" for i in range(1, 9)]
    with Pool(processes=cpu_count()) as pool:
        pool.map(process_collection, collections)

if __name__ == "__main__":
    clean_comments_parallel()
    print("\n🎉 Semua koleksi selesai diproses secara paralel.")
