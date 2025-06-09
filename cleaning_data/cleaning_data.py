import string
import emoji
import re
import pymongo
from langdetect import detect, DetectorFactory
from collect_data.connection import get_db

# Untuk hasil deteksi bahasa yang konsisten
DetectorFactory.seed = 0

# Pola regex untuk mendeteksi emoji custom dari YouTube Membership
CUSTOM_EMOJI_PATTERNS = [
    r'\b(?:smiling|face|eyes|tears|lol|clap|fire|heart|laugh|rolling|cry|joy|wow|omg|cool|grin|blush|wink|sad|angry|love|shock|sleep|zzz|party|thinking|thicc|benno|benyes|gyatt|wellwell|sewey2|demon|godisgood|banana|box|football|gay|harold|lll|pikatchu|sus|monkey|uno|timeout)[a-z0-9]*\b',
    r'(?::[a-zA-Z0-9_]+:)'  # format :emoji:
]

def remove_custom_youtube_emoji(text):
    """Menghapus emoji custom yang berasal dari YouTube Membership."""
    for pattern in CUSTOM_EMOJI_PATTERNS:
        text = re.sub(pattern, '', text, flags=re.IGNORECASE)
    return text

def clean_text(text):
    """Bersihkan komentar: huruf kecil, emoji unicode, URL, mention, hashtag, tanda baca, spasi berlebih, emoji custom."""
    text = text.lower()
    text = emoji.replace_emoji(text, replace='')  # Hapus emoji Unicode
    text = re.sub(r'http\S+|www\S+|https\S+', '', text)  # Hapus URL
    text = re.sub(r'@\w+', '', text)  # Hapus mention
    text = re.sub(r'#\w+', '', text)  # Hapus hashtag
    text = text.translate(str.maketrans("", "", string.punctuation))  # Hapus tanda baca
    text = remove_custom_youtube_emoji(text)  # Hapus emoji membership
    text = ' '.join(text.split())  # Hapus spasi berlebih
    return text.strip()

def is_english(text):
    """Deteksi apakah teks dalam Bahasa Inggris."""
    try:
        return detect(text) == "en"
    except:
        return False

def is_spam(text):
    """Deteksi spam berdasarkan pola umum promosi, ajakan, dan format mencurigakan."""
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

    # Komentar terlalu pendek atau hanya karakter non-alfabet
    if len(words) < 2 or re.match(r'^(?:[\s\w]*(?:\W)\s*)+$', text):
        return True

    if any(keyword in text for keyword in spam_keywords):
        return True

    return False

def clean_comments():
    """Fungsi utama untuk membersihkan komentar-komentar YouTube dari DB kotor ke DB bersih."""
    db_source = get_db("db_data_kotor")
    db_target = get_db("db_data_bersih_2")
    collections = [f"video_{i}" for i in range(1, 9)]

    for collection_name in collections:
        print(f"\n🚀 Memproses koleksi {collection_name}...")
        source_collection = db_source[collection_name]
        target_collection = db_target[collection_name]

        try:
            target_collection.create_index("comment_text", unique=True)
        except Exception as e:
            print(f"⚠️ Gagal membuat indeks unik di {collection_name}: {e}")

        documents = source_collection.find()
        total_processed = 0
        total_english = 0
        total_spam = 0
        total_duplicates = 0

        for doc in documents:
            try:
                snippet = doc["snippet"]["topLevelComment"]["snippet"]
                video_id = doc["snippet"]["videoId"]
                author_name = snippet["authorDisplayName"]
                comment_text = snippet["textDisplay"]
                published_at = snippet["publishedAt"]
                updated_at = snippet["updatedAt"]

                cleaned_comment = clean_text(comment_text)

                if is_spam(cleaned_comment):
                    total_spam += 1
                    total_processed += 1
                    continue

                if cleaned_comment and is_english(cleaned_comment):
                    cleaned_doc = {
                        "video_id": video_id,
                        "author_name": author_name,
                        "comment_text": cleaned_comment,
                        "published_at": published_at,
                        "updated_at": updated_at
                    }

                    try:
                        target_collection.insert_one(cleaned_doc)
                        total_english += 1
                    except pymongo.errors.DuplicateKeyError:
                        total_duplicates += 1

                total_processed += 1

            except KeyError as e:
                print(f"⚠️ Dokumen tidak lengkap di {collection_name}: {e}")
                continue
            except Exception as ex:
                print(f"❌ Error saat memproses komentar: {ex}")
                continue

        print(f"✅ Selesai memproses {collection_name}:")
        print(f"   Disimpan: {total_english}/{total_processed}, Spam: {total_spam}, Duplikat: {total_duplicates}")

if __name__ == "__main__":
    clean_comments()
    print("\n🎉 Semua koleksi selesai diproses.")
