from elasticsearch import Elasticsearch, helpers
import pandas as pd
import os

# === CONFIGURATION ===
ES_ENDPOINT = "https://100eb59edc024b3d9e5380e2c6db5aec.us-central1.gcp.cloud.es.io:443"
ES_API_KEY = "SUxTSUw1b0JBbno5R3ExR0pzbXo6UTZkOXBhSzZNODVnU1ZVNDRuOFotZw=="
CSV_FILE = "C:/Users/VijayaBugga/Desktop/it_asset_inventory_cleaned_updated.csv"  # e.g., "elastic_exports/my_index.csv"
TARGET_INDEX = "miniproject_target_index"

# === CONNECT TO ELASTIC ===
es = Elasticsearch(
    ES_ENDPOINT,
    api_key=ES_API_KEY,
    verify_certs=True
)

# === CHECK CONNECTION ===
if not es.ping():
    print("❌ Connection failed! Please check endpoint or API key.")
    exit()
else:
    print("✅ Connected to Elasticsearch!")

# === READ CSV FILE ===
if not os.path.exists(CSV_FILE):
    print(f"❌ CSV file not found: {CSV_FILE}")
    exit()

df = pd.read_csv(CSV_FILE)
print(f"📄 Loaded {len(df)} records from {CSV_FILE}")

# === PREPARE BULK DATA ===
actions = [
    {
        "_index": TARGET_INDEX,
        "_source": row.to_dict()
    }
    for _, row in df.iterrows()
]

# === BULK UPLOAD ===
try:
    helpers.bulk(es, actions)
    print(f"✅ Successfully uploaded {len(actions)} documents to index '{TARGET_INDEX}'")
except Exception as e:
    print(f"❌ Bulk upload failed: {e}")