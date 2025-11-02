from elasticsearch import Elasticsearch

# === CONFIGURATION ===
ES_ENDPOINT = "https://100eb59edc024b3d9e5380e2c6db5aec.us-central1.gcp.cloud.es.io:443"
ES_API_KEY = "SUxTSUw1b0JBbno5R3ExR0pzbXo6UTZkOXBhSzZNODVnU1ZVNDRuOFotZw=="
SOURCE_INDEX = "miniproject_target_index"
TARGET_INDEX = "miniproject_reindexed2"

# === CONNECT TO ELASTICSEARCH ===
es = Elasticsearch(
    ES_ENDPOINT,
    api_key=ES_API_KEY,
    verify_certs=True
)

# === STEP 1: REINDEX ===
reindex_body = {
    "source": {"index": SOURCE_INDEX},
    "dest": {"index": TARGET_INDEX}
}
es.reindex(body=reindex_body, wait_for_completion=True)
print("✅ Reindexing completed.")

# === STEP 2 & 3: Add 'risk_level' and 'system_age_years' ===
update_script = {
    "script": {
        "lang": "painless",
        "source": """
            if (ctx._source.containsKey('operating_system_lifecycle_status')) {
                def status = ctx._source.operating_system_lifecycle_status;
                ctx._source.risk_level = (status == 'EOL' || status == 'EOS') ? 'High' : 'Low';
            } else {
                ctx._source.risk_level = 'Low';
            }

            if (ctx._source.containsKey('installation_date')) {
                try {
                    def sdf = new SimpleDateFormat('yyyy-MM-dd');
                    def installDate = sdf.parse(ctx._source.installation_date);
                    def now = new Date();
                    def age = (now.getTime() - installDate.getTime()) / (1000L * 60 * 60 * 24 * 365);
                    ctx._source.system_age_years = Math.floor(age);
                } catch (Exception e) {
                    ctx._source.system_age_years = null;
                }
            }
        """
    }
}
es.update_by_query(index=TARGET_INDEX, body=update_script, refresh=True, conflicts="proceed")
print("✅ Updated documents with 'risk_level' and 'system_age_years'.")

# === STEP 4: Delete records with missing 'hostname' or 'Unknown' provider ===
delete_query = {
    "query": {
        "bool": {
            "should": [
                {"bool": {"must_not": {"exists": {"field": "hostname"}}}},
                {"match": {"provider": "Unknown"}}
            ]
        }
    }
}
es.delete_by_query(index=TARGET_INDEX, body=delete_query, refresh=True)
print("✅ Deleted records with missing hostname or unknown provider.")