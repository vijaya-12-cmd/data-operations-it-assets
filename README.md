# data-operations-it-assets
Mini project 

Phase 1: 

Cleaned data: Downloaded .csv file and opened this file in excel and cleaned data by "removing duplicates" by hostname based, changed date format as yyyyy-mm-dd, removed white spaces by using "Trim" function, And replaced empty cells with keyword "UNKNOWN" by using find and replace

Phase 2:

Developed python code(index_data.py) for uploading cleaned data to elastic search index. And tested in elastic by using few queries to ensure data indexed successfully or not . Example "GET mini_project_index/_search"

phase 3: 

Step:1 
Reindex data to another index and checked in elastic search
Query in elastic for confirmation of reindexed
GET /mini_project_transformed/_search

step:2  Added a derived field: 
o risk_level = "High" if operating_system_lifecycle_status is "EOL" 
or "EOS", 
else "Low". 
**test Query:**

  "_source": ["hostname", "operating_system_lifecycle_status", "risk_level"],
  "query": {
    "exists": {
      "field": "risk_level"
    }
  },
  "size": 10
}

step: 3
Calculated system age (in years) from the installation date.
Test Query
POST mini_project_transformed/_search
{
  "_source": ["hostname", "operating_system_installation_date", "system_age"],
  "query": {
    "exists": {
      "field": "system_age"
    }
  },
  "size": 10
}

   
5. Delete records that have missing hostnames or Unknown providers. 
6. Update existing records with the new fields using _update_by_query. 

phase 4:

 Export or view data in Kibana. 
2. Created charts such as: 
o Assets by Country 
o Lifecycle Status Distribution 
o High vs Low Risk Assets 
o Top OS Providers 
3. Saved screenshots of my dashboards in a folder: 
datavisualization/ 
given final report with suggesstion of upgrade or migrate OS for EOS and EOL
