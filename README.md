# data-operations-it-assets
Mini project 
Phase 1: Cleaned data: Downloaded .csv file and opened this file in excel and cleaned data by "removing duplicates" by hostname based, changed date format as yyyyy-mm-dd, removed white spaces by using "Trim" function, And replaced empty cells with keyword "UNKNOWN" by using find and replace
Phase 2: Developed python code(index_data.py) for uploading cleaned data to elastic search index. And tested in elastic by using few queries to ensure data indexed successfully or not . Example "GET mini_project_index/_search"
phase 3: 1. Reindex data to another index 
2. Add a derived field: 
o risk_level = "High" if operating_system_lifecycle_status is "EOL" 
or "EOS", 
else "Low". 
3. Calculate system age (in years) from the installation date. 
4. Delete records that have missing hostnames or Unknown providers. 
5. Update existing records with the new fields using _update_by_query. 
phase 4:
 Export or view data in Kibana. 
2. Create charts such as: 
o Assets by Country 
o Lifecycle Status Distribution 
o High vs Low Risk Assets 
o Top OS Providers 
3. Save screenshots of your dashboards in a folder: 
visualization_screenshots/ 
4. Write short business insights — e.g.: 
“40% of assets are EOL — indicating an urgent need for OS upgrades
