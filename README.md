# data-operations-it-assets
Mini project 
Phase 1: Cleaned data: Downloaded .csv file and opened this file in excel and cleaned data by "removing duplicates" by hostname based, changed date format as yyyyy-mm-dd, removed white spaces by using "Trim" function, And replaced empty cells with keyword "UNKNOWN" by using find and replace
Phase 2: Developed python code(index_data.py) for uploading cleaned data to elastic search index. And tested in elastic by using few queries to ensure data indexed successfully or not . Example "GET mini_project_index/_search"
Captured data visualization screen shots
As per the insights we found Around 40% of assets are under categoty of EOL and EOS These assets needs to migrate or upgrade as soon as possible
