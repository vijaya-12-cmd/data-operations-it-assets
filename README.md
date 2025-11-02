# data-operations-it-assets
Mini project 
Phase 1: Cleaned data: Downloaded .csv file and opened this file in excel and cleaned data by "removing duplicates" by hostname based, changed date format as yyyyy-mm-dd, removed white spaces by using "Trim" function, And replaced empty cells with keyword "UNKNOWN" by using find and replace
Phase 2: Developed python code(index_data.py) for uploading cleaned data to elastic search index. And tested in elastic by using few queries to ensure data indexed successfully or not . Example "GET mini_project_index/_search"
Developed another python code for reindexing file and deriving new field called risk level, and caluculating system age etc
Creating visualization of data and finding insights of data
