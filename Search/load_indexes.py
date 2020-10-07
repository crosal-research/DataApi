import os
from whoosh.index import open_dir
from DB.transactions import fetch_series_list
from Search.transactions import add_index

ix = open_dir(os.path.abspath(os.path.dirname(__file__)) + "/index")
# ix = open_dir("./Search/index")
df = fetch_series_list()
for i in df.index:
    add_index(*list(df.iloc[[i],:].values[0]))
ix.close()
print("Done indexing documents!")
    
