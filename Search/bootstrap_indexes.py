import os
from whoosh.index import create_in, exists_in
from whoosh.fields import *

schema = Schema(ticker=ID(stored=True), 
                description=TEXT(stored=True), 
                country=TEXT(stored=True), 
                source=TEXT(stored=True))

#if __name__ == "__main__":
fname = os.path.abspath(os.path.dirname(__file__)) + "/index"
print(fname)
if not os.path.exists(fname):
    os.mkdir(fname)
create_in(fname, schema)
        


