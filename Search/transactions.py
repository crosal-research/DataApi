# import from system
import os

# import from external packages
import whoosh.index as index
from whoosh.fields import *
from whoosh.query import *
from whoosh.qparser import QueryParser, MultifieldParser, FuzzyTermPlugin

# constants
index_dir = os.path.abspath(os.path.dirname(__file__)) + "/index"
# index_dir = "./index"

# load doc into index base
def query_docs(name:str, limit=None):
    # This function is returning one item per field that
    # meets the criteria (needs to fix it)
    ix = index.open_dir(index_dir)
    with ix.searcher() as searcher:
        parser = MultifieldParser(["description", "country", "source"], schema=ix.schema)
        parser.add_plugin(FuzzyTermPlugin()) 
        results = searcher.search(parser.parse(f"{name}˜2"), limit=limit)
        res =  [dict(results[i]) for i in range(0, len(results))]
    ix.close()
    return res


def add_index(ticker:str, description:str, country:str, source:str) -> None:
    """
    Add a doc to the index database. Doc is taken from the 
    app database
    """
    ix = index.open_dir(index_dir)
    writer = ix.writer()
    writer.add_document(ticker=ticker, description=description,
                           country=country, source=source)
    writer.commit()
    ix.close()


def delete_doc_by_ticker(ticker:str):
    ix = index.open_dir(index_dir)
    writer = ix.writer()
    writer.delete_by_term("ticker", ticker.lower())
    writer.commit()
    ix.close()
