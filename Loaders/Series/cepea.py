# fetches data from cepea database
#
# Obs:
# for clarification in regards to the olefile module check
# this stackoverflow thread: https://stackoverflow.com/questions/
# 58336366/compdocerror-when-importing-xls-file-format-to-python-using-pandas-read-excel


# import form the system
from concurrent.futures import ThreadPoolExecutor as executor

# import from packages
import requests
import olefile
import pandas as pd

#app imports
from DB.transactions import add_series


info = [("milho", 77), 
        ("acucar", 53),
        ("etanol-diario-paulinia", 111), 
        ("boi-gordo", 2)]

tickers = [f"cepea.{n}" for _, n in info]

def build_url(name:str, number:int) -> str:
    """
    builds the relevant url for the security in case
    """
    return f'https://www.cepea.esalq.usp.br/br/indicador/series/{name}.aspx?id={number}'

def process(url:str) -> pd.DataFrame:
    """
    fetchs the series related to the url used as input. Return a 
    dataframe
    """
    resp = requests.get(url)
    # try:
    #     return pd.read_excel(url)
    # except:
    ole = olefile.OleFileIO(resp.content)
    return  pd.read_excel(ole.openstream("Workbook"))

with executor() as e:
    urls = []
    for p in info:
        urls.append(build_url(*p))
    dfs = list(e.map(process, urls, timeout=60))

for n,tck in enumerate(tickers):
    add_series(tck, dfs[n].columns[0], "Brasil", "CEPEA", )


print("series from cepea added to the database")    



    


