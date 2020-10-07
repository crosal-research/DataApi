# fetches data from cepea database
# and adds to the economic database
#
# Obs:
# for clarification in regards to the olefile module check
# this stackoverflow thread: https://stackoverflow.com/questions/
# 58336366/compdocerror-when-importing-xls-file-format-to-python-using-pandas-read-excel


# import form the system
from concurrent.futures import ThreadPoolExecutor as executor
import time

# import from packages
import requests
import olefile
import pandas as pd

#app imports
from DB.transactions import add_batch_observations
t1 = time.time()


info = [("milho", 77), 
        ("acucar", 53),
        ("etanol-diario-paulinia", 111), 
        ("boi-gordo", 2)]


def build_url(name:str, number:int) -> str:
    """
    builds the relevant url for the security in case
    """
    return f'https://www.cepea.esalq.usp.br/br/indicador/series/{name}.aspx?id={number}'

def process(resp:requests.models.Response) -> pd.DataFrame:
    """
    fetchs the series related to the url used as input. Return a 
    dataframe
    """
    try:
        ole = olefile.OleFileIO(resp.content)
        df = pd.read_excel(ole.openstream("Workbook")).iloc[3:, [0,1]].dropna()
        df.columns = ["data", "value"]
        df.set_index(["data"], inplace=True)
        df.index = pd.to_datetime(df.index)
        return df.applymap(lambda v: float(v))
    except:
        print("resp.url")


def fetch(tickers):
    urls = [build_url(*p) for p in info]
    with requests.session() as session:
        with executor() as e:
            dfs = list(e.map(lambda url:process(session.get(url)), 
                             list(urls)))
    with executor() as e1:
        z = list(zip(tickers, dfs))
        e1.map(lambda tck: add_batch_observations(*tck), z)


##############################Main##############################

tickers = [f"cepea.{n}" for _, n in info]

fetch(tickers)

print("################################################################")
print(f"series from cepea added to the database:{time.time()-t1}")    
