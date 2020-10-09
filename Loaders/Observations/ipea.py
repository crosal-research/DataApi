# import from system
from concurrent.futures import ThreadPoolExecutor as executor
from typing import Optional

#import from packages
import requests
import pandas as pd
import pendulum


#app imports
from DB.transactions import add_batch_observations

def build_url(num:int) -> str:
    """
    builds the url to fetch the data at ipeadatas webpage. 
    """
    return f'http://ipeadata.gov.br/ExibeSerie.aspx?oper=export&serid{num}={num}'
    

def process(url:str) -> pd.DataFrame:
    """
    fetch the data returning a dataframe
    """
    df = pd.read_excel(url, parse_dates=[0])
    df.columns = ["data", "values"]
    return df.set_index(["data"])


def fetch(tickers:list, limit: Optional[int]) -> dict:
    """
    """
    global dfs
    urls =[build_url(tcks.split(".")[1]) for tcks in tickers]
    with executor() as e:
        dfs = list(e.map(process, urls))

    with executor() as e1:
        dz = zip(tickers, dfs)
        e1.map(lambda z: add_batch_observations(*z), list(dz))
                
    return {"source": "IPEA", "status": "updated", 
            "@": pendulum.now().to_datetime_string(), 
            "limit": limit}



