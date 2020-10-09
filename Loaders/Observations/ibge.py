# import from system
from concurrent.futures import ThreadPoolExecutor as executor
import re
import json, time

# import from packges
import requests
import pandas as pd
import numpy as np
import pendulum

#import from app
from DB.transactions import add_batch_observations


def build_url(tck:str, limit=None) -> str:
    tck_new = tck.split(".")[1]
    if not limit:
        return f"http://api.sidra.ibge.gov.br/values/t/{tck_new}/n1/1/f/a"
    return f"http://api.sidra.ibge.gov.br/values/t/{tck_new}/p/last {limit}/n1/1/f/a"

def process(resp: requests.models.Response):
    try:
        table = (re.compile("\d+")).findall(resp.url)[0]
        df = pd.DataFrame(json.loads((resp.content).decode())[1:]).loc[:,["D1C", "V"]].set_index("D1C")
        df.index = pd.to_datetime(df.index, format="%Y%m")
        periods = len(np.unique(df.index.month))
        if table in ["1620", "1621"]:
            di = pd.period_range(df.index[0], periods=len(df), freq='Q')
            df.index = di.to_timestamp()
        else:
            di = pd.period_range(df.index[0], periods=len(df), freq='M')
            df.index = di.to_timestamp()
        df.index.name = 'date'
        df = df.replace(to_replace="\A[\.|-]", regex=True, value=pd.NA).dropna()
        return df.applymap(lambda v: float(v))
    except:
        print(resp.url)




def fetch(tickers:list, limit=None) -> None:
    t1 = time.time()
    urls = [build_url(tck) for tck in tickers]
    with requests.session() as session:
        with executor() as e:
            dfs = list(e.map(lambda url: process(session.get(url)), urls))

    print("###################################################")
    print(f"Done downloading ibge data: {time.time() - t1} seconds")

    with executor() as e1:
        dzs = list(zip(tickers, dfs))
        e1.map(lambda dz: add_batch_observations(*dz), dzs)

    print(f"Done updating ibge data: {time.time() - t1} seconds")

    return {"source": "FRED", "status": "updated", 
            "@": pendulum.now().to_datetime_string(), 
            "limit": limit}

##############################MAIN##############################

