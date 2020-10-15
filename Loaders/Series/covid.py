#import from system
from concurrent.futures import ThreadPoolExecutor as executor
from typing import Optional

#import from package
import requests
import pandas as pd
from io import StringIO

#import from app
from DB.transactions import add_series


def build_url(cases:str)->str:
    """
    forms the relevant url. case = [confirmed, deaths]
    """
    return "https://raw.githubusercontent.com/CSSEGISandData/COVID-19/"+ \
        "master/csse_covid_19_data/csse_covid_19_time_series/"+  \ 
        f"time_series_covid19_{cases}_global.csv"

def _process(resp:requests.models.Response)-> pd.DataFrame:
    dr = pd.read_csv(StringIO(resp.text))
    df =  (dr.groupby("Country/Region").sum().drop(columns=["Lat","Long"])).T
    df.index = pd.to_datetime(df.index, dayfirst=False)
    types = "cases" if "confirmed" in resp.url else "fatalities"
    df.columns = [f"JHU.{c.replace(' ','_')}_{types}".upper() 
                  for c in df.columns]
    return df


def insert():
    global df, dz
    urls = [build_url(cases) for cases in ["confirmed", "deaths"]]
    
    with requests.session() as session:
        with executor(max_workers=2) as e:
            resps = e.map(lambda url: session.get(url), urls)

    dfs = [_process(r) for r in resps]
    df = dfs[0].merge(dfs[1], left_index=True, right_index=True, how="inner")
    
    def _extra_country(ticker:str):
        sl = ticker.split(".")[1].split("_")[:-1]
        if len(sl) ==1:
            return sl[0]
        else:
            return " ".join(sl)


    def _input(tck):
        ticker = tck
        c = _extra_country(tck)
        description = f"Total Covid-19 Registered fatalities in {c}"
        source = "JHU"
        try:
            add_series(ticker, description, c, source)
            print(f"ticker {tck} added to the database")
        except:
            print(f"ticker {tck} failed to be added to the database")

    with executor() as e1:
        e1.map(lambda tck: add_series(_input(tck)), list(df.columns))
        


    

