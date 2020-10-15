#import from system
from concurrent.futures import ThreadPoolExecutor as executor
from typing import Optional

# import from packages
import requests
import pandas as pd
from io import StringIO
import pendulum

# import from app
from DB.transactions import add_batch_observations


__all__ = ["fetch"]

def build_url(cases:str)->str:
    """
    forms the relevant url. case = [confirmed, deaths]
    """
    return f"https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_time_series/time_series_covid19_{cases}_global.csv"

def _process(resp:requests.models.Response)-> pd.DataFrame:
    """
    process the response of a request coming from the disired url
    a return a dataframe
    """
    dr = pd.read_csv(StringIO(resp.text))
    df =  (dr.groupby("Country/Region").sum().drop(columns=["Lat","Long"])).T
    df.index = pd.to_datetime(df.index, dayfirst=False)
    types = "cases" if "confirmed" in resp.url else "fatalities"
    df.columns = [f"JHU.{c.replace(' ','_')}_{types}".upper() 
                  for c in df.columns]
    return df


def fetch(tickers:str, limit:Optional[int] = 10) -> dict:
    """
    upsert the observations of tickers into the database. If limit is None
    to that for the whole set of observations; if limit is int, does that
    only for the latest limit observation. Return information about the upsert,
    but operates via side-effects
    """
    urls = [build_url(cases) for cases in ["confirmed", "deaths"]]
    
    with requests.session() as session:
        with executor(max_workers=2) as e:
            resps = e.map(lambda url: session.get(url), urls)

    dfs = [_process(r) for r in resps]
    df = dfs[0].merge(dfs[1], left_index=True, right_index=True, how="inner")
    df = df if limit is None else df.tail(limit)

    with executor() as e1:
        dz = [(tck, df.loc[:, [tck]]) for tck in tickers]
        e1.map(lambda z: add_batch_observations(*z), dz)

    return {"source": "JHU", 
            "time": pendulum.now().to_datetime_string(),
            "limit": limit}


