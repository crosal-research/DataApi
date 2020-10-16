# import from python system
from concurrent.futures import ThreadPoolExecutor as executor
from typing import Optional

# import from packages
import requests
import pandas as pd
import pendulum

# import from app
from DB.transactions import add_batch_observations, fetch_series_list, delete_obs

__all__ = ['fetch']


url = 'https://www4.bcb.gov.br/pom/demab/cronograma/vencdata_csv.asp?'


def _process(resp:requests.models.Response):
    txt = [l.split(";") for l in (resp.text).split("\n")]
    df = pd.DataFrame(txt).iloc[3:-3, :-1]
    cols = [f"BCB_VENCIMENTOS.{col}" for col in df.iloc[0,:].values]
    df.columns = cols
    df.drop(axis=0, labels=[3], inplace=True)
    df.set_index(["BCB_VENCIMENTOS.VENCIMENTO"], inplace=True)
    df.index.name = "Date"
    df.index = pd.to_datetime(df.index, format="%d/%m/%Y")
    return df


def fetch(tickers:list, limit:Optional=None):
    resp= requests.get(url)
    df = _process(resp)
    df = df if limit is None else df.tail(limit)

    def _add(tck: str):
        """
        Help function that upserts data on vencimentos,
        first cleaning up and them upserting
        """
        delete_obs(tck)
        add_batch_observations(tck, df.loc[: ,[tck]])

    with executor() as e:
        e.map(_add, tickers)

    return {"source": "BCB_VENCIMENTOS", 
            "status": "Updated", 
            "time": pendulum.now().to_datetime_string(), 
            "limit": limit}

    


    

    
