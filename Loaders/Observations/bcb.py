# imports from system
from concurrent.futures import ThreadPoolExecutor as executor
import io, json, time

# import from packages
import requests
import pandas as pd

#import from aap
from DB.transactions import add_batch_observations, fetch_series_list

__all__ = ["fetch", "tickers"]


def build_url(fulltck: str, limit=None) -> str:
    """
    build the url of request to the BCB's api
    """""
    tck= fulltck.split(".")[1].upper()
    tck_new = tck if len(tck) > 2 else "0" + tck
    if not limit:
        return f"http://api.bcb.gov.br/dados/serie/bcdata.sgs.{tck_new}/dados?formato=json"
    return f"http://api.bcb.gov.br/dados/serie/bcdata.sgs.{tck_new}/dados/ultimos/{limit}?formato=json"


def process(resp:requests.models.Response) -> pd.DataFrame:
    """
    handles the successful response to a request the bcb api
    """
    try:
        rjson = json.loads((resp.content).decode())
        df = pd.DataFrame(rjson).set_index('data')
        df.index = pd.to_datetime(df.index, format="%d/%m/%Y")
        df.iloc[:, 0] = pd.to_numeric(df.iloc[:, 0], errors='coerce')
        return df.dropna().applymap(lambda v: float(v))
    except:
        print(resp.url)


def fetch(tickers: list, limit) -> None:
    """
    Fetch the observations from the bcb's api. If 
    """
    t1 = time.time()
    urls = (build_url(tck, limit) for tck in tickers)
    with requests.session() as session:
        with executor() as e:
            dfs = list(e.map(lambda url:process(session.get(url)), list(urls)))
    
    print(f"BCB's Data donwloaded: {time.time() - t1}")
    
    with executor() as e1:
        z = list(zip(tickers, dfs))
        e1.map(lambda tck: add_batch_observations(*tck), z)

    print("##############################################")
    print(f"Done updating Observations for BCB: {time.time() - t1} seconds")
    return {"source": "BCB", "status": "Updated", 
            "@": pendulum.now().to_datetime_string(), 
            "limit": limit}


##############################MAIN##############################

remove_list = ["BCB.195", "BCB.25"]
tickers = [t for t in fetch_series_list("bcb").Ticker.values 
               if t not in remove_list]
###fetch(tickers, 10) 

