# import from systems
from concurrent.futures import ThreadPoolExecutor as executor

# import from packages
import pandas as pd
import requests
import pendulum
from io import StringIO
import datetime as dt

# app imports
from DB.transactions import add_batch_observations

__all__ = ['fetch', 'tickers']

tickers = ["ons.carga"]

# dates = ["2017_05_31",
#          "2017_06_30",
#          "2017_07_31",
#          "2017_08_31",
#          "2017_09_30",
#          "2017_10_31",
#          "2017_11_30",
#          "2017_12_31",    
#          "2018_01_31",
#          "2018_02_28",
#          "2018_03_31",
#          "2018_04_30",
#          "2018_05_31",
#          "2018_06_30",
#          "2018_07_31",
#          "2018_08_31",
#          "2018_09_30",
#          "2018_10_31",
#          "2018_11_30",
#          "2018_12_31",
#          "2018_01_31",
#          "2018_02_28",
#          "2018_03_31",
#          "2018_04_30",
#          "2018_05_31",
#          "2018_06_30",
#          "2018_07_31",
#          "2018_08_31",
#          "2018_09_30",
#          "2018_10_31",
#          "2018_11_30",
#          "2018_12_31",
#          "2019_01_31",
#          "2019_02_28",
#          "2019_03_31",
#          "2019_04_30",
#          "2019_05_31",
#          "2019_06_30",
#          "2019_07_31",
#          "2019_08_31",
#          "2019_09_30",
#          "2019_10_31",
#          "2019_11_30",
#          "2019_12_31",
#          "2020_01_31",
#          "2020_02_29",
#          "2020_03_31",
#          "2020_04_30",






def build_url(dat):
    return f"http://sdro.ons.org.br/SDRO/DIARIO/{dat}/HTML/07_DadosDiariosAcumulados_Regiao.html"


def process(resp):
    try:
        df = pd.read_html(StringIO(resp.text))[0]
        dc = df.iloc[2:, :]
        dc.columns = df.iloc[1, :].values
        dc.set_index(["Data"], inplace=True)
        return dc.applymap(lambda x: pd.to_numeric(x))
    except:
        print(resp.url)


def fetch(tickers: list, limit):
    dates= ["2020_07_31", 
            "2020_08_31", 
            "2020_09_30", 
            "2020_10_08"]

    urls = [build_url(d) for d in dates]

    with requests.session() as session:
        with executor() as e:
            dfs = list(e.map(lambda url: process(requests.get(url)), urls))

    dfinal = pd.concat(dfs, axis=0, join="inner")
    dfinal.drop_duplicates(inplace=True)
    dfinal.dropna(inplace=True)
    dats = [dt.datetime.strptime(d,"%d/%m/%Y") for d in dfinal.index]
    dfinal.index = dats
    if not limit:
        dfinal = dfinal.tail(limit)
    
    try:
        add_batch_observations(tickers[0], dfinal.iloc[:, [-1]])
        print("ONS.CARGA updated!")
        return {"source": "ONS", "status": "Updated!", 
                "time": pendulum.now().to_datetime_string(), 
                "limit": limit}
    except:
        print("Failed to add observations ons.CARGA in to the database")
        return {"source": "ONS", "status": "Failed to Update", 
                "time": pendulum.now().to_datetime_string(), 
                "limit": limit}
    


