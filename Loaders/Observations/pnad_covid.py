# import from system
import datetime as dt
from concurrent.futures import ThreadPoolExecutor as executor
from typing import Optional

# import external packages
import requests
import pandas as pd
from bs4 import BeautifulSoup
from io import StringIO
from dateutil.rrule import rrule, WEEKLY
import pendulum


# app imports
from DB.transactions import add_batch_observations, fetch_series_list

url = "https://www.ibge.gov.br/estatisticas/"
url = url + "investigacoes-experimentais/estatisticas-experimentais/27946-divulgacao-semanal-pnadcovid1?"
url = url + "t=resultados&utm_source=covid19&utm_medium=hotsite&utm_campaign=covid_19"


def _process(resp:requests.models.Response)-> pd.DataFrame:
    """
    takes a response from the source url and process it into a datafram
    that can be consumed in order to add observations into the database
    """
    soup = BeautifulSoup(resp.content, "html.parser")
    resource = [l for l in soup.select("p a") 
                if l.text == 'Tabelas de resultados'][0]["href"]
    dfs = pd.read_excel(resource, skiprows=[0, 1, 2, 3, 4], sheet_name= [0, 1],
                        na_values=["-", 0])
    dp = (dfs[0]).loc[[("CV" not in str(i)) for i in dfs[0].Indicador], 
                      [("Situação" not in c) for c in dfs[0].columns]]


    # separates data
    dp_td = dp[[("Taxa de desocupação" in str(i)) for i in dp.Indicador]].iloc[[0],:]
    dp_tp = dp[[("Taxa de participação" in str(i)) for i in dp.Indicador]].iloc[[0], :]
    dp_ts = dp[[("distanciamento" in str(i)) for i in dp.Indicador]].iloc[[6],:]
    df_final = pd.concat([dp_td, dp_tp, dp_ts]).set_index(["Indicador"])
    df_final = df_final.drop(["Nível Territorial", "Abertura Territorial"], axis=1).T

    # fix dtes
    start_date = dt.datetime(2020, 5, 9)
    dates = list(rrule(freq=WEEKLY, count=df_final.shape[0], dtstart=start_date))
    df_final.index = dates
    
    # rename columns:
    df_final.columns = ["PNAD_COVID.taxa_de_desocupacao", 
                  "PNAD_COVID.taxa_de_participacao", 
                  "PNAD_COVID.efeito_distanciamento"]
    return df_final


def fetch(tickers:str, limit:Optional[int]=10):
    """
    upserts observations pertaining the series given by the tickers.
    If limit is None, do that for all observations; if limit is int, do that
    for the last limit-nth observations
    """
    resp = requests.get(url)
    df = _process(resp)
    df = df if limit is None else df.tail(limit)
    with executor() as e:
        e.map(lambda tck: add_batch_observations(tck, df.loc[:,[tck]]), 
              tickers)
    return {"source":"PNAD_COVID", 
            "time": pendulum.now().to_datetime_string(), 
            "limit": limit}

