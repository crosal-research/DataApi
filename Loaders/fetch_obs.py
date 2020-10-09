# import from system
from functools import wraps
from typing import Optional

# imports from app
from Loaders.Observations import bcb, cepea, ibge, ons, fred
# from Loaders.Observations import covid_international, ons, bcb_exp
# from Loaders.Observations import fred, pnad_covid, mobility_apple
from DB.transactions import fetch_series_list


__all__ = ["fetch_obs"]


source_dict = {"BCB": bcb.fetch,
               "IBGE": ibge.fetch, 
               "IPEA":"IPEA", 
               "FRED": fred.fetch, 
               "CEPEA": cepea.fetch, 
               "COVID_INTERNATIONAL":"COVID_INTERNATIONAL", 
               "PNAD_COVID":"PNAD_COVID", 
               "APPLE": "APPLE", 
               "ONS": ons.fetch}


def fetch_obs(source: str, limit: Optional[int]=10) -> dict:
    """fetches and updates (inserts) observations of a particular source
    (ex:BCB) and for last (limit) observations. If limit = None, all
    observations are updated. Does the upserts through side effects
    """
    Usource = source.upper()
    tickers = list(fetch_series_list(Usource).Ticker)
    return source_dict[Usource](tickers, limit)
