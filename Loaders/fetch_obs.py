# import from system
from functools import wraps
from typing import Optional

# imports from app
from Loaders.Observations import bcb, cepea, ibge, ons, fred, ipea
# from Loaders.Observations import covid_international, ons, bcb_exp
# from Loaders.Observations import fred, pnad_covid, mobility_apple
from DB.transactions import fetch_series_list
import pendulum

__all__ = ["fetch_obs"]


source_dict = {"BCB": bcb.fetch,
               "IBGE": ibge.fetch, 
               "IPEA": ipea.fetch, 
               "FRED": fred.fetch, 
               "CEPEA": cepea.fetch, 
               "COVID_INTERNATIONAL":"COVID_INTERNATIONAL", 
               "PNAD_COVID":"PNAD_COVID", 
               "APPLE": "APPLE", 
               "ONS": ons.fetch, 
               "BCB_EXP": "BCB_EXP"}


def fetch_obs(source: str, limit: Optional[int]=10) -> dict:
    """fetches and updates (inserts) observations of a particular source
    (ex:BCB) and for last (limit) observations. If limit = None, all
    observations are updated. Does the upserts through side effects
    """
    Usource = source.upper()
    start = pendulum.now().to_datetime_string()
    tickers = list(fetch_series_list(Usource).Ticker)
    results = source_dict[Usource](tickers, limit)
    results["start"] = start
    return results
