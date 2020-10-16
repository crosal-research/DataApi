# import from system
from functools import wraps
from typing import Optional

# import from packages
import pendulum

# imports from app
from Loaders.Observations import bcb, cepea, ibge, ons, fred, ipea
from Loaders.Observations import mobility_apple, bcb_exp, covid, pnad_covid
from Loaders.Observations import bcb_vencimentos
from DB.transactions import fetch_series_list

__all__ = ["fetch_obs"]


source_dict = {"BCB": bcb.fetch,
               "IBGE": ibge.fetch, 
               "IPEA": ipea.fetch, 
               "FRED": fred.fetch, 
               "CEPEA": cepea.fetch, 
               "COVID": covid.fetch, 
               "PNAD_COVID": pnad_covid.fetch, 
               "APPLE": mobility_apple.fetch, 
               "ONS": ons.fetch, 
               "BCB_EXP": bcb_exp.fetch, 
               "BCB_VENCIMENTOS": bcb_vencimentos.fetch}


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
