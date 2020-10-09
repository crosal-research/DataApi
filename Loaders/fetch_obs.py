# imports from app
# from Loaders.Observations import bcb, bcb_exp, cepea, ibge
# from Loaders.Observations import covid_international, ons
# from Loaders.Observations import fred, pnad_covid, mobility_apple
from DB.transactions import fetch_series_list


__all__ = ["fetch_obs"]


source_dict = {"BCB":"BCB", "IBGE":"IBGE" , "IPEA":"IPEA", "FRED":"FRED", 
               "CEPEA":"CEPEA", "COVID_INTERNATIONAL":"COVID_INTERNATIONAL", 
               "PNAD_COVID":"PNAD_COVID", "APPLE": "APPLE"}


def fetch_obs(source: str, limit=None) -> None:
    """fetches and updates (inserts) observations of a particular source
    (ex:BCB) and for last (limit) observations. If limit = None, all
    observations are updated
    """
    Usource = source.upper()
    return source_dict[Usource]
