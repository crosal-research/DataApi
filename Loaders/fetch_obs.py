#imports from app
from Loaders.Observations import bcb, bcb_exp, cepea, ibge
from Loaders.Observations import covid_international, ons
from Loaders.Observations import fred, pnad_covid, mobility_apple
from DB.transactions import fetch_series_list


def fecth_obs(source: str, limit=None) ->None:
    """fetches and updates (inserts) observations of a particular source
    (ex:BCB) and for last (limit) observations. If limit = None, all
    observations are updated
    """
    Usource = source.upper()
    if Usource == "BCB":
        # needs to be implemented
        return "BCB"

    elif Usource == "IBGE":
        # needs to be implemented
        return "IBGE"

    elif Usource == "IPEA":
        # needs to be implemented
        return "IPEA"

    elif Usource == "FRED":
        # needs to be implemented
        return "FRED"

    elif Usource == "CEPEA":
        # needs to be implemented
        return "CEPEA"

    elif Usource == "COVID_INTERNATIONAL":
        # needs to be implemented
        return "COVID_INTERNATIONAL"

    elif Usource == "PNAD_COVID":
        # needs to be implemented
        return "PNAD_COVID"

    else:
        # needs to be implemented: 
        return "APPLE"

