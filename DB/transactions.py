from pony import orm
import DB.db as db
import datetime as dt
import pandas as pd
from functools import reduce, wraps


def multi_dfs_wrapper(func):
    """
    reduces a list of dataframes into a signal dataframe given by joint "outer
    """
    @wraps(func)
    def wrapper(*args, **dargs):
        return reduce(lambda x, y: x.merge(y, left_index=True, right_index=True, how="outer"), func(*args, **dargs))
    return wrapper


@orm.db_session
def add_source(name, description):
    """
    Add source to the base with info in capital letters
    """
    Uname = name.upper()
    src = db.Source.get(name=Uname)
    if src is None:
        db.Source(name=Uname, description=description)
        print(f"New source {Uname} added to the Database")
    else:
        src.description = description
        print(f"Source {Uname} already in the Database. Description Updated")
        

@orm.db_session
def add_series(ticker:str, description:str, country:str, source: str) -> None:
    """
    Add series into the database. Strings are all in upper case
    """
    Uticker = ticker.upper()
    series = db.Series.get(ticker=Uticker)
    if series:
        series.description = description
        series.country = country
        series.source = db.Source.get(name=source.upper())
        print(f"Series {Uticker} already exists. Updated")
    else:
        src = db.Source.get(name=source.upper())
        if src:
            db.Series(ticker=Uticker, description=description, 
                      country=country.upper(), source=src)
            print(f"Series {ticker.upper()} added to the ")
        else:
            print(f"Source {source.upper()} not in the database")


@orm.db_session
def add_observation(ticker: str, dat:dt.datetime , value:float) -> None:
    """
    Adds observation of a particular series.
    """
    series = db.Series.get(ticker=ticker.upper())
    if series:
        obs = db.Observation.get(date=dat, series=series)
        if obs:
            obs.set(date=dat, obs=value)
        else:
            db.Observation(date=dat, obs=value, series=series)
    else:
        print(f"Ticker {ticker.upper()} not in the database")


@orm.db_session        
def fetch_series_list(source:str=None) -> pd.DataFrame:
    """
    Fetch the list of series in the database. If source is determined, 
    only from source
    """
    if source:
        srs = orm.select((s.ticker, s.description, s.country, s.source.name) for s in db.Series if s.source.name == source.upper())
    else:
        srs = orm.select((s.ticker, s.description, s.country, s.source.name) for s in db.Series)

    return pd.DataFrame(data=list(srs), columns=["Ticker", "Description", 
                                                 "Country", "Source"])


def add_batch_observations(ticker:str, df:pd.DataFrame):
    """
    Adds dataframe n x 1 into the database. The index should be
    a datetime.index and the observations of the data column float numbers
    
    """
    if df.shape[1] == 1:
        for ind in df.index:
            add_observation(ticker.upper(), ind, float(df.loc[ind].values))
    else:
        print(f"Data Frame with of {ticker} has wrong dimension")
    

@orm.db_session
def fetch_series(ticker:str, ini:str=None, final:str=None) -> pd.DataFrame:
    """Fetches from database the observation of a single series. Return a
    data frame with the observations.
    missing: checking whether ini <= final
    """
    Uticker = ticker.upper()
    sr =  db.Series.get(ticker=Uticker)
    ini = dt.datetime.fromisoformat(ini) if ini is not None else None
    final = dt.datetime.fromisoformat(final) if final is not None else None
    if sr:
        if (ini is None) and (final is None):
            obs = orm.select((o.date, o.obs) for o in db.Observation 
                             if o.series.ticker == Uticker)
        elif (ini is not None) and (final is None):
            obs = orm.select((o.date, o.obs) for o in db.Observation 
                             if o.series.ticker == Uticker and o.date >= ini)
        elif ini is None and (final is not None):
            obs = orm.select((o.date, o.obs) for o in db.Observation 
                             if o.series.ticker == Uticker and o.date <= final)
        else:
            obs = orm.select((o.date, o.obs) for o in db.Observation 
                             if o.series.ticker == Uticker and o.date <= final
                             and o.date >= ini)
        return  pd.DataFrame(list(obs), columns=["Data", Uticker]).set_index("Data")
    else:
        print(f"Ticker {Uticker} in not in the database")


@multi_dfs_wrapper
def fetch_multi_series(srs: list, ini:str=None, final:str=None) -> [pd.DataFrame]:
    """
    fetch multiple series in a single data frame
    """
    return  [fetch_series(sr, ini, final) for sr in srs]
    
        
        
@orm.db_session            
def delete_obs(ticker:str, ini=None, end=None):
    Uticker = ticker.upper()
    (orm.select(o for o in db.Observation 
                if o.series.ticker == Uticker)).delete(bulk=True)


@orm.db_session            
def delete_series(ticker:str):
    Uticker = ticker.upper()
    (orm.select(s for s in db.Series 
                if s.ticker == Uticker)).delete()
    
