# import system's package
from concurrent.futures import ThreadPoolExecutor as executor
from typing import Optional

# import packages
import helium as hl
import requests
from io import StringIO
import pandas as pd
from datetime import datetime as dt
import time

# app imports
from DB.transactions import add_batch_observations

__all__ = ['fetch']


def build_url():
    """
    fetches the url for grabbing the data from apples webgpage
    hidden in the relevant link
    """
    driver = hl.start_chrome("https://www.apple.com/covid19/mobility", 
                             headless=True)
    while True:
        try:
            hl.wait_until(hl.S(".download-button-container").exists, timeout_secs=40)
            break
        except:
            time.sleep(1.0)
    while True:
        el = driver.find_element_by_class_name("download-button-container")
        link = el.find_element_by_tag_name("a")
        url = link.get_attribute("href")
        if url is not None:
            break
        time.sleep(1.0)
    hl.kill_browser()
    return url


def grab_data(url:str):
    """
    fetches the mobility data from apple using the url 
    of the relevant link
    """
    resp = requests.get(url).text
    df = pd.read_csv(StringIO(resp), header=[0])
    ds = df[df.geo_type == "country/region"].groupby("region").sum().T
    ds.columns = [f"APPLE.{(c.upper()).replace(' ', '_')}" for c in ds.columns]
    return ds


def fetch(tickers: list, limit: Optional[int]=10):
    """
    Upserts the data into the database considering the desired tickers.
    If limit is a integer, upserts only the last limit-th observations
    """
    url = build_url()
    ds = grab_data(url)
    df = ds if not limit else ds.tail(limit)
    with executor() as e:
        zl = [(tck, df.loc[:, [tck]]) for tck in tickers]
        def _add(tck):
            try:
                add_batch_observations(*tck)
            except:
                print (f"Ticker {tck[0]} not sucessfully added")
        e.map(lambda tck: _add(tck), zl)
    return df
