# import packages
import helium as hl
import requests
from io import StringIO
import pandas as pd
from datetime import datetime as dt
import time

# app imports
from DB.transactions import add_series

driver = hl.start_chrome("https://www.apple.com/covid19/mobility", 
                         headless=True)

while True:
    hl.wait_until(hl.S(".download-button-container").exists, timeout_secs=40)

    el = driver.find_element_by_class_name("download-button-container")
    link = el.find_element_by_tag_name("a")
    url = link.get_attribute("href")
    if url is not None:
        break
    time.sleep(1.0)

hl.kill_browser()
print(url)

resp = requests.get(url).text
df = pd.read_csv(StringIO(resp), header=[0])
print("Data Downloaded!")

ds = df[df.geo_type == "country/region"].groupby("region").sum().T
ds.columns = [c.lower() for c in ds.columns]

for c in ds.columns[:-4]:
    ticker = f"APPLE.{c.replace(' ','_').upper()}"
    description = f"Aggregated Mobility Index in {c}"
    country = c
    source = "APPLE"
    add_series(ticker, description, country, source)
    
print("series from apple added to the database")
