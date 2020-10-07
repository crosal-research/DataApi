# import packages
import helium as hl
import requests
from io import StringIO
import pandas as pd
from datetime import datetime as dt
import time

# app imports
from DB.transactions import add_batch_observations


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

#dtoday = dt.strftime((dt.today()), format="%Y-%m-%d")

ds = df[df.geo_type == "country/region"].groupby("region").sum().T
ds.columns = [c.lower() for c in ds.columns]

tickers = ["apple.brazil", "apple.spain", 
           "apple.france", "apple.germany", 
           "apple.united_states", "apple.united_kingdom", "apple.italy"]


try:
    for tck in tickers:
        country = (tck.split(".")[1]).replace("_", " ")
        add_batch_observations(tck, ds.loc[:, [country]])
        print(f"Update for {tck}'s mobility sucessfull")
except:
    print(f"Country {tck} could not sucessfully be updated")
