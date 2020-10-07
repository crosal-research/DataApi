import requests
import pandas as pd
from io import StringIO
from DB.transactions import add_series

url = "https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_time_series/time_series_covid19_confirmed_global.csv"

dfresp = pd.read_csv(StringIO(requests.get(url).text))
df = (dfresp.groupby("Country/Region").sum().drop(columns=["Lat","Long"])).T


#cases
for c in df.columns:
    Uticker = f"JHU.{c.replace(' ','_')}_cases".upper()
    description = f"Total Covid-19 Registered cases in {c}"
    country = c
    add_series(Uticker, description, country, "JHU")
    
#fatalities
for c in df.columns:
    ticker = f"JHU.{c.replace(' ','_')}_fatalities".upper()
    description = f"Total Covid-19 Registered fatalities in {c}"
    country = c
    add_series(ticker, description, country, "JHU")

    
print("series from covid added to the database")
