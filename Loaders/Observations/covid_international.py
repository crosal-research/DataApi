import concurrent.futures
import requests
import pandas as pd
from io import StringIO
from DB.transactions import add_batch_observations

url_c = "https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_time_series/time_series_covid19_confirmed_global.csv"

url_d = "https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_time_series/time_series_covid19_deaths_global.csv"

with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
    futs = executor.map(lambda url: requests.get(url), [url_c, url_d])

dresps = [pd.read_csv(StringIO(f.text)) for f in futs]    

dfs = [(df.groupby("Country/Region").sum().drop(columns=["Lat","Long"])).T
           for df in dresps]

for df in dfs:
    df.index = pd.to_datetime(df.index, dayfirst=False)

countries = ['Italy', 'United Kingdom', 'US', 'Germany',
             'France', 'Brazil', 'China', "Spain"]

# cases:
for c in countries:
    tck = f"JHU.{c.replace(' ','_')}_cases".upper()
    try:
        add_batch_observations(tck, dfs[0].loc[:, [c]])
        print(f"Series {tck} sucessfully added!")

    except:
        print(f"Series {tck} failed to be added!")

# fatalities:
for c in countries:
    tck = f"JHU.{c.replace(' ','_')}_fatalities".upper()
    try:
        add_batch_observations(tck, dfs[1].loc[:, [c]])
        print(f"Series {tck} sucessfully added!")
    except:
        print(f"Series {tck} failed to be added!")


