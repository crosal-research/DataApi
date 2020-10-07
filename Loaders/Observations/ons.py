# import from packages
import pandas as pd
import asyncio, aiohttp
from io import StringIO
import datetime as dt

# app imports
from DB.transactions import add_batch_observations


# dates = ["2017_05_31",
#          "2017_06_30",
#          "2017_07_31",
#          "2017_08_31",
#          "2017_09_30",
#          "2017_10_31",
#          "2017_11_30",
#          "2017_12_31",    
#          "2018_01_31",
#          "2018_02_28",
#          "2018_03_31",
#          "2018_04_30",
#          "2018_05_31",
#          "2018_06_30",
#          "2018_07_31",
#          "2018_08_31",
#          "2018_09_30",
#          "2018_10_31",
#          "2018_11_30",
#          "2018_12_31",
#          "2018_01_31",
#          "2018_02_28",
#          "2018_03_31",
#          "2018_04_30",
#          "2018_05_31",
#          "2018_06_30",
#          "2018_07_31",
#          "2018_08_31",
#          "2018_09_30",
#          "2018_10_31",
#          "2018_11_30",
#          "2018_12_31",
#          "2019_01_31",
#          "2019_02_28",
#          "2019_03_31",
#          "2019_04_30",
#          "2019_05_31",
#          "2019_06_30",
#          "2019_07_31",
#          "2019_08_31",
#          "2019_09_30",
#          "2019_10_31",
#          "2019_11_30",
#          "2019_12_31",
#          "2020_01_31",
#          "2020_02_29",
#          "2020_03_31",
#          "2020_04_30",



dates= [ "2020_07_31", 
         "2020_08_31", 
         "2020_09_30", 
         "2020_10_06"]



def url(dat):
    return f"http://sdro.ons.org.br/SDRO/DIARIO/{dat}/HTML/07_DadosDiariosAcumulados_Regiao.html"

urls = [url(d) for d in dates]

async def fetch_data(url):
    async with aiohttp.ClientSession() as session:
        async with session.get(url) as response:
            resp = await response.text()
            try:
                df = pd.read_html(resp)[0]
                df = pd.read_html(url)[0]
                dc = df.iloc[2:, :]
                dc.columns = df.iloc[1, :].values
                dc.set_index(["Data"], inplace=True)
                return dc.applymap(lambda x: pd.to_numeric(x))
            except:
                print(url)

async def fetch_all(urls):
    return  await asyncio.gather(*[fetch_data(url) for url in urls])
    
dfs = asyncio.run(fetch_all(urls))

dfinal = pd.concat(dfs, axis=0, join="inner")
dfinal.drop_duplicates(inplace=True)
dfinal.dropna(inplace=True)
dats = [dt.datetime.strptime(d,"%d/%m/%Y") for d in dfinal.index]
dfinal.index = dats

try:
    add_batch_observations("ons.carga", dfinal.iloc[:, [-1]])
    print("ONS.CARGA updated!")
except:
    print("Failed to add observations ons.CARGA in to the database")

