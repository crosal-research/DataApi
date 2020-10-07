# import system
from concurrent.futures import ThreadPoolExecutor as executor

# import from packages
import requests

# import from app
from DB.transactions import add_series
import config

_key_fred = config.keys["fred"] 

tickers = ["DGS10",
           "ICSA",
           "INDPRO",
           "PAYEMS",
           "CPIAUCSL",
           "CPILFESL",
           "UNRATE",
           "CIVPART",
           "VIXCLS",
           "T10YIE",
           "VXEWZCLS", 
           "PCEC96", 
           "PCEPILFE", 
           "PCEPI", 
           "USRECD", 
           "CFNAI", 
           "UMCSENT"]


def build_fred(key, ticker):
    return f"https://api.stlouisfed.org/fred/series?series_id={ticker}"  \
        + f"&api_key={key}&file_type=json"


def process(url):
    resp = requests.get(url).json()["seriess"][0]
    sea = resp['seasonal_adjustment']
    title = resp['title']
    freq = resp['frequency']
    units= resp['units']
    return f"{title}, {sea}, {freq}, {units}"

urls = [build_fred(_key_fred, tck) for tck in tickers]
Atickers = [f"FRED.{tck}" for tck in tickers]

with executor() as e:
    infos = list(e.map(process, urls))

for num, tck in enumerate(Atickers):
    add_series(tck, infos[num], "United States (US)", "FRED")
    
print("series added to the database")
