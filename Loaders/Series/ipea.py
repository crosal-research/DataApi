# import from system
from concurrent.futures import ThreadPoolExecutor as executor


#import from packages
import requests
import pandas as pd


#app imports
from DB.transactions import add_series


# series to be fetched
numbers = [30005, #FBCF
           30007, #FBCF com ajuste
           35681, #exportações preço
           35584, #importações preco
           31873, #papel ondulado
           37849, #SPC consultas
           37848, #Usecheque consultas
           35690, #exportações quantum
           35590, # importaçõe quantum 
           31873] # expdição de caixas, acessórios e chapas

def build_url(num:int) -> str:
    """
    builds the url to fetch the data at ipeadatas webpage. 
    """
    return f'http://ipeadata.gov.br/ExibeSerie.aspx?oper=export&serid{num}={num}'
    
urls =[build_url(num) for num in numbers]
tickers =[f"IPEA.{num}" for num in numbers]

def process(url:str) -> pd.DataFrame:
    """
    fetch the data returning a dataframe
    """
    return pd.read_excel(url)

with executor() as e:
    dfs = list(e.map(process, urls))

for num,tck in enumerate(tickers):
    add_series(tck, dfs[num].columns[1], "Brasil", "IPEA")

print("series from IPEA added to the database")    
