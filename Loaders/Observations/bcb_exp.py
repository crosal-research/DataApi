# import from system
import shutil, time, os
from concurrent.futures import ThreadPoolExecutor as executor
from typing import Optional

# import from packages
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
import pendulum
import pandas as pd

# import from the app
from DB.transactions import add_batch_observations


__all__ = ["fetch"]


chrome_options = webdriver.ChromeOptions()
try:
    fpath = os.path.abspath(os.path.dirname(__file__)) + "/temp/"
except:
    fpath = "/Users/jmrosal/Documents/studies/data_api/Loaders/Observations/temp/"
prefs = {'download.default_directory' : fpath}

chrome_options.add_experimental_option('prefs', prefs)
# chrome_options.add_argument("--headless")

url = "https://www3.bcb.gov.br/expectativas/publico/consulta/serieestatisticas"

date_ini = pendulum.today().previous(pendulum.FRIDAY)\
                            .subtract(years=1).previous(pendulum.FRIDAY).format("DD/MM/Y")
date_final = pendulum.today().previous(pendulum.FRIDAY).format("DD/MM/Y")
ini = 2020
end = 2022


### Muda de acordo com o indicador
indicadores = ["Índice de preços", "PIB", "Meta para Taxa Over-selic", 
               "Taxa de Câmbio"] #, "Fiscal"]

grupo = {indicadores[0]: "grupoIndicePreco:opcoes_5", 
         indicadores[1]: "grupoPib:opcoes_3"}

def _download() -> None:
    for indicador in indicadores:
        if os.path.exists(f"{fpath}{indicador}.xls"):
            os.remove(f"{fpath}{indicador}.xls")

    driver = webdriver.Chrome(options=chrome_options)
    driver.get(url)

    for indicador in indicadores:
        driver.find_element_by_id("indicador").send_keys(indicador)
        if indicador in ["Índice de preços", "PIB"]:
            driver.find_element_by_id(grupo[indicador]).click()

        # typos de projeções
        driver.find_element_by_id("calculo").send_keys("Mediana")
        driver.find_element_by_id("periodicidade").send_keys("Anual")

    
        if indicador in ["Meta para Taxa Over-selic", "Taxa de Câmbio"]:
            driver.find_element_by_id("tipoDeTaxa").send_keys("Fim de ano")

        # Datas
        driver.find_element_by_id("tfDataInicial1").send_keys(date_ini)
        driver.find_element_by_id("tfDataFinal2").send_keys("11/09/2020")

        # change this to work in headless mode
        name1="divPeriodoRefereEstatisticas:grupoAnoReferencia:anoReferenciaInicial"
        name2="divPeriodoRefereEstatisticas:grupoAnoReferencia:anoReferenciaFinal"
        driver.find_element_by_name(f"{name1}").send_keys(ini)
        driver.find_element_by_name(f"{name2}").send_keys(end)

        # submit para arquivos csv
        button = driver.find_element_by_name("btnXLS").click()
        
        # fetch downloaded file e clean-up
        while True:
            if os.path.exists(f"{fpath}Séries de estatísticas.xls"):
                shutil.move(f"{fpath}Séries de estatísticas.xls", 
                            f"{fpath}{indicador}.xls")    
                break
            time.sleep(0.3)
    driver.close()



def _process(ticker: str) -> pd.DataFrame:
    if "IPCA" in ticker:
        mea ='Índice de preços'
    elif "PIB" in ticker:
        mea ='PIB'
    elif "SELIC" in ticker:
        mea ='Meta para Taxa Over-selic'
    else:
        mea = 'Taxa de Câmbio'
    df = pd.read_excel(f"{fpath}{mea}.xls", skiprows=[0], 
                       index_col=[0]).dropna()
    df.index = pd.to_datetime(df.index, format="%d/%m/%Y")
    df_final =  df.loc[:, [re.search("\d\d\d\d", tickers[0])[0]]]
    df_final.columns = [ticker]
    return df_final



def fetch(tickers: str, limit: Optional[int]=10) -> dict:
    """
    Update the series, given by tickers, in the database. limit define the n-th
    observations. If limit=None, adds the past 365 observations. Do the upserts
    through side effects.
    """
    _download()
    def _add(ticker):
        df = _process(ticker)
        df = df if not limit else df.tail(limit)
        add_batch_observations(ticker, df)

    with executor() as e:
        e.map(_add, tickers)
        
    return {"source": "BCB_EXP", 
            "limit": limit,
            "time": pendulum.now().to_datetime_string()}

