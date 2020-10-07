# import from system
import shutil, time, os

# import from packages
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
import pendulum
import pandas as pd


# import from the app
from DB.transactions import add_batch_observations

url = "https://www3.bcb.gov.br/expectativas/publico/consulta/serieestatisticas"
fpath = os.path.abspath(os.path.dirname(__file__)) + "/temp/"
#fpath = "./temp/"
chrome_options = Options()
driver = webdriver.Chrome(options=chrome_options)
driver.get(url)


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

    name1="divPeriodoRefereEstatisticas:grupoAnoReferencia:anoReferenciaInicial"
    name2="divPeriodoRefereEstatisticas:grupoAnoReferencia:anoReferenciaFinal"
    driver.find_element_by_name(f"{name1}").send_keys(ini)
    driver.find_element_by_name(f"{name2}").send_keys(end)

    # submit para arquivos csv
    button = driver.find_element_by_name("btnXLS").click()

    # fetch downloaded file e clean-up
    while True:
        if os.path.exists("/Users/jmrosal/Downloads/Séries de estatísticas.xls"):
            break
        time.sleep(0.3)

    shutil.move("/Users/jmrosal/Downloads/Séries de estatísticas.xls", 
            f"{fpath}{indicador}.xls")    

### 
driver.close()

def _process(mea: str):
    df = pd.read_excel(f"{fpath}/{mea}.xls", skiprows=[0], 
                       index_col=[0]).dropna()
    df.index = pd.to_datetime(df.index, format="%d/%m/%Y")
    return df


def _send_to_db(indicator):
    df = _process(indicator)
    if indicator == 'Índice de preços':
        for y in df.columns:
            add_batch_observations(f"bcb_exp.ipca_{y}year_end", df.loc[:, [y]])
    elif indicator == "PIB":
        for y in df.columns:
            add_batch_observations(f"bcb_exp.pib_{y}year_end", df.loc[:, [y]])
    elif indicator == "Meta para Taxa Over-selic":
        for y in df.columns:
            add_batch_observations(f"bcb_exp.selic_{y}year_end.", df.loc[:, [y]])
    else:
        for y in df.columns:
            add_batch_observations(f"bcb_exp.cambio_{y}year_end", df.loc[:, [y]])

for indicador in indicadores:
    _send_to_db(indicador)

print("###############################################")
print("#####BCB Expectation Data added to the Database!#####")


for indicador in indicadores:
    if os.path.exists(f"./temp{indicador}.xls"):
        os.remove(f"./temp{indicador}.xls")

