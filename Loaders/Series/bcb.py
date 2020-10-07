# imports from python's system
from concurrent.futures import ThreadPoolExecutor as executor
import time

# 
import requests
import suds.client
import suds_requests

#app imports
from DB.transactions import add_series

remove = {"fonte": ['IBGE'], 
          "gestorProprietario": ["DEPEF/DIMIF/COIMP", 
                                  "DEPEC/CONAP"], 
          "tickers":["9"]}

def process_info(resp:suds.sudsobject):
    """
    Process informações do soap service do BCB para captar os valores 
    a serem adicionadas na base de dados
    """
    if (resp.fonte not in remove["fonte"]):
        if (resp.gestorProprietario not in remove["gestorProprietario"]):
            if (f"{resp.oid}" not in remove["tickers"]):
                if (resp.ultimoValor.ano == 2020 and
                    f"{resp.periodicidade}" != "Anual" or 
                    resp.ultimoValor.ano == 2019 and 
                      f"{resp.periodicidade}" == "Anual"):
                    ticker = f"BCB.{resp.oid}"
                    description = f"{resp.nomeCompleto}"
                    country = "Brasil"
                    source = "BCB"
                    return [ticker, description, country, source]

tcks = [t for t in range(1, 30000)]

t1 = time.time()

with requests.Session() as session:
    c = suds.client.Client(
    'https://www3.bcb.gov.br/sgspub/JSP/sgsgeral/FachadaWSSGS.wsdl',
    transport=suds_requests.RequestsTransport(session))

    def _fetch(tck):
        try:
            return process_info(c.service.getUltimoValorVO(tck))
        except:
            pass
        
    with executor() as e:
        ls = list(e.map(_fetch, tcks))

fls = [t for t in ls if t is not None]

with executor() as e1:
    e1.map(lambda fl: add_series(*fl), fls)

        
print("############################################################")
print(f"Fetch BCB series done: {time.time() - t1} seconds")
