# import from app
from DB.transactions import add_series

# years for which add series for year-end estimates
anos = [2020, 2021, 2022]

# definitions of tickers and descrptions
series_list  = [[(f"bcb_exp.ipca_{ano}year_end", f" Expectativa Mediana de Mercado: IPCA (yoy%), final de {ano}", "Brasil", "BCB_exp"),
            (f"bcb_exp.pib_{ano}year_end", f"Expectativa Mediana de Mercado: PIB (yoy%), final de {ano}", "Brasil", "BCB_exp"),
            (f"bcb_exp.selic_{ano}year_end", f"Expectativa Mediana de Mercado: Meta Over-Selic (%), final de {ano}", "Brasil", "BCB_exp"),
            (f"bcb_exp.cambio_{ano}year_end", f"Expectativa Mediana de Mercado: Taxa de Câmbio (R$/USD), final de {ano}", "Brasil", "BCB_exp")] 
           for ano in anos]

series = [item for sublist in series_list for item in sublist]

for s in series:
    add_series(*s)

print("#####################################")
print("Done adding series of BCB's expectation into the database")
print(' ')
