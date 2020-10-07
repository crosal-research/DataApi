# app imports
from DB.transactions import add_series

add_series("ONS.carga", "Carga Total despachada pelo o sistema", 
           "Brasil", "ONS")

print("series from ONS added to the database")
