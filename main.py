from dw_insertions import dw_insertions
from get_json import load_data
# Parámetros de conexión a la base de datos
db_params = {
    "host": "54.145.74.186",
    "port": 5555,
    "database": "postgres",
    "user": "postgres",
    "password": "post123"
}


dw = dw_insertions(db_params)

fronius_data = load_data("urlsFronius.json")
enphase_data = load_data("./urlsEnphase.json")
fronius_dm_data = load_data("./urlsFroniusDM.json")

#Inserts
dw.insert_fronius(fronius_data)
dw.insert_enphase(enphase_data)
dw.insert_froniusdm(fronius_dm_data)


dw.close_connection()
