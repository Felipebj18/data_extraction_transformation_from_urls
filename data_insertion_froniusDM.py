import psycopg2
from datetime import datetime, timezone, timedelta
from get_json import load_data, openJson

# Datos de conexión a la base de datos
db_params = {
    "host": "54.145.74.186",
    "port": 5555,
    "database": "postgres",
    "user": "postgres",
    "password": "post123"
}

data = load_data(openJson("./urlsFroniusDM.json"))

bogota_timezone = timezone(timedelta(hours=-5))
current_time_bogota = datetime.now(bogota_timezone)

e_nominal_power = 250.0

conn = psycopg2.connect(**db_params)
cur = conn.cursor()

insert_query = """
    INSERT INTO DIM_FroniusDataManager (FDM_Radiacion, FDM_Temperatura1, FDM_Temperatura2, FDM_NominalPower, FDM_TimeStamp,FDM_DeviceName)
    VALUES (%s, %s, %s, %s, %s,%s)
"""

for device_id, device_data in data.items():
    cur.execute(
        insert_query,
        (
            device_data.get('Radiacion', 0),
            device_data.get('Temperatura_1', 0),
            device_data.get('Temperatura_2', 0),
            e_nominal_power,
            current_time_bogota,
            device_id
        )
    )

# Confirmar y cerrar la transacción
conn.commit()
conn.close()
