from fronius import load_data,openJson
import psycopg2
from datetime import datetime, timezone, timedelta

# Datos de conexión a la base de datos
db_params = {
    "host": "54.145.74.186",
    "port": 5555,
    "database": "postgres",
    "user": "postgres",
    "password": "post123"
}

# Cargar el diccionario de datos
data = load_data(openJson("urlsFronius.json"))

bogota_timezone = timezone(timedelta(hours=-5))
current_time_bogota = datetime.now(bogota_timezone)

e_nominal_power = 250.0

# Conexión a la base de datos
conn = psycopg2.connect(**db_params)
cur = conn.cursor()

# Insertar datos en DIM_FroniusDevice para cada dispositivo
insert_query = """
    INSERT INTO DIM_FroniusDevice (FD_EnergyDay,FD_ENERGYYEAR, FD_UAC, FD_UDC, FD_IAC, FD_IDC, FD_PAC, FD_NominalPower, FD_TimeStamp)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s,%s)
"""

for device_id, device_data in data.items():
    cur.execute(
        insert_query,
        (
            device_data.get('DAY_ENERGY', 0),
            device_data.get('YEAR_ENERGY', 0),
            device_data.get('UAC', 0),
            device_data.get('UDC', 0),
            device_data.get('IAC', 0),
            device_data.get('IDC', 0),
            device_data.get('PAC', 0),
            e_nominal_power,
            current_time_bogota
        )
    )

# Confirmar y cerrar la transacción
conn.commit()
conn.close()
