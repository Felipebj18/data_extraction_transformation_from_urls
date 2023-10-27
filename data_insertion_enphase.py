import psycopg2
from datetime import datetime, timezone, timedelta
from enphase import load_data,openJson
# Datos de conexión a la base de datos
db_params = {
    "host": "54.145.74.186",
    "port": 5555,
    "database": "postgres",
    "user": "postgres",
    "password": "post123"
}

# Cargar el diccionario de datos
data = load_data(openJson("./urlsEnphase.json"))

bogota_timezone = timezone(timedelta(hours=-5))
current_time_bogota = datetime.now(bogota_timezone)

e_nominal_power = 250.0


conn = psycopg2.connect(**db_params)
cur = conn.cursor()

insert_query = """
    INSERT INTO DIM_Enphase (E_EnergyDay, E_PAC, P_I1, P_I2, P_I3, P_I4, P_I5, P_I6, P_I7, P_I8, P_I9, P_I10, P_I11, P_I12, P_I13, P_I14, P_I15, P_I16, P_I17, P_I18, P_I19, P_I20, E_NominalPower, E_TimeStamp)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
"""

for device_id, device_data in data.items():
    cur.execute(
        insert_query,
        (
            float(device_data.get('EnergyDay', 0)),
            int(device_data.get('PAC', 0)),
            int(device_data.get('P_I1', 0)),
            int(device_data.get('P_I2', 0)),
            int(device_data.get('P_I3', 0)),
            int(device_data.get('P_I4', 0)),
            int(device_data.get('P_I5', 0)),
            int(device_data.get('P_I6', 0)),
            int(device_data.get('P_I7', 0)),
            int(device_data.get('P_I8', 0)),
            int(device_data.get('P_I9', 0)),
            int(device_data.get('P_I10', 0)),
            int(device_data.get('P_I11', 0)),
            int(device_data.get('P_I12', 0)),
            int(device_data.get('P_I13', 0)),
            int(device_data.get('P_I14', 0)),
            int(device_data.get('P_I15', 0)),
            int(device_data.get('P_I16', 0)),
            int(device_data.get('P_I17', 0)),
            int(device_data.get('P_I18', 0)),
            int(device_data.get('P_I19', 0)),
            int(device_data.get('P_I20', 0)),
            e_nominal_power,
            current_time_bogota
        )
    )

# Confirmar y cerrar la transacción
conn.commit()
conn.close()
