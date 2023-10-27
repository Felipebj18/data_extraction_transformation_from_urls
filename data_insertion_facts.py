import psycopg2
from datetime import datetime
from psycopg2.extras import execute_values

# Datos de conexión a la base de datos
db_params = {
    "host": "54.145.74.186",
    "port": 5555,
    "database": "postgres",
    "user": "postgres",
    "password": "post123"
}


# Conexión a la base de datos
conn = psycopg2.connect(**db_params)
cur = conn.cursor()

# Datos de las dimensiones (reemplaza con tus datos)
dimension_data = [
    # Añade tus datos de las dimensiones aquí en forma de tuplas
    # Ejemplo: (id_fd, fd_energyday, fd_energyyear, ...),
    # ...
]

# Consulta para verificar si los registros ya existen en FACTS
# Aquí se asume que tienes una clave única en las dimensiones que se usa para verificar duplicados.
existing_data_query = """
SELECT DISTINCT ON (id_fd, id_e, id_fdm)
       id_fd, id_e, id_fdm
  FROM FACTS_SSFV
"""

# Ejecuta la consulta
cur.execute(existing_data_query)
existing_data = cur.fetchall()

# Filtra los datos de las dimensiones para obtener solo los registros que no existen en FACTS
new_data = [data for data in dimension_data if (data[0], data[1], data[2]) not in existing_data]

# Insertar registros en FACTS para los nuevos datos
if new_data:
    insert_query = """
    INSERT INTO FACTS_SSFV (id_fd, id_e, id_fdm, ind_fd_fc, ind_fd_hsp, ind_fd_uacvsudc,
                            ind_fd_iacvsidc, ind_fdm_hsp, ind_e_hsp, ind_e_fc, ind_e_meanp, facts_timestamp,
                            e_devicename, fd_devicename, fdm_devicename)
    VALUES %s
    """

    # Transforma los datos en una lista de tuplas para realizar la inserción masiva
    data_to_insert = [(*data, datetime.now(), e_devicename, fd_devicename, fdm_devicename) for data, e_devicename, fd_devicename, fdm_devicename in zip(new_data, e_devicename_list, fd_devicename_list, fdm_devicename_list)]

    execute_values(cur, insert_query, data_to_insert)

    conn.commit()
    conn.close()
else:
    print("No se encontraron nuevos registros para insertar en FACTS.")
