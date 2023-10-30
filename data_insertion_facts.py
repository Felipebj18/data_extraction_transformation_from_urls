import psycopg2
from datetime import datetime
from psycopg2.extras import execute_values
from get_json import load_data,openJson

# Datos de conexión a la base de datos
db_params = {
    "host": "54.145.74.186",
    "port": 5555,
    "database": "postgres",
    "user": "postgres",
    "password": "post123"
}

# Conexión a la base de datos
# conn = psycopg2.connect(**db_params)
# cur = conn.cursor()

# Consulta SQL para recuperar datos de la tabla 'dim_froniusdevice'
sql_query = "SELECT * FROM dim_froniusdevice;"

try:
    # Establecer la conexión a la base de datos
    conn = psycopg2.connect(**db_params)

    # Crear un cursor para ejecutar la consulta
    cur = conn.cursor()

    # Ejecutar la consulta SQL
    cur.execute(sql_query)

    # Obtener los nombres de las columnas
    column_names = [desc[0] for desc in cur.description]

    # Imprimir los nombres de las columnas
    print("Nombres de las columnas:")
    print(column_names)

    # Recuperar todos los datos de la tabla
    data = cur.fetchall()

    # Imprimir los datos
    for row in data:
        print(row)

    # Cerrar el cursor y la conexión
    cur.close()
    conn.close()

except psycopg2.Error as e:
    print("Error al conectarse a la base de datos:", e)

# Datos de las dimensiones (diccionarios) - Reemplaza con tus datos
dim_fronius_data = load_data(openJson("urlsFronius.json"))
dim_enphase_data = load_data(openJson("urlsEnphase.json"))
dim_froniusdatamanager_data = load_data(openJson("urlsFroniusDM.json"))


# Calcula los indicadores
ind_fd_fc = dim_fronius_data['fd_energyday'] / (dim_fronius_data['fd_nominalpower'] * 24)
ind_fd_hsp = (dim_fronius_data['fd_energyday'] / 2) / 1000
ind_fd_uacvsudc = dim_fronius_data['fd_uac'] / dim_fronius_data['fd_udc']
ind_fd_iacvsidc = dim_fronius_data['fd_iac'] / dim_fronius_data['fd_idc']
ind_e_hsp = (dim_enphase_data['e_energyday'] / 2) / 1000
ind_e_fc = dim_enphase_data['e_energyday'] / (dim_enphase_data['e_nominalpower'] * 24)

# Calcular ind_e_meanp - Promedio de las potencias P_I1 a P_I20
potencia_promedio = sum(dim_enphase_data[f'P_I{i}'] for i in range(1, 21)) / 20
ind_e_meanp = potencia_promedio

# Crear una lista de tuplas para los nuevos registros
data_to_insert = [
    (
        dim_fronius_data['id_fd'],
        dim_enphase_data['id_e'],
        dim_froniusdatamanager_data['id_fdm'],
        ind_fd_fc,
        ind_fd_hsp,
        ind_fd_uacvsudc,
        ind_fd_iacvsidc,
        #froniusdatamanager_data['fdm_hsp'],  # Usamos el valor existente
        ind_e_hsp,
        ind_e_fc,
        ind_e_meanp,
        datetime.now(),
        dim_enphase_data['e_devicename'],
        dim_fronius_data['fd_devicename'],
        dim_froniusdatamanager_data['fdm_devicename']
    )
]

# Consulta para insertar los nuevos registros en FACTS_SSFV
insert_query = """
INSERT INTO FACTS_SSFV (id_fd, id_e, id_fdm, ind_fd_fc, ind_fd_hsp, ind_fd_uacvsudc,
                        ind_fd_iacvsidc, ind_fdm_hsp, ind_e_hsp, ind_e_fc, ind_e_meanp, facts_timestamp,
                        e_devicename, fd_devicename, fdm_devicename)
VALUES %s
"""
print(insert_query)
# Ejecuta la inserción
# execute_values(cur, insert_query, data_to_insert)
# conn.commit()
# conn.close()


# import psycopg2
# from datetime import datetime
# from psycopg2.extras import execute_values

# # Datos de conexión a la base de datos
# db_params = {
#     "host": "54.145.74.186",
#     "port": 5555,
#     "database": "postgres",
#     "user": "postgres",
#     "password": "post123"
# }

# # Conexión a la base de datos
# conn = psycopg2.connect(**db_params)
# cur = conn.cursor()

# # Datos de las dimensiones (deben ser listas de tuplas, una lista para cada dimensión)
# dim_fronius_data = [
#     # Lista de tuplas para Dim_Fronius
# ]
# dim_enphase_data = [
#     # Lista de tuplas para Dim_Enphase
# ]
# dim_froniusdatamanager_data = [
#     # Lista de tuplas para Dim_FroniusDataManager
# ]

# # Consulta para verificar si los registros ya existen en FACTS_SSFV
# existing_data_query = """
# SELECT DISTINCT ON (id_fd, id_e, id_fdm)
#        id_fd, id_e, id_fdm
#   FROM FACTS_SSFV
# """

# # Ejecuta la consulta
# cur.execute(existing_data_query)
# existing_data = cur.fetchall()

# # Filtra los datos de las dimensiones para obtener solo los registros que no existen en FACTS_SSFV
# new_fronius_data = [data for data in dim_fronius_data if (data[0], data[1], data[2]) not in existing_data]
# new_enphase_data = [data for data in dim_enphase_data if (data[0], data[1], data[2]) not in existing_data]
# new_froniusdatamanager_data = [data for data in dim_froniusdatamanager_data if (data[0], data[1], data[2]) not in existing_data]

# # Insertar registros en FACTS_SSFV para los nuevos datos
# if new_fronius_data or new_enphase_data or new_froniusdatamanager_data:
#     insert_query = """
#     INSERT INTO FACTS_SSFV (id_fd, id_e, id_fdm, ind_fd_fc, ind_fd_hsp, ind_fd_uacvsudc,
#                             ind_fd_iacvsidc, ind_fdm_hsp, ind_e_hsp, ind_e_fc, ind_e_meanp, facts_timestamp,
#                             e_devicename, fd_devicename, fdm_devicename)
#     VALUES %s
#     """

#     # Combina los datos de las dimensiones en una sola lista de tuplas
#     combined_data = []
#     combined_data.extend(new_fronius_data)
#     combined_data.extend(new_enphase_data)
#     combined_data.extend(new_froniusdatamanager_data)

#     # Crea listas con los nombres de dispositivo correspondientes a cada dimensión
#     e_devicename_list = [device_name for device_name, *_ in new_enphase_data]
#     fd_devicename_list = [device_name for device_name, *_ in new_fronius_data]
#     fdm_devicename_list = [device_name for device_name, *_ in new_froniusdatamanager_data]

#     # Transforma los datos en una lista de tuplas para realizar la inserción masiva
#     data_to_insert = [
#         (*data, datetime.now(), e_devicename, fd_devicename, fdm_devicename)
#         for data, e_devicename, fd_devicename, fdm_devicename in zip(
#             combined_data, e_devicename_list, fd_devicename_list, fdm_devicename_list
#         )
#     ]

#     execute_values(cur, insert_query, data_to_insert)

#     conn.commit()
#     conn.close()
# else:
#     print("No se encontraron nuevos registros para insertar en FACTS_SSFV.")
