import psycopg2
from datetime import datetime

# Parámetros de conexión a la base de datos
db_params = {
    "host": "54.145.74.186",
    "port": 5555,
    "database": "postgres",
    "user": "postgres",
    "password": "post123"
}

try:
    # Establecer la conexión a la base de datos
    conn = psycopg2.connect(**db_params)

    # Crear un cursor para ejecutar consultas
    cur = conn.cursor()

    # Datos de las dimensiones (Reemplaza con los datos reales)
    dimension_data = [
        # Tupla con datos de dimensiones (id_fd, id_e, id_fdm, e_devicename, fd_devicename, fdm_devicename)
    ]

    # Obtener la fecha y hora actual
    facts_timestamp = datetime.now()

    # Recorrer cada registro de dimensiones y calcular los indicadores
    for data in dimension_data:
        (id_fd, id_e, id_fdm, e_devicename, fd_devicename, fdm_devicename) = data

        # Realizar los cálculos de los indicadores
        ind_fd_fc = data[1] / (data[4] * 24)
        ind_fd_hsp = (data[1] / 2) / 1000
        ind_fd_uacvsudc = data[6] / data[7]
        ind_fd_iacvsidc = data[5] / data[3]
        ind_e_hsp = (data[1] / 2) / 1000
        ind_e_fc = data[1] / (data[2] * 24)

        # Calcular el promedio de las potencias de dim_enphane
        # Reemplaza esto con el cálculo real
        ind_e_meanp = 0.0

        # Insertar registros en la tabla "facts" con los indicadores calculados
        insert_query = """
        INSERT INTO facts (id_fd, id_e, id_fdm, ind_fd_fc, ind_fd_hsp, ind_fd_uacvsudc,
                            ind_fd_iacvsidc, ind_e_hsp, ind_e_fc, ind_e_meanp, facts_timestamp,
                            e_devicename, fd_devicename, fdm_devicename)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """

        data_to_insert = (
            id_fd,
            id_e,
            id_fdm,
            ind_fd_fc,
            ind_fd_hsp,
            ind_fd_uacvsudc,
            ind_fd_iacvsidc,
            ind_e_hsp,
            ind_e_fc,
            ind_e_meanp,
            facts_timestamp,
            e_devicename,
            fd_devicename,  
            fdm_devicename
        )

        cur.execute(insert_query, data_to_insert)

    # Confirmar la transacción y cerrar la conexión
    conn.commit()
    conn.close()

except psycopg2.Error as e:
    print("Error al conectarse a la base de datos o insertar datos en la tabla facts:", e)


# import psycopg2

# # Parámetros de conexión a la base de datos
# db_params = {
#     "host": "54.145.74.186",
#     "port": 5555,
#     "database": "postgres",
#     "user": "postgres",
#     "password": "post123"
# }

# try:
#     # Establecer la conexión a la base de datos
#     conn = psycopg2.connect(**db_params)

#     # Crear un cursor para ejecutar consultas
#     cur = conn.cursor()

#     # Consulta para obtener la lista de tablas en el esquema público
#     table_query = """
#     SELECT table_name
#     FROM information_schema.tables
#     WHERE table_schema = 'public';
#     """
    
#     # Ejecutar la consulta para obtener la lista de tablas
#     cur.execute(table_query)
#     tables = [row[0] for row in cur.fetchall()]

#     # Recorrer cada tabla e imprimir sus datos
#     for table in tables:
#         print(f"Tabla: {table}")
#         select_query = f"SELECT * FROM {table};"
#         cur.execute(select_query)
#         data = cur.fetchall()

#         # Obtener los nombres de las columnas
#         column_names = [desc[0] for desc in cur.description]

#         # Imprimir los nombres de las columnas
#         print("Nombres de las columnas:")
#         print(column_names)

#         # Imprimir los datos
#         for row in data:
#             print(row)
#         print("\n")

#     # Cerrar el cursor y la conexión
#     cur.close()
#     conn.close()

# except psycopg2.Error as e:
#     print("Error al conectarse a la base de datos:", e)
