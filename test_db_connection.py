import psycopg2

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

    # Consulta para obtener la lista de tablas en el esquema público
    table_query = """
    SELECT table_name
    FROM information_schema.tables
    WHERE table_schema = 'public';
    """
    
    # Ejecutar la consulta para obtener la lista de tablas
    cur.execute(table_query)
    tables = [row[0] for row in cur.fetchall()]

    # Recorrer cada tabla e imprimir sus datos
    for table in tables:
        print(f"Tabla: {table}")
        select_query = f"SELECT * FROM {table};"
        cur.execute(select_query)
        data = cur.fetchall()

        # Obtener los nombres de las columnas
        column_names = [desc[0] for desc in cur.description]

        # Imprimir los nombres de las columnas
        print("Nombres de las columnas:")
        print(column_names)

        # Imprimir los datos
        for row in data:
            print(row)
        print("\n")

    # Cerrar el cursor y la conexión
    cur.close()
    conn.close()

except psycopg2.Error as e:
    print("Error al conectarse a la base de datos:", e)
