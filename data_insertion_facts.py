import psycopg2

def connect_to_database(db_params):
    try:
        conn = psycopg2.connect(**db_params)
        return conn
    except Exception as e:
        print(f"Error al conectar a la base de datos: {e}")
        return None

def get_last_dim_enphase_record(conn):
    try:
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM dim_enphase ORDER BY e_timestamp DESC LIMIT 1")
        record = cursor.fetchone()
        cursor.close()
        if record:
            return record
        else:
            return None
    except Exception as e:
        print(f"Error al obtener el último registro de dim_enphase: {e}")
        return None

def get_last_dim_froniusdatamanager_record(conn):
    try:
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM dim_froniusdatamanager ORDER BY fdm_timestamp DESC LIMIT 1")
        record = cursor.fetchone()
        cursor.close()
        if record:
            return record
        else:
            return None
    except Exception as e:
        print(f"Error al obtener el último registro de dim_froniusdatamanager: {e}")
        return None

def get_last_dim_froniusdevice_records(conn, limit=6):
    try:
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM dim_froniusdevice ORDER BY fd_timestamp DESC LIMIT %s", (limit,))
        records = cursor.fetchall()
        cursor.close()
        if records:
            return records
        else:
            return None
    except Exception as e:
        print(f"Error al obtener los últimos registros de dim_froniusdevice: {e}")
        return None
    

def calculateFdInd(records):
    # print(records[1])
    indicators = []

    ind_fd_fc = records[1] / (records[8] * 24)
    indicators.append(ind_fd_fc)

    ind_fd_hsp = (records[1] / 2) / 1000
    indicators.append(ind_fd_hsp)

    ind_fd_uacvsudc = records[3] / records[4]
    indicators.append(ind_fd_uacvsudc)

    ind_fd_iacvsidc = records[5] / records[6]
    indicators.append(ind_fd_iacvsidc)
    # for record in records:
    #     print()

    #     # energyDay = record[0]
    #     # Calcula los indicadores
    #     # ind_fd_fc = dim_fronius_data['fd_energyday'] / (dim_fronius_data['fd_nominalpower'] * 24)
    #     # ind_fd_fc = 
    #     # print(energyDay)
    #     # ind_fd_hsp = (dim_fronius_data['fd_energyday'] / 2) / 1000
    #     # ind_fd_uacvsudc = dim_fronius_data['fd_uac'] / dim_fronius_data['fd_udc']
    #     # ind_fd_iacvsidc = dim_fronius_data['fd_iac'] / dim_fronius_data['fd_idc']
    #     # ind_e_hsp = (dim_enphase_data['e_energyday'] / 2) / 1000
    #     # ind_e_fc = dim_enphase_data['e_energyday'] / (dim_enphase_data['e_nominalpower'] * 24)

    #     # # Calcular ind_e_meanp - Promedio de las potencias P_I1 a P_I20
    #     # potencia_promedio = sum(dim_enphase_data[f'P_I{i}'] for i in range(1, 21)) / 20
    #     # ind_e_meanp = potencia_promedio
    #     # Realiza tus cálculos personalizados aquí para cada registro.
    #     # Aquí hay un ejemplo simple para calcular un indicador ficticio:
    #     # Supongamos que queremos calcular el indicador como la suma de dos columnas del registro.
    #     # indicator = record[1] + record[2]  # Suma de la segunda y tercera columna (cambia esto según tus necesidades).
        
    #     # Agrega el indicador calculado a la lista de indicadores.
    #     # indicators.append(indicator)
    
    return indicators

def calculateEInd(record):

    indicators = []

    ind_e_hsp = (record[1] / 2) / 1000
    indicators.append(ind_e_hsp)

    ind_e_fc = record[1] / (record[23] * 24)
    indicators.append(ind_e_fc)

    return indicators

    

        # return

if __name__ == "__main__":

    indicators = []
    fd_indicators = []
    e_indicators = []

    db_params = {
        "host": "54.145.74.186",
        "port": 5555,
        "database": "postgres",
        "user": "postgres",
        "password": "post123"
    }

    conn = connect_to_database(db_params)

    if conn:
        last_dim_enphase_record = get_last_dim_enphase_record(conn)
        last_dim_froniusdatamanager_record = get_last_dim_froniusdatamanager_record(conn)
        last_dim_froniusdevice_records = get_last_dim_froniusdevice_records(conn, limit=6)

        if last_dim_enphase_record:
        #    print("Último registro de dim_enphase:", last_dim_enphase_record)
           e_indicators = calculateEInd(last_dim_enphase_record)
           print(e_indicators)
        
        # if last_dim_froniusdatamanager_record:
        #    print("Último registro de dim_froniusdatamanager:", last_dim_froniusdatamanager_record)
        
        if last_dim_froniusdevice_records:
            # print("Últimos registros de dim_froniusdevice:")
            for record in last_dim_froniusdevice_records:
                fd_indicators.append(calculateFdInd(record))

            print(fd_indicators)

        conn.close()

'''
id_fd1	id_fd2	id_fd3	id_fd4	id_fd5	id_fd6	id_e	id_fdm	ind_fd1_fc	ind_fd2_fc	ind_fd3_fc	ind_fd4_fc	ind_fd5_fc	ind_fd6_fc	ind_fd1_hsp	ind_fd2_hsp	ind_fd3_hsp	ind_fd4_hsp	ind_fd5_hsp	ind_fd6_hsp	ind_fd1_uacvsudc	ind_fd2_uacvsudc	ind_fd3_uacvsudc	ind_fd4_uacvsudc	ind_fd5_uacvsudc	ind_fd6_uacvsudc	ind_fd1_iacvsidc	ind_fd2_iacvsidc	ind_fd3_iacvsidc	ind_fd4_iacvsidc	ind_fd5_iacvsidc	ind_fd6_iacvsidc	ind_e_hsp	ind_e_fc	ind_e_meanp	facts_timestamp	e_devicename	fd1_devicename	fd2_devicename	fd3_devicename	fd4_devicename	fd5_devicename	fd6_devicename	fdm_devicename	fk_fd1	fk_fd2	fk_fd3	fk_fd4	fk_fd5	fk_fd6 timestamp_facts
'''


