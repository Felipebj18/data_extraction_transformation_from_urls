from get_json import get_json_from_url,load_data,openJson

data_dict = load_data(openJson("urlsEnphase.json"))

def create_dim_enphase_tuples(data_dict):
    """
    Crea una lista de tuplas con los datos de los dispositivos Enphase a partir de un diccionario.

    Args:
        data_dict (dict): Un diccionario con los datos de los dispositivos Enphase.

    Returns:
        list: Una lista de tuplas con los campos de la tabla Dim_Enphase.
    """
    dim_enphase_tuples = []

    for device_name, device_data in data_dict.items():
        dim_enphase_tuples.append(
            (
                device_name,  # ID_E (nombre del dispositivo)
                float(device_data.get('EnergyDay', 0.0)),
                float(device_data.get('PAC', 0.0)),
                float(device_data.get('P_I1', 0.0)),
                float(device_data.get('P_I2', 0.0)),
                float(device_data.get('P_I3', 0.0)),
                float(device_data.get('P_I4', 0.0)),
                float(device_data.get('P_I5', 0.0)),
                float(device_data.get('P_I6', 0.0)),
                float(device_data.get('P_I7', 0.0)),
                float(device_data.get('P_I8', 0.0)),
                float(device_data.get('P_I9', 0.0)),
                float(device_data.get('P_I10', 0.0)),
                float(device_data.get('P_I11', 0.0)),
                float(device_data.get('P_I12', 0.0)),
                float(device_data.get('P_I13', 0.0)),
                float(device_data.get('P_I14', 0.0)),
                float(device_data.get('P_I15', 0.0)),
                float(device_data.get('P_I16', 0.0)),
                float(device_data.get('P_I17', 0.0)),
                float(device_data.get('P_I18', 0.0)),
                float(device_data.get('P_I19', 0.0)),
                float(device_data.get('P_I20', 0.0)),
                float(device_data.get('EnergyTotal', 0.0)),
            )
        )

    return dim_enphase_tuples


dimension_data = create_dim_enphase_tuples(data_dict)
print(dimension_data)


# # Extraer el campo e_devicename y los demás campos
# e_devicename = list(data_dict.keys())[0]  # En este caso, solo hay una clave en el diccionario
# other_fields = data_dict[e_devicename]

# # Crear una tupla con los valores de los campos en el orden requerido
# dimension_data_tuple = (
#     e_devicename,
#     float(other_fields['EnergyDay']),
#     float(other_fields['EnergyTotal']),
#     int(other_fields['PAC']),
#     int(other_fields['P_I1']),
#     int(other_fields['P_I10']),
#     int(other_fields['P_I11']),
#     int(other_fields['P_I12']),
#     int(other_fields['P_I13']),
#     int(other_fields['P_I14']),
#     int(other_fields['P_I15']),
#     int(other_fields['P_I16']),
#     int(other_fields['P_I17']),
#     int(other_fields['P_I18']),
#     int(other_fields['P_I19']),
#     int(other_fields['P_I2']),
#     int(other_fields['P_I20']),
#     int(other_fields['P_I3']),
#     int(other_fields['P_I4']),
#     int(other_fields['P_I5']),
#     int(other_fields['P_I6']),
#     int(other_fields['P_I7']),
#     int(other_fields['P_I8']),
#     int(other_fields['P_I9'])
# )

# print(dimension_data_tuple)

