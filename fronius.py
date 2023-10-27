from get_json import get_json_from_url,load_data,openJson

data_dict = load_data(openJson("urlsFronius.json"))

def create_dim_froniusdevice_tuples(data_dict):
    """
    Crea una lista de tuplas con los datos de los dispositivos FroniusDevice a partir de un diccionario.

    Args:
        data_dict (dict): Un diccionario con los datos de los dispositivos FroniusDevice.

    Returns:
        list: Una lista de tuplas con los campos de la tabla Dim_FroniusDevice.
    """
    dim_froniusdevice_tuples = []

    for device_name, device_data in data_dict.items():
        dim_froniusdevice_tuples.append(
            (
                device_name,  # ID_FD (nombre del dispositivo)
                float(device_data.get('DAY_ENERGY', 0.0)),
                float(device_data.get('FAC', 0.0)),
                float(device_data.get('IAC', 0.0)),
                float(device_data.get('IDC', 0.0)),
                float(device_data.get('PAC', 0.0)),
                float(device_data.get('TOTAL_ENERGY', 0.0)),
                float(device_data.get('UAC', 0.0)),
                float(device_data.get('UDC', 0.0)),
                float(device_data.get('YEAR_ENERGY', 0.0)),
            )
        )

    return dim_froniusdevice_tuples


dimension_data = create_dim_froniusdevice_tuples(data_dict)
print(dimension_data)


# # Lista para almacenar las tuplas de datos
# dimension_data_list = []

# # Iterar a través de las claves del diccionario
# for device_name, device_data in data_dict.items():
#     device_tuple = (
#         device_name,  # ID_FD (nombre del dispositivo)
#         float(device_data['DAY_ENERGY']),
#         float(device_data['FAC']),
#         float(device_data['IAC']),
#         float(device_data['IDC']),
#         int(device_data['PAC']),
#         float(device_data['TOTAL_ENERGY']),
#         float(device_data['UAC']),
#         float(device_data['UDC']),
#         float(device_data['YEAR_ENERGY'])
#     )
#     dimension_data_list.append(device_tuple)

# # Imprimir la lista de tuplas
# for dimension_data in dimension_data_list:
#     print(dimension_data)
