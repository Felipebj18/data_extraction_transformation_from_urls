from get_json import get_json_from_url,load_data,openJson
 
data_dict = load_data(openJson("urlsFroniusDM.json"))

def create_dim_froniusdatamanager_tuple(data_dict):
    """
    Crea una tupla con los datos de un dispositivo FroniusDataManager a partir de un diccionario.

    Args:
        data_dict (dict): Un diccionario con los datos del dispositivo FroniusDataManager.

    Returns:
        tuple: La tupla con los campos de la tabla Dim_FroniusDataManager.
    """
    device_name = list(data_dict.keys())[0]  # Obtiene el nombre del dispositivo
    device_data = data_dict[device_name]

    return (
        device_name,  # ID_FDM (nombre del dispositivo)
        float(device_data.get('Radiacion', 0.0)),
        float(device_data.get('Temperatura_1', 0.0)),
        float(device_data.get('Temperatura_2', 0.0))
    )

# Ejemplo de uso:

dimension_data = create_dim_froniusdatamanager_tuple(data_dict)
print(dimension_data)

# # Lista para almacenar las tuplas de datos
# dimension_data_list = []

# # Iterar a través de las claves del diccionario
# for device_name, device_data in data_dict.items():
#     device_tuple = (
#         device_name,  # ID_FDM (nombre del dispositivo)
#         float(device_data['Radiacion']),
#         float(device_data['Temperatura_1']),
#         float(device_data['Temperatura_2'])
#     )
#     dimension_data_list.append(device_tuple)

# # Imprimir la lista de tuplas
# for dimension_data in dimension_data_list:
#     print(dimension_data)
