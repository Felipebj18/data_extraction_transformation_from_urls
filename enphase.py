from get_json import get_json_from_url
import json

def openJson(fileName):
    with open(fileName, 'r') as archivo_json:
        datos = json.load(archivo_json)

    return datos

def load_data(urls):
    final_dict={}
    for url_id in urls:
        url=urls[url_id]["url"]
        aux_data = get_json_from_url(url)
        result_dict = {}
        for key, value in aux_data.items():
            if isinstance(value, dict) and 'value' in value:
                # Si el valor es un diccionario con una clave "value", asigna ese valor
                result_dict[key] = value['value']
            else:
                result_dict[key] = value
        id=result_dict["id"]
        if 'id' in result_dict:
            result_dict.pop('id')
        final_dict[id]=result_dict
    return final_dict

print(load_data(openJson("./urlsEnphase.json")))