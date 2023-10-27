from get_json import get_json_from_url
import json

def openJson(fileName):
    with open(fileName, 'r') as archivo_json:
        datos = json.load(archivo_json)

    return datos

def load_data(urls):
    for url_id in urls:
        url=urls[url_id]["url"]
        aux_data = get_json_from_url(url)
        
        id=aux_data["id"]
        radiation_fronius=aux_data["Radiacion"]["value"]
        temp1_fronius = aux_data["Temperatura_1"]["value"]
        temp2_fronius = aux_data["Temperatura_2"]["value"]
        result_dict = {"Radicion":radiation_fronius,"Temperatura_1":temp1_fronius,"Temperatura_2":temp2_fronius}
        final_dict = {id:result_dict}
        return final_dict
 
print(load_data(openJson("./urlsFroniusDM.json")))
