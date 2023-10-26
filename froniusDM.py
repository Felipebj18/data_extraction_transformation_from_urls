from get_json import get_json_from_url
import json

def openJson(fileName):
    with open(fileName, 'r') as archivo_json:
        datos = json.load(archivo_json)

    return datos

def load_data(urls):
    for url_id,attributes in urls:
        aux_data = get_json_from_url(attributes["url"])
        if(url_id=="url1"):
            radiation_fronius_1=aux_data["Radiacion"]["value"]
            temp1_fronius_1 = aux_data["Temperatura_1"]["value"]
            temp2_fronius_2 = aux_data["Temperatura_2"]["value"]
            str_f=f"rad:{radiation_fronius_1},t1:{temp1_fronius_1},t2:{temp2_fronius_2}"
        return str_f
        # elif(url_id=="url2"):
        # elif(url_id=="url3"):    
        

# id = data["id"]
# type = data["type"]
# temp1=data["Temperatura_1"]["value"]
# temp2=data["Temperatura_2"]["value"]

print(load_data(openJson("./urlsFroniusDM.json")))
