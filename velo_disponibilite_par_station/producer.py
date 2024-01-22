import time
import json
import requests
from kafka import KafkaProducer

producteur_bikes = KafkaProducer(bootstrap_servers='localhost:9092')

def get_api_data():
    api_url = "http://127.0.0.1:8000/data/timestamp/20240122100901"
    response = requests.get(api_url)

    if response.status_code == 200:
        return response.json()
    else:
        print(f"Erreur lors de la récupération des données de l'API. Code d'erreur : {response.status_code}")
        return None

while True:
    api_data = get_api_data()

    if api_data:
        for station_data in api_data['data']['stations']:
            station_id = station_data['station_id']
            bikes_available = station_data['num_bikes_available']

            msg_bikes = {'station_id': station_id, 'bikes_available': bikes_available}
            producteur_bikes.send("velo_disponibilite_par_station", json.dumps(msg_bikes).encode(), str(station_id).encode())

        time.sleep(60)
