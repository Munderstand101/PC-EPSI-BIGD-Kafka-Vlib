import time
import json
import requests
from kafka import KafkaProducer

producer_docks = KafkaProducer(bootstrap_servers='localhost:9092')


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

            num_docks_available = station_data.get('num_docks_available', station_data.get('docks_available', 0))

            msg_docks = {'station_id': station_id, 'num_docks_available': num_docks_available}
            producer_docks.send("bornette_libre_par_station", json.dumps(msg_docks).encode(), str(station_id).encode())

        time.sleep(60)