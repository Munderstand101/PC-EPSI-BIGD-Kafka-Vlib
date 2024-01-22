import json
from kafka import KafkaConsumer

station_data_docks = {}

consumer_docks = KafkaConsumer('bornette_libre_par_station', group_id='velo_station_group',
                               bootstrap_servers='localhost:9092', enable_auto_commit=False)

for msg in consumer_docks:
    data = json.loads(msg.value)
    station_id = data['station_id']

    if station_id not in station_data_docks:
        station_data_docks[station_id] = {'total_docks': 0, 'count': 0}

    num_docks_available = data.get('num_docks_available', 0)

    station_data_docks[station_id]['total_docks'] += num_docks_available
    station_data_docks[station_id]['count'] += 1

    average_docks = station_data_docks[station_id]['total_docks'] / station_data_docks[station_id]['count']
    print(f"Taux quotidien de bornettes libres pour la station {station_id} (Commit Key: {msg.offset}): {average_docks}")