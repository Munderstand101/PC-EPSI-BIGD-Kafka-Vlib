import json
from kafka import KafkaConsumer

station_data_bikes = {}

consumer_bikes = KafkaConsumer('velo_disponibilite_par_station', group_id='velo_station_group',
                               bootstrap_servers='localhost:9092', enable_auto_commit=False)

for msg in consumer_bikes:
    data = json.loads(msg.value)
    station_id = data['station_id']

    if station_id not in station_data_bikes:
        station_data_bikes[station_id] = {'total_bikes': 0, 'count': 0}

    station_data_bikes[station_id]['total_bikes'] += data['bikes_available']
    station_data_bikes[station_id]['count'] += 1

    average_bikes = station_data_bikes[station_id]['total_bikes'] / station_data_bikes[station_id]['count']
    print(f"Moyenne des vélos disponibles pour la station {station_id} (Commit Key: {msg.offset}): {average_bikes}")