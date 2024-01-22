import json
from kafka import KafkaConsumer

mem_bikes = {str(c): 0 for c in range(1, 10)}  # Remplacez 1, 10 par la plage réelle des station_id

consumer_bikes = KafkaConsumer('velo_disponibilite_par_station', group_id='velo_station_group',
                               bootstrap_servers='localhost:9092', enable_auto_commit=True,
                               auto_commit_interval_ms=1000)  # Auto-commit toutes les secondes

for msg in consumer_bikes:
    data = json.loads(msg.value)
    station_id = data['station_id']

    mem_bikes[str(station_id)] = data['bikes_available']
    # Calcul de la moyenne quotidienne pour les vélos disponibles (indicateur prescriptif)
    daily_avg_bikes = sum(mem_bikes.values()) / len(mem_bikes)
    print(f"Moyenne quotidienne des vélos disponibles par station : {daily_avg_bikes}")
    # Ajoutez ici la logique pour les indicateurs prédictifs en utilisant des modèles de séries temporelles
    # (cette partie dépend des bibliothèques et modèles que vous choisissez pour la prédiction)
