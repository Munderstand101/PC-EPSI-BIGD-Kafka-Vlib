import json
from kafka import KafkaConsumer

mem_docks = {str(c): 0 for c in range(1, 10)}  # Remplacez 1, 10 par la plage réelle des station_id

consumer_docks = KafkaConsumer('bornette_libre_par_station', group_id='bornette_station_group',
                                bootstrap_servers='localhost:9092', enable_auto_commit=True,
                                auto_commit_interval_ms=1000)  # Auto-commit toutes les secondes

for msg in consumer_docks:
    data = json.loads(msg.value)
    station_id = data['station_id']

    mem_docks[str(station_id)] = data['docks_available']
    # Calcul du taux quotidien de bornettes libres (indicateur prescriptif)
    total_docks = 10  # Remplacez par le nombre total de bornettes par station
    daily_docks_rate = sum(mem_docks.values()) / (total_docks * len(mem_docks))
    print(f"Taux quotidien de bornettes libres par station : {daily_docks_rate}")
    # Ajoutez ici la logique pour les indicateurs prédictifs en utilisant des modèles de séries temporelles
    # (cette partie dépend des bibliothèques et modèles que vous choisissez pour la prédiction)
