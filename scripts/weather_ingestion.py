# scripts/weather_ingestion.py
import os
import json
import time
import datetime
import requests
from minio import Minio

def fetch_weather_data(api_key, city="New York"):
    """Récupère les données météo pour une ville donnée"""
    url = f"http://api.openweathermap.org/data/2.5/weather?q={city}&appid={api_key}&units=metric"
    response = requests.get(url)
    return response.json()

def upload_to_minio(data, bucket_name="weather-data"):
    """Upload les données météo vers MinIO"""
    client = Minio(
        "minio:9000",
        access_key="minioadmin",
        secret_key="minioadmin",
        secure=False
    )
    
    # Créer le bucket s'il n'existe pas
    if not client.bucket_exists(bucket_name):
        client.make_bucket(bucket_name)
    
    # Créer un fichier temporaire
    timestamp = datetime.datetime.now().strftime("%Y%m%d%H%M%S")
    file_name = f"weather_{timestamp}.json"
    local_path = f"/tmp/{file_name}"
    
    with open(local_path, 'w') as f:
        json.dump(data, f)
    
    # Upload le fichier
    client.fput_object(bucket_name, file_name, local_path)
    
    # Nettoyage
    os.remove(local_path)
    
    return f"s3://{bucket_name}/{file_name}"

def main(api_key, interval=3600):
    """Fonction principale pour l'ingestion des données météo"""
    # Simuler un streaming en récupérant les données toutes les heures
    data = fetch_weather_data(api_key)
    minio_path = upload_to_minio(data)
    print(f"Données météo importées: {minio_path}")
    
    # Dans un vrai cas d'utilisation, on aurait une boucle infinie avec un sleep
    # while True:
    #     data = fetch_weather_data(api_key)
    #     minio_path = upload_to_minio(data)
    #     print(f"Données météo importées: {minio_path}")
    #     time.sleep(interval)

if __name__ == "__main__":
    import sys
    api_key = sys.argv[1] if len(sys.argv) > 1 else "951d8917fa8b154afd44712c1c73ac4c"
    main(api_key)