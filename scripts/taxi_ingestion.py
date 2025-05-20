# scripts/taxi_ingestion.py
import os
import requests
import pandas as pd
from minio import Minio

def download_taxi_data(year, month):
    """Télécharge les données Yellow Taxi pour une période donnée"""
    url = f"https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{year}-{month:02d}.parquet"
    local_path = f"/tmp/yellow_tripdata_{year}-{month:02d}.parquet"
    
    response = requests.get(url, stream=True)
    with open(local_path, 'wb') as f:
        for chunk in response.iter_content(chunk_size=8192):
            f.write(chunk)
    
    return local_path

def upload_to_minio(file_path, bucket_name="taxi-data"):
    """Upload le fichier vers MinIO"""
    client = Minio(
        "minio:9000",
        access_key="minioadmin",
        secret_key="minioadmin",
        secure=False
    )
    
    # Créer le bucket s'il n'existe pas
    if not client.bucket_exists(bucket_name):
        client.make_bucket(bucket_name)
    
    # Upload le fichier
    file_name = os.path.basename(file_path)
    client.fput_object(bucket_name, file_name, file_path)
    
    return f"s3://{bucket_name}/{file_name}"

# scripts/taxi_ingestion.py
def main(years=[2022, 2023, 2024], months=None):
    """Fonction principale pour l'ingestion des données taxi pour plusieurs périodes"""
    # Si months n'est pas spécifié, utiliser tous les mois pour chaque année
    if months is None:
        # Pour 2022 et 2023, prendre tous les mois
        # Pour 2024, prendre jusqu'à avril (ou le mois actuel si avant avril)
        periods = []
        for year in years:
            if year == 2024:
                max_month = 4  # Jusqu'à avril 2024
            else:
                max_month = 12  # Toute l'année
            
            for month in range(1, max_month + 1):
                periods.append((year, month))
    else:
        # Utiliser les mois spécifiés pour chaque année
        periods = [(year, month) for year in years for month in months]
    
    # Télécharger et traiter chaque période
    for year, month in periods:
        try:
            print(f"Traitement des données pour {year}-{month:02d}")
            file_path = download_taxi_data(year, month)
            minio_path = upload_to_minio(file_path)
            os.remove(file_path)
            print(f"Données taxi importées: {minio_path}")
        except Exception as e:
            print(f"Erreur lors du traitement des données pour {year}-{month:02d}: {e}")

if __name__ == "__main__":
    import sys
    year = int(sys.argv[1]) if len(sys.argv) > 1 else 2022
    month = int(sys.argv[2]) if len(sys.argv) > 2 else 1
    main(year, month)