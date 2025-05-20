# scripts/taxi_transformation.py
import os
import findspark
findspark.init()  # Initialiser findspark pour trouver l'installation de Spark

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, hour, dayofweek, when, lit, expr

def create_spark_session():
    """Crée une session Spark"""
    # Configuration des credentials AWS pour l'accès S3A
    os.environ['AWS_ACCESS_KEY_ID'] = 'minioadmin'
    os.environ['AWS_SECRET_ACCESS_KEY'] = 'minioadmin'
    
    # Création de la session Spark avec configuration S3A
    spark = SparkSession.builder \
        .appName("TaxiTransformation") \
        .config("spark.jars.packages", 
                "org.postgresql:postgresql:42.2.18,org.apache.hadoop:hadoop-aws:3.3.1") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
        .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
        .config("spark.driver.memory", "2g") \
        .config("spark.executor.memory", "2g") \
        .master("local[*]") \
        .getOrCreate()
    
    return spark

def process_taxi_data(spark, input_path, output_table):
    """Traite les données taxi avec PySpark"""
    # Conversion du chemin S3 en format S3A pour Spark
    input_path = input_path.replace("s3://", "s3a://")
    
    try:
        # Liste des fichiers dans le bucket MinIO
        files = spark._jsc.hadoopConfiguration().get("fs.s3a.impl")
        print(f"Implémentation S3A: {files}")
        
        # Lecture des fichiers parquet
        df = spark.read.parquet(input_path)
        
        # Votre code de transformation existant
        transformed_df = df.withColumn(
            "trip_duration", 
            expr("unix_timestamp(tpep_dropoff_datetime) - unix_timestamp(tpep_pickup_datetime)")
        ).withColumn(
            "distance_category",
            when(col("trip_distance") < 2, "0-2 km")
            .when(col("trip_distance") < 5, "2-5 km")
            .otherwise(">5 km")
        ).withColumn(
            "tip_percentage", 
            when(col("fare_amount") > 0, col("tip_amount") / col("fare_amount") * 100)
            .otherwise(0)
        ).withColumn(
            "pickup_hour", 
            hour(col("tpep_pickup_datetime"))
        ).withColumn(
            "pickup_day", 
            dayofweek(col("tpep_pickup_datetime"))
        )
        
        # Sélectionner les colonnes pertinentes
        result_df = transformed_df.select(
            "tpep_pickup_datetime",
            "tpep_dropoff_datetime",
            "trip_duration",
            "distance_category",
            "payment_type",
            "tip_percentage",
            "pickup_hour",
            "pickup_day",
            "PULocationID",
            "DOLocationID",
            "fare_amount",
            "tip_amount",
            "total_amount",
            "passenger_count"
        )
        
        # Écriture vers PostgreSQL avec le driver JDBC approprié
        result_df.write \
            .format("jdbc") \
            .option("url", "jdbc:postgresql://postgres:5432/airflow") \
            .option("dbtable", output_table) \
            .option("user", "airflow") \
            .option("password", "airflow") \
            .option("driver", "org.postgresql.Driver") \
            .mode("overwrite") \
            .save()
            
        print(f"Traitement et sauvegarde des données taxi réussis dans {output_table}")
        
    except Exception as e:
        print(f"Erreur lors du traitement des données taxi: {str(e)}")
        raise e

def main():
    """Fonction principale pour la transformation des données taxi"""
    try:
        print("Démarrage de la transformation des données taxi")
        spark = create_spark_session()
        
        input_path = "s3a://taxi-data/"
        output_table = "fact_taxi_trips"
        
        process_taxi_data(spark, input_path, output_table)
        
        spark.stop()
        print("Transformation des données taxi terminée avec succès")
        
    except Exception as e:
        print(f"Erreur dans la fonction principale de transformation taxi: {str(e)}")
        raise e

if __name__ == "__main__":
    main()