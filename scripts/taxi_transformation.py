# scripts/taxi_transformation.py
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, hour, dayofweek, when, lit, expr

def create_spark_session():
    """Crée une session Spark"""
    return SparkSession.builder \
        .appName("TaxiTransformation") \
        .config("spark.jars.packages", "org.postgresql:postgresql:42.2.18") \
        .getOrCreate()

def process_taxi_data(spark, input_path, output_table):
    """Traite les données taxi avec PySpark"""
    # Lire les données depuis MinIO
    df = spark.read.parquet(input_path)
    
    # Nettoyage et transformation
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
    
    # Enregistrer dans PostgreSQL
    result_df.write \
        .format("jdbc") \
        .option("url", "jdbc:postgresql://postgres:5432/airflow") \
        .option("dbtable", output_table) \
        .option("user", "airflow") \
        .option("password", "airflow") \
        .mode("overwrite") \
        .save()

def main():
    """Fonction principale pour la transformation des données taxi"""
    spark = create_spark_session()
    
    input_path = "s3a://taxi-data/"
    output_table = "fact_taxi_trips"
    
    process_taxi_data(spark, input_path, output_table)
    
    spark.stop()

if __name__ == "__main__":
    main()