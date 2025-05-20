# scripts/weather_transformation.py
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, when, hour, dayofweek
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType

def create_spark_session():
    """Crée une session Spark"""
    return SparkSession.builder \
        .appName("WeatherTransformation") \
        .config("spark.jars.packages", "org.postgresql:postgresql:42.2.18") \
        .getOrCreate()

def process_weather_data(spark):
    """Traite les données météo avec PySpark Streaming"""
    # Définir le schéma pour les données météo
    schema = StructType([
        StructField("dt", LongType(), True),
        StructField("main", StructType([
            StructField("temp", DoubleType(), True),
            StructField("humidity", DoubleType(), True)
        ]), True),
        StructField("wind", StructType([
            StructField("speed", DoubleType(), True)
        ]), True),
        StructField("weather", StructType([
            StructField("main", StringType(), True),
            StructField("description", StringType(), True)
        ]), True)
    ])
    
    # Lire les données depuis MinIO (simulation de streaming)
    df = spark.readStream \
        .format("json") \
        .schema(schema) \
        .option("path", "s3a://weather-data/") \
        .load()
    
    # Transformation
    transformed_df = df.select(
        col("dt").cast("timestamp").alias("timestamp"),
        col("main.temp").alias("temperature"),
        col("main.humidity").alias("humidity"),
        col("wind.speed").alias("wind_speed"),
        col("weather.main").alias("weather_condition")
    ).withColumn(
        "weather_category",
        when(col("weather_condition").like("%Rain%"), "Rainy")
        .when(col("weather_condition").like("%Cloud%"), "Cloudy")
        .when(col("weather_condition").like("%Clear%"), "Clear")
        .otherwise("Other")
    ).withColumn(
        "hour", 
        hour(col("timestamp"))
    ).withColumn(
        "day_of_week", 
        dayofweek(col("timestamp"))
    )
    
    # Écrire dans PostgreSQL
    query = transformed_df.writeStream \
        .foreachBatch(lambda df, epoch_id: df.write \
            .format("jdbc") \
            .option("url", "jdbc:postgresql://postgres:5432/airflow") \
            .option("dbtable", "dim_weather") \
            .option("user", "airflow") \
            .option("password", "airflow") \
            .mode("append") \
            .save()) \
        .outputMode("append") \
        .start()
    
    query.awaitTermination()

def main():
    """Fonction principale pour la transformation des données météo"""
    spark = create_spark_session()
    process_weather_data(spark)
    spark.stop()

if __name__ == "__main__":
    main()