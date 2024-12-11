from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, current_timestamp, when
from pyspark.sql.types import DoubleType

# Створення Spark сесії
spark = SparkSession.builder \
    .appName("Athlete Data Processing") \
    .config("spark.jars", "mysql-connector-java.jar") \
    .getOrCreate()

# 1. Зчитування даних з MySQL
jdbc_url = "jdbc:mysql://your_mysql_host:3306/olympic_dataset"
db_properties = {
    "user": "your_username",
    "password": "your_password",
    "driver": "com.mysql.cj.jdbc.Driver"
}

athlete_bio_df = spark.read.jdbc(url=jdbc_url, table="athlete_bio", properties=db_properties)

# 2. Фільтрація даних за зростом і вагою
athlete_bio_filtered_df = athlete_bio_df \
    .withColumn("height", col("height").cast(DoubleType())) \
    .withColumn("weight", col("weight").cast(DoubleType())) \
    .filter((col("height").isNotNull()) & (col("weight").isNotNull()))

# 3. Зчитування даних з MySQL і запис у Kafka
from pyspark.sql.avro.functions import to_avro

athlete_event_results_df = spark.read.jdbc(url=jdbc_url, table="athlete_event_results", properties=db_properties)

athlete_event_results_df \
    .select(to_avro(col("*")).alias("value")) \
    .write \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "your_kafka_broker:9092") \
    .option("topic", "athlete_event_results") \
    .save()

# Зчитування даних із Kafka
from pyspark.sql.avro.functions import from_avro

schema = athlete_event_results_df.schema

data_from_kafka = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "your_kafka_broker:9092") \
    .option("subscribe", "athlete_event_results") \
    .load()

data_as_json = data_from_kafka \
    .select(from_avro(col("value"), schema).alias("data")) \
    .select("data.*")

# 4. Об’єднання даних
combined_df = data_as_json.join(athlete_bio_filtered_df, on="athlete_id", how="inner")

# 5. Розрахунок середніх показників
aggregated_df = combined_df \
    .groupBy("sport", "medal", "gender", "country_noc") \
    .agg(
        avg("height").alias("avg_height"),
        avg("weight").alias("avg_weight")
    ) \
    .withColumn("timestamp", current_timestamp())

# 6. Вивід результатів
# a) Стрим у Kafka
aggregated_df \
    .select(to_avro(col("*")).alias("value")) \
    .writeStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "your_kafka_broker:9092") \
    .option("topic", "aggregated_athlete_stats") \
    .option("checkpointLocation", "/path/to/kafka/checkpoint") \
    .start()

# b) Стрим у базу даних
def write_to_mysql(batch_df, batch_id):
    batch_df.write.jdbc(url=jdbc_url, table="aggregated_stats", mode="append", properties=db_properties)

aggregated_df \
    .writeStream \
    .foreachBatch(write_to_mysql) \
    .option("checkpointLocation", "/path/to/mysql/checkpoint") \
    .start()

spark.streams.awaitAnyTermination()
