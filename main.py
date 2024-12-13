from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, current_timestamp, struct, to_json
from pyspark.sql.types import (
    DoubleType,
    StructType,
    StructField,
    StringType,
    IntegerType,
)

# Створення Spark сесії
spark = (
    SparkSession.builder.appName("Athlete Data Processing")
    .master("local[*]")
    .config(
        "spark.jars.packages",
        "mysql:mysql-connector-java:8.0.32,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3",
    )
    .getOrCreate()
)

# Визначення схеми для JSON-даних
json_schema = StructType(
    [
        StructField("athlete_id", IntegerType(), True),
        StructField("sport", StringType(), True),
        StructField("medal", StringType(), True),
    ]
)

# 1. Зчитування даних з MySQL
jdbc_url = "jdbc:mysql://217.61.57.46:3306/neo_data"
db_properties = {
    "user": "neo_data_admin",
    "password": "Proyahaxuqithab9oplp",
    "driver": "com.mysql.cj.jdbc.Driver",
}

athlete_bio_df = spark.read.jdbc(
    url=jdbc_url, table="olympic_dataset.athlete_bio", properties=db_properties
)

# 2. Фільтрація даних за зростом і вагою
athlete_bio_filtered_df = (
    athlete_bio_df.withColumn("height", col("height").cast(DoubleType()))
    .withColumn("weight", col("weight").cast(DoubleType()))
    .filter((col("height").isNotNull()) & (col("weight").isNotNull()))
)

print("Filtered Athlete Bio DataFrame:")
athlete_bio_filtered_df.show()

# 3. Зчитування даних з MySQL і запис у Kafka
athlete_event_results_df = spark.read.jdbc(
    url=jdbc_url,
    table="olympic_dataset.athlete_event_results",
    properties=db_properties,
    column="result_id",  # Колонка для партиціонування
    lowerBound=1,  # Мінімальне значення result_id
    upperBound=100000,  # Максимальне значення result_id
    numPartitions=10,  # Кількість партицій
)

print("Athlete Event Results DataFrame:")
athlete_event_results_df.show()

athlete_event_results_df.selectExpr(
    "CAST(result_id AS STRING) AS key",
    "to_json(struct(athlete_id, sport, medal)) AS value",
).write.format("kafka").option("kafka.bootstrap.servers", "77.81.230.104:9092").option(
    "kafka.security.protocol", "SASL_PLAINTEXT"
).option(
    "kafka.sasl.mechanism", "PLAIN"
).option(
    "kafka.sasl.jaas.config",
    "org.apache.kafka.common.security.plain.PlainLoginModule required username='admin' password='VawEzo1ikLtrA8Ug8THa';",
).option(
    "topic", "athlete_event_results"
).save()

# Зчитування даних із Kafka
data_from_kafka = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", "77.81.230.104:9092")
    .option("kafka.security.protocol", "SASL_PLAINTEXT")
    .option("kafka.sasl.mechanism", "PLAIN")
    .option(
        "kafka.sasl.jaas.config",
        "org.apache.kafka.common.security.plain.PlainLoginModule required username='admin' password='VawEzo1ikLtrA8Ug8THa';",
    )
    .option("subscribe", "athlete_event_results")
    .load()
)

from pyspark.sql.functions import from_json

data_as_json = (
    data_from_kafka.selectExpr("CAST(value AS STRING) AS json_data")
    .select(from_json(col("json_data"), json_schema).alias("data"))
    .select("data.*")
)

print("Data as JSON:")
data_from_kafka.show()
# # Для дебагінгу, перевіримо, що дані декодуються правильно
# query = (
#     data_from_kafka.writeStream.outputMode("complete")
#     .format("console")
#     .option("truncate", False)
#     .start()
#     .awaitTermination()
# )

# # 4. Об’єднання даних
# combined_df = athlete_event_results_df.select("athlete_id", "sport", "medal").join(
#     athlete_bio_filtered_df, on="athlete_id", how="inner"
# )

# print("Combined DataFrame (обмежений):")
# combined_df.show()

# # 5. Розрахунок середніх показників
# aggregated_df = (
#     combined_df.groupBy("sport", "medal")
#     .agg(avg("height").alias("avg_height"), avg("weight").alias("avg_weight"))
#     .withColumn("timestamp", current_timestamp())
# )

# print("Aggregated DataFrame:")
# aggregated_df.show()

# # 6. Вивід результатів
# # a) Стрим у Kafka
# aggregated_df.selectExpr(
#     "CAST(NULL AS STRING) AS key", "to_json(struct(*)) AS value"
# ).write.format("kafka").option("kafka.bootstrap.servers", "77.81.230.104:9092").option(
#     "kafka.security.protocol", "SASL_PLAINTEXT"
# ).option(
#     "kafka.sasl.mechanism", "PLAIN"
# ).option(
#     "kafka.sasl.jaas.config",
#     "org.apache.kafka.common.security.plain.PlainLoginModule required username='admin' password='VawEzo1ikLtrA8Ug8THa';",
# ).option(
#     "topic", "athlete_event_results"
# ).save()


# # b) Стрим у базу даних
# def write_to_mysql(batch_df, batch_id):
#     batch_df.select(
#         "sport", "medal", "avg_height", "avg_weight", "timestamp"
#     ).write.jdbc(
#         url=jdbc_url,
#         table="neo_data.oleh_aggregated_stats",
#         mode="complete",
#         properties=db_properties,
#     )


# combined_streaming_df = data_as_json.join(
#     athlete_bio_filtered_df, on="athlete_id", how="inner"
# )

# aggregated_streaming_df = (
#     combined_streaming_df.groupBy("sport", "medal")
#     .agg(avg("height").alias("avg_height"), avg("weight").alias("avg_weight"))
#     .withColumn("timestamp", current_timestamp())
# )

# print("Aggregated Streaming DataFrame before streaming:")
# # Для дебагінгу, перевіримо, що дані декодуються правильно
# query = (
#     aggregated_streaming_df.writeStream.outputMode("complete")
#     .format("console")
#     .option("truncate", False)
#     .start()
# )

# query.awaitTermination()

# aggregated_streaming_df.writeStream.foreachBatch(write_to_mysql).option(
#     "checkpointLocation", "/path/to/mysql/checkpoint"
# ).start()

# spark.streams.awaitAnyTermination()
