from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    DoubleType,
)

# Ініціалізація Spark сесії
spark = SparkSession.builder.appName("KafkaStreamProcessing").getOrCreate()

# Визначення схеми для JSON-даних
json_schema = StructType(
    [
        StructField("athlete_id", IntegerType(), True),
        StructField("sport", StringType(), True),
        StructField("medal", StringType(), True),
        StructField("height", DoubleType(), True),
        StructField("weight", DoubleType(), True),
        StructField("country_noc", StringType(), True),
        StructField("sex", StringType(), True),
    ]
)

# Читання даних із Kafka
data_from_kafka = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", "77.81.230.104:9092")
    .option("kafka.security.protocol", "SASL_PLAINTEXT")
    .option("kafka.sasl.mechanism", "PLAIN")
    .option(
        "kafka.sasl.jaas.config",
        'org.apache.kafka.common.security.plain.PlainLoginModule required username="admin" password="VawEzo1ikLtrA8Ug8THa";',
    )
    .option("subscribe", "oleh_athlete_event_results")
    .option("startingOffsets", "earliest")
    .option("maxOffsetsPerTrigger", "10")
    .option("failOnDataLoss", "false")
    .load()
)

# Обробка даних
processed_data = (
    data_from_kafka.selectExpr("CAST(value AS STRING)")
    .select(from_json(col("value"), json_schema).alias("data"))
    .select(
        "data.athlete_id",
        "data.sport",
        "data.medal",
        "data.height",
        "data.weight",
        "data.country_noc",
        "data.sex",
    )
)

# Виведення даних у консоль
processed_data.writeStream.trigger(processingTime="10 seconds").outputMode(
    "append"
).format("console").option("truncate", "false").start().awaitTermination()
