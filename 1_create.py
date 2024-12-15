from kafka.admin import KafkaAdminClient, NewTopic
from configs import kafka_config

# Створення клієнта Kafka
admin_client = KafkaAdminClient(
    bootstrap_servers=kafka_config["bootstrap_servers"],
    security_protocol=kafka_config["security_protocol"],
    sasl_mechanism=kafka_config["sasl_mechanism"],
    sasl_plain_username=kafka_config["username"],
    sasl_plain_password=kafka_config["password"],
)

# Визначення нових топіків
my_name = "oleh"
athlete_event_results = f"{my_name}_athlete_event_results"
aggregated_athlete_stats = f"{my_name}_aggregated_athlete_stats"
num_partitions = 2
replication_factor = 1

athlete_event_results = NewTopic(
    name=athlete_event_results,
    num_partitions=num_partitions,
    replication_factor=replication_factor,
)
aggregated_athlete_stats = NewTopic(
    name=aggregated_athlete_stats,
    num_partitions=num_partitions,
    replication_factor=replication_factor,
)

# Видалення старих топіків
try:
    admin_client.delete_topics(topics=[athlete_event_results, aggregated_athlete_stats])
    print(f"Топіки успішно видалено.")
except Exception as e:
    print(f"Помилка при видаленні топіку: {e}")


# Створення нових топіків
try:
    admin_client.create_topics(
        new_topics=[athlete_event_results, aggregated_athlete_stats],
        validate_only=False,
    )
    print(f"Topic '{athlete_event_results}' created successfully.")
    print(f"Topic '{aggregated_athlete_stats}' created successfully.")
except Exception as e:
    print(f"An error occurred: {e}")

# Перевіряємо список існуючих топіків
for topic in admin_client.list_topics():
    if my_name in topic:
        print(f"Topic '{topic}' already exists.")

# Закриття зв'язку з клієнтом
admin_client.close()
