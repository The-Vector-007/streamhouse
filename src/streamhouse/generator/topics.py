import os

from confluent_kafka import KafkaError, KafkaException
from confluent_kafka.admin import AdminClient, NewTopic

# Configurations
BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")

# Topic schema: (topic_name, num_partitions, replication_factor)
TOPIC_CONFIGS: list[tuple[str, int, int]] = [
    ("txn.raw", 6, 1),
    ("txn.dlq", 1, 1),
]


def create_admin_client(bootstrap_servers: str = BOOTSTRAP_SERVERS) -> AdminClient:
    return AdminClient({"bootstrap.servers": bootstrap_servers})


def print_partition_count(admin: AdminClient, topic_name: str) -> None:
    """Describes the topic and prints its current partition count."""
    metadata = admin.list_topics(topic=topic_name, timeout=10)
    topic_metadata = metadata.topics.get(topic_name)

    if topic_metadata and topic_metadata.error is None:
        partition_count = len(topic_metadata.partitions)
        print(f"Topic '{topic_name}' currently has {partition_count} partition(s).")
    else:
        print(f"Failed to fetch metadata for '{topic_name}'.")


def create_kafka_topics(
    admin: AdminClient,
    topic_definitions: list[tuple[str, int, int]],
    retention_ms: int = 604800000,
) -> None:
    # Build topic list cleanly using list comprehension
    kafka_topics = [
        NewTopic(
            topic=name,
            num_partitions=partitions,
            replication_factor=rf,
            config={"retention.ms": str(retention_ms)},
        )
        for name, partitions, rf in topic_definitions
    ]

    # Batch create topics
    futures = admin.create_topics(kafka_topics)

    for topic, future in futures.items():
        try:
            future.result()
            print(f"Topic '{topic}' created successfully.")
        except KafkaException as e:
            err = e.args[0]
            # Idempotent check: Treat existing topics gracefully
            if err.code() == KafkaError.TOPIC_ALREADY_EXISTS:
                print(f"Topic '{topic}' already exists.")
            else:
                print(f"Failed to create topic '{topic}': {e}")

    # Inspect topic partition details
    for name, _, _ in topic_definitions:
        print_partition_count(admin, name)


if __name__ == "__main__":
    client = create_admin_client()
    create_kafka_topics(client, TOPIC_CONFIGS)
