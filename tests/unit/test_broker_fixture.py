"""The suite's broker. apache/kafka-native, single node, KRaft -- see spec 9.3."""

from confluent_kafka.admin import AdminClient


def test_the_topic_exists_with_three_partitions(broker: str, topic: str) -> None:
    metadata = AdminClient({"bootstrap.servers": broker}).list_topics(timeout=10)
    assert topic in metadata.topics
    assert len(metadata.topics[topic].partitions) == 3
