"""Small Kafka helpers shared by the pipeline tasks: publish with confirmation, publish only once."""
from __future__ import annotations

import os
from typing import Callable

BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")


def publish(topic: str, key: str, value: str) -> None:
    """Publish one message and raise if Kafka does not confirm it."""
    from confluent_kafka import Producer

    errors = []

    def on_delivery(err, _msg):
        if err is not None:
            errors.append(str(err))

    producer = Producer({"bootstrap.servers": BOOTSTRAP_SERVERS, "enable.idempotence": True})
    producer.produce(topic, key=key, value=value, on_delivery=on_delivery)
    remaining = producer.flush(30)
    if remaining or errors:
        raise RuntimeError(f"Publishing to {topic} failed: {errors or 'timeout'}")


def already_published(topic: str, matches: Callable[[str | None, str], bool]) -> bool:
    """True if a message already stored in `topic` satisfies matches(key, value).

    Reads every partition from the beginning up to its current end. No consumer group is joined
    and no offset is committed, so this check has no side effect.
    """
    from confluent_kafka import OFFSET_BEGINNING, Consumer, TopicPartition

    consumer = Consumer({
        "bootstrap.servers": BOOTSTRAP_SERVERS,
        "group.id": "idempotence-check",  # required by the client, but never used (assign, no commit)
        "enable.auto.commit": False,
    })
    try:
        partitions = list(consumer.list_topics(topic, timeout=10).topics[topic].partitions)
        last_offset = {}
        for p in partitions:
            low, high = consumer.get_watermark_offsets(TopicPartition(topic, p), timeout=10)
            if high > low:
                last_offset[p] = high - 1
        if not last_offset:
            return False
        consumer.assign([TopicPartition(topic, p, OFFSET_BEGINNING) for p in last_offset])
        while last_offset:
            msg = consumer.poll(5.0)
            if msg is None:
                break
            if msg.error():
                raise RuntimeError(f"Cannot read {topic}: {msg.error()}")
            key = msg.key().decode("utf-8", "replace") if msg.key() is not None else None
            value = msg.value().decode("utf-8", "replace") if msg.value() is not None else ""
            if matches(key, value):
                return True
            if msg.offset() >= last_offset.get(msg.partition(), -1):
                last_offset.pop(msg.partition(), None)
        return False
    finally:
        consumer.close()


def publish_once(topic: str, key: str, value: str,
                 matches: Callable[[str | None, str], bool]) -> bool:
    """Publish unless a matching message is already in the topic. Returns True if published."""
    if already_published(topic, matches):
        print(f"Skipped: a matching message is already in {topic} (key={key})")
        return False
    publish(topic, key, value)
    print(f"Published to {topic} (key={key})")
    return True
