from confluent_kafka import Consumer, KafkaError

# Configuration of the alerts notifier consumer
conf = {
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'alerts-notifier',
    'auto.offset.reset': 'earliest',
    'enable.auto.commit': False,  # Requirement: Manual offset commit
    'partition.assignment.strategy': 'cooperative-sticky'  # Requirement: Cooperative-sticky assignment
}

# Creation of the consumer instance
consumer = Consumer(conf)

# Subscribe to your alerts
topic = 'alerts'
consumer.subscribe([topic])

try:
    while True:

        msg = consumer.poll(1.0) # Poll for messages (timeout of 1 second)

        if msg is None: continue
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                continue # End of partition event
            else:
                print(f"Error: {msg.error()}")
                break

        # Process the alert
        print(f"Received alert from partition {msg.partition()} "
              f"at offset {msg.offset()}: {msg.value().decode('utf-8')}")

        # Simulate handling/processing the alert

        # Manual commit
        try:
            # Commit the offset synchronously after handling
            consumer.commit(msg, asynchronous=False)
            print(f"-> Offset committed for partition {msg.partition()} at offset {msg.offset()}")
        except Exception as e:
            print(f"Failed to commit offset: {e}")

except KeyboardInterrupt: pass
finally:
    # Close down consumer to commit final offsets and disconnect cleanly
    consumer.close()