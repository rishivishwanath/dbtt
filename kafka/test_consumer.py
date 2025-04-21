from kafka import KafkaConsumer
import json

# Kafka config
consumer = KafkaConsumer(
    'runs',  # Subscribe to the topic
    bootstrap_servers='localhost:9092',
    value_deserializer=lambda v: json.loads(v.decode('utf-8'))  # Deserialize message
)

# Consume and print messages
try:
    for message in consumer:
        # Print each message
        print(json.dumps(message.value, indent=4))
except KeyboardInterrupt:
    print("Consumption stopped.")
finally:
    consumer.close()

