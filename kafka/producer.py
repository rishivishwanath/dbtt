from kafka import KafkaProducer
import csv
import time
import json

# Kafka config
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

deliveries_topic = 'deliveries_raw'
runs_topic = 'runs'
rate_topic = 'rate'
bowler_topic = 'bowler'
csv_file_path = '../data/ipl_2022_deliveries.csv'

# Desired columns for the 'runs' topic
selected_columns = ['match_id', 'batting_team', 'innings', 'over', 'striker', 'runs_of_bat']
bowler_columns = ['match_id', 'batting_team', 'bowling_team', 'striker', 'bowler', 'wicket_type', 'player_dismissed', 'fielder']

with open(csv_file_path, 'r') as file:
    reader = csv.DictReader(file)
    for row in reader:
        # Send full row to deliveries_raw topic
        producer.send(deliveries_topic, value=row)
        print(f"Sent to {deliveries_topic}: {row}")
        
        # Extract only selected columns and send to runs topic
        filtered_row = {key: row[key] for key in selected_columns if key in row}
        producer.send(runs_topic, value=filtered_row)
        print(f"Sent to {runs_topic}: {filtered_row}")
        
        # Append 'extras' and send to rate topic
        filtered_row['extras'] = row['extras']
        producer.send(rate_topic, value=filtered_row)
        print(f"Sent to {rate_topic}: {filtered_row}")
        
        bowler_data = {key: row[key] for key in bowler_columns if key in row}
        producer.send(bowler_topic, value=bowler_data)
        print(f"Sent to {bowler_topic}: {bowler_data}")
        
        #time.sleep(0.1)  # simulate streaming

