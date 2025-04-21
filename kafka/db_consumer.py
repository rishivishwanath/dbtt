from kafka import KafkaConsumer
import json
import mysql.connector
from datetime import datetime
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class DatabaseConsumer:
    def __init__(self, host='localhost', user='root', password='', database='ipl_analytics'):
        self.db_config = {
            'host': host,
            'user': user,
            'password': password,
            'database': database
        }
        # Initialize Kafka consumers for different topics
        self.consumers = {
            'deliveries_raw': KafkaConsumer(
                'deliveries_raw',
                bootstrap_servers='localhost:9092',
                value_deserializer=lambda v: json.loads(v.decode('utf-8'))
            ),
            'runs': KafkaConsumer(
                'runs',
                bootstrap_servers='localhost:9092',
                value_deserializer=lambda v: json.loads(v.decode('utf-8'))
            ),
            'bowler': KafkaConsumer(
                'bowler',
                bootstrap_servers='localhost:9092',
                value_deserializer=lambda v: json.loads(v.decode('utf-8'))
            )
        }

    def store_delivery(self, data):
        conn = mysql.connector.connect(**self.db_config)
        cursor = conn.cursor()
        try:
            # First ensure match exists
            cursor.execute(
                "INSERT IGNORE INTO matches (match_id, season, match_no, date, venue) "
                "VALUES (%s, %s, %s, %s, %s)",
                (data['match_id'], data.get('season'), data.get('match_no'),
                 datetime.now().date(), data.get('venue'))
            )

            # Insert delivery data
            cursor.execute(
                "INSERT INTO deliveries "
                "(match_id, innings, over_num, ball_num, striker, non_striker, "
                "bowler, runs_off_bat, extras, wide_runs, noball_runs, "
                "penalty_runs, wicket_type, player_dismissed, fielder, "
                "batting_team, bowling_team) "
                "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
                (data['match_id'], data['innings'], data['over'], data['ball'],
                 data['striker'], data['non_striker'], data['bowler'],
                 data['runs_off_bat'], data.get('extras', 0), data.get('wide_runs', 0),
                 data.get('noball_runs', 0), data.get('penalty_runs', 0),
                 data.get('wicket_type'), data.get('player_dismissed'),
                 data.get('fielder'), data['batting_team'], data['bowling_team'])
            )
            conn.commit()
            logger.info(f"Stored delivery data for match {data['match_id']}")
        except Exception as e:
            logger.error(f"Error storing delivery: {str(e)}")
            conn.rollback()
        finally:
            cursor.close()
            conn.close()

    def update_player_stats(self, data, stat_type='batting'):
        conn = mysql.connector.connect(**self.db_config)
        cursor = conn.cursor()
        try:
            if stat_type == 'batting':
                cursor.execute(
                    "INSERT INTO player_stats (player_name, runs_scored) "
                    "VALUES (%s, %s) "
                    "ON DUPLICATE KEY UPDATE "
                    "runs_scored = runs_scored + VALUES(runs_scored)",
                    (data['striker'], int(data['runs_of_bat']))
                )
            elif stat_type == 'bowling' and data.get('wicket_type'):
                cursor.execute(
                    "INSERT INTO player_stats (player_name, wickets_taken) "
                    "VALUES (%s, 1) "
                    "ON DUPLICATE KEY UPDATE "
                    "wickets_taken = wickets_taken + 1",
                    (data['bowler'],)
                )
            conn.commit()
        except Exception as e:
            logger.error(f"Error updating player stats: {str(e)}")
            conn.rollback()
        finally:
            cursor.close()
            conn.close()

    def consume_and_store(self):
        try:
            while True:
                # Process deliveries_raw topic
                for message in self.consumers['deliveries_raw'].poll(timeout_ms=100).values():
                    for msg in message:
                        self.store_delivery(msg.value)

                # Process runs topic
                for message in self.consumers['runs'].poll(timeout_ms=100).values():
                    for msg in message:
                        self.update_player_stats(msg.value, 'batting')

                # Process bowler topic
                for message in self.consumers['bowler'].poll(timeout_ms=100).values():
                    for msg in message:
                        if msg.value.get('wicket_type'):
                            self.update_player_stats(msg.value, 'bowling')

        except KeyboardInterrupt:
            logger.info("Stopping consumers...")
        finally:
            for consumer in self.consumers.values():
                consumer.close()

if __name__ == "__main__":
    consumer = DatabaseConsumer()
    consumer.consume_and_store()