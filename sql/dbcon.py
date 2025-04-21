from kafka import KafkaConsumer
import mysql.connector
import json

# 1. Connect to MySQL
try:
    db = mysql.connector.connect(
        host='localhost',
        user='dbt_user',   # your MySQL username
        password='dbt_password',   # your MySQL password
        database='ipl_analytics'
    )
    cursor = db.cursor()
    print("✅ Connected to MySQL database.")
except mysql.connector.Error as err:
    print(f"❌ Error connecting to MySQL: {err}")
    exit(1)

# 2. Create tables if not exist
cursor.execute('''
CREATE TABLE IF NOT EXISTS runs (
    id INT AUTO_INCREMENT PRIMARY KEY,
    match_id VARCHAR(20),
    batting_team VARCHAR(100),
    innings INT,
    `over` FLOAT,
    striker VARCHAR(50),
    runs_of_bat INT
);
''')

cursor.execute('''
CREATE TABLE IF NOT EXISTS rate (
    id INT AUTO_INCREMENT PRIMARY KEY,
    match_id VARCHAR(20),
    batting_team VARCHAR(100),
    innings INT,
    `over` FLOAT,
    striker VARCHAR(50),
    runs_of_bat INT,
    extras INT
);
''')

cursor.execute('''
CREATE TABLE IF NOT EXISTS bowler (
    id INT AUTO_INCREMENT PRIMARY KEY,
    match_id VARCHAR(20),
    batting_team VARCHAR(100),
    bowling_team VARCHAR(100),
    striker VARCHAR(50),
    bowler VARCHAR(50),
    wicket_type VARCHAR(50),
    player_dismissed VARCHAR(50),
    fielder VARCHAR(50)
);
''')
db.commit()

# 3. Kafka Consumer setup
consumer = KafkaConsumer(
    'runs', 'rate', 'bowler',   # Only these three topics
    bootstrap_servers='localhost:9092',
    value_deserializer=lambda m: json.loads(m.decode('utf-8'))
)

# 4. Start listening
print("✅ Connected to Kafka. Listening for messages on runs, rate, bowler...")

for message in consumer:
    topic = message.topic
    data = message.value

    if topic == 'runs':
        # Validate the presence of key fields
        if all(key in data for key in ['match_id', 'batting_team', 'innings', 'over', 'striker', 'runs_of_bat']):
            sql = '''
            INSERT INTO runs (match_id, batting_team, innings, `over`, striker, runs_of_bat)
            VALUES (%s, %s, %s, %s, %s, %s)
            '''
            values = (
                data.get('match_id'), data.get('batting_team'),
                int(data.get('innings', 0)), float(data.get('over', 0.0)),
                data.get('striker'), int(data.get('runs_of_bat', 0))
            )
        else:
            print("❌ Missing data in runs topic:", data)
            continue
    elif topic == 'rate':
        if all(key in data for key in ['match_id', 'batting_team', 'innings', 'over', 'striker', 'runs_of_bat', 'extras']):
            sql = '''
            INSERT INTO rate (match_id, batting_team, innings, `over`, striker, runs_of_bat, extras)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            '''
            values = (
                data.get('match_id'), data.get('batting_team'),
                int(data.get('innings', 0)), float(data.get('over', 0.0)),
                data.get('striker'), int(data.get('runs_of_bat', 0)), int(data.get('extras', 0))
            )
        else:
            print("❌ Missing data in rate topic:", data)
            continue
    elif topic == 'bowler':
        if all(key in data for key in ['match_id', 'batting_team', 'bowling_team', 'striker', 'bowler', 'wicket_type', 'player_dismissed', 'fielder']):
            sql = '''
            INSERT INTO bowler (match_id, batting_team, bowling_team, striker, bowler, wicket_type, player_dismissed, fielder)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            '''
            values = (
                data.get('match_id'), data.get('batting_team'), data.get('bowling_team'),
                data.get('striker'), data.get('bowler'), data.get('wicket_type'),
                data.get('player_dismissed'), data.get('fielder')
            )
        else:
            print("❌ Missing data in bowler topic:", data)
            continue
    else:
        print("❌ Unexpected topic:", topic)
        continue

    # Insert into the database
    try:
        cursor.execute(sql, values)
        db.commit()
        print(f"✅ Inserted into {topic}: {data}")
    except mysql.connector.Error as e:
        print(f"❌ Error inserting into {topic}: {e}")

