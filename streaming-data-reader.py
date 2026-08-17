from datetime import datetime
from kafka import KafkaConsumer
import mysql.connector

TOPIC = 'toll'
DATABASE = 'tolldata'
USERNAME = 'root'
PASSWORD = 'YOUR_MYSQL_PASSWORD'  # Replace with your actual MySQL password

print("Connecting to MySQL Database...")
try:
    db_connection = mysql.connector.connect(
        host='127.0.0.1',
        port=3306,
        user=USERNAME,
        password=PASSWORD,
        database=DATABASE
    )
    cursor = db_connection.cursor()
    print("Successfully connected to MySQL database.")
except Exception as e:
    print(f"Database connection failed: {e}")
    exit(1)

print("Connecting to Kafka Consumer...")
consumer = KafkaConsumer(
    TOPIC,
    bootstrap_servers=['localhost:9092'],
    auto_offset_reset='latest'
)
print(f"Listening for messages on topic: {TOPIC}...\n")

for msg in consumer:
    try:
        # Decode UTF-8 string payload
        message = msg.value.decode("utf-8")
        
        # Parse payload variables
        raw_timestamp, vehicle_id, vehicle_type, plaza_id = message.split(",")
        
        # Convert timestamp format from "Day Mon DD HH:MM:SS YYYY" to "YYYY-MM-DD HH:MM:SS"
        date_obj = datetime.strptime(raw_timestamp, '%a %b %d %H:%M:%S %Y')
        formatted_timestamp = date_obj.strftime("%Y-%m-%d %H:%M:%S")
        
        # Insert record into database table
        sql = "INSERT INTO livetolldata (timestamp, vehicle_id, vehicle_type, toll_plaza_id) VALUES (%s, %s, %s, %s)"
        values = (formatted_timestamp, int(vehicle_id), vehicle_type, int(plaza_id))
        
        cursor.execute(sql, values)
        db_connection.commit()
        
        print(f"Loaded record into MySQL -> Timestamp: {formatted_timestamp} | Vehicle ID: {vehicle_id}")

    except Exception as err:
        print(f"Error processing record: {err}")