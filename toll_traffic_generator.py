import random
import time
from datetime import datetime
from kafka import KafkaProducer

# Initialize Kafka Producer
producer = KafkaProducer(bootstrap_servers='localhost:9092')
TOPIC = 'toll'

VEHICLE_TYPES = ['Car', 'Truck', 'Van', 'Bus', 'SUV']

print(f"Starting traffic simulation... Sending events to topic '{TOPIC}'")

try:
    while True:
        now = datetime.now().strftime("%a %b %d %H:%M:%S %Y")
        vehicle_id = random.randint(10000, 99999)
        vehicle_type = random.choice(VEHICLE_TYPES)
        plaza_id = random.randint(100, 110)
        
        # Message Format: Timestamp, Vehicle_ID, Vehicle_Type, Toll_Plaza_ID
        message = f"{now},{vehicle_id},{vehicle_type},{plaza_id}"
        
        # Send message encoded as UTF-8
        producer.send(TOPIC, message.encode('utf-8'))
        print(f"Streamed event: {message}")
        
        # Stream data every 1-2 seconds
        time.sleep(random.uniform(1.0, 2.0))

except KeyboardInterrupt:
    print("\nSimulation stopped.")
finally:
    producer.flush()
    producer.close()