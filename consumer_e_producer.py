from kafka import KafkaConsumer, KafkaProducer
from const import *
import sys

try:
    topic_in = sys.argv[1]
    topic_out = sys.argv[2]
except:
    print('Usage: python3 consumer_producer.py <topic_in> <topic_out>')
    exit(1)

# Create consumer: Option 1 -- only consume new events
consumer = KafkaConsumer(bootstrap_servers=[BROKER_ADDR + ':' + BROKER_PORT])

# Create consumer: Option 2 -- consume old events (uncomment to test -- and comment Option 1 above)
#consumer = KafkaConsumer(bootstrap_servers=[BROKER_ADDR + ':' + BROKER_PORT], auto_offset_reset='earliest')

producer = KafkaProducer(bootstrap_servers=[BROKER_ADDR + ':' + BROKER_PORT])

consumer.subscribe([topic_in])

for msg in consumer:
    event = msg.value.decode()
    print('Received: ' + event)

    new_event = 'Event from ' + topic_in + ': ' + event
    producer.send(topic_out, value=new_event.encode())
    producer.flush()
    print('Forwarded to ' + topic_out)