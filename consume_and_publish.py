#!/usr/bin/env python

import pika
import pprint
import requests
from config import RMQ_PORT, RMQ_SERVER, RMQ_QUEUE, API_SERVER, EVENT_STREAM_NAME, API_KEY, CLIENT_ID

# Start with the connection to local queue
local_connection = pika.BlockingConnection(pika.ConnectionParameters(RMQ_SERVER))

# Create a channel for rabbit to send data over
local_channel = local_connection.channel()

# Set the queue and durability flag
local_channel.queue_declare(queue=RMQ_QUEUE, durable=True)

# Setup a session and its authentication
amp_session = requests.Session()
amp_session.auth = (CLIENT_ID, API_KEY)

# Get a list of all event streams on the server
event_streams = amp_session.get(API_SERVER).json()['data']

# Declare a dictionary to store the event stream ID
event_stream = {}

# Iterate though the available event_streams, identify the event stream that matches EVENT_STREAM_NAME
# Store the event stream data in event_stream
for k in event_streams:
    if k['name'] == EVENT_STREAM_NAME:
        event_stream = k


# Build the URL that our listener will monitor on the AMP cloud and store it as queue_url
cloud_queue_url = pika.URLParameters('amqps://{user_name}:{password}@{host}:{port}'
                                     .format(**event_stream['amqp_credentials']))
pprint.pprint(event_stream['amqp_credentials']['password'])


# Configure the connection by passing it the queue URL
cloud_connection = pika.BlockingConnection(cloud_queue_url)

# Open a channel using the connection
cloud_channel = cloud_connection.channel()

def on_message(channel, method_frame, header_frame, body):
       local_channel.basic_publish(exchange='',
                                   routing_key=RMQ_QUEUE,
                                   body=body,
                                   properties=pika.BasicProperties(
                                       delivery_mode=2,  # make message persistent
                                   ))

cloud_channel.basic_consume(on_message, event_stream['amqp_credentials']['queue_name'], no_ack=True)
try:
    cloud_channel.start_consuming()
except KeyboardInterrupt:
    cloud_channel.stop_consuming()
cloud_connection.close()


print(" [*] Connecting to:\t%r" % cloud_queue_url)
print(" [*] Waiting for messages. To exit press CTRL+C")
cloud_channel.start_consuming()
