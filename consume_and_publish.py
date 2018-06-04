#!/usr/bin/env python

import pika
import requests
from config import RMQ_PORT, RMQ_SERVER, RMQ_QUEUE, API_SERVER, EVENT_STREAM_NAME, API_KEY, CLIENT_ID

# Start with the connection to local queue
local_connection = pika.BlockingConnection(pika.ConnectionParameters(RMQ_SERVER))

# Create a channel for rabbit to send data over
local_channel = local_connection.channel()

# Set the queue and durability flag
local_channel.queue_declare(queue=RMQ_QUEUE, durable=True)

# Setup a session and its authentication
session = requests.Session()
session.auth = (CLIENT_ID, API_KEY)

# Get a list of all event streams on the server
event_streams = session.get(API_SERVER).json()['data']

# Declare a dictionary to the event stream ID
event_stream = {}

# Iterate though the available event_streams, identify the event stream that matches EVENT_STREAM_NAME
# Store the event stream data in e
for e in event_streams:
	if e['name'] is EVENT_STREAM_NAME:
		event_stream = e

# Build the URL that our listener will monitor on the AMP cloud and store it as queue_url
cloud_queue_url = pika.URLParameters('amqps://{user_name}:{password}@{host}:{port}'.format(**e['amqp_credentials']))

# Configure the connection by passing it the queue URL
cloud_connection = pika.BlockingConnection(cloud_queue_url)

# Open a channel using the connection
cloud_channel = cloud_connection.channel()

def callback(ch, method, properties, body):
    print(" [x] Received meth:\t%r" % method)
    print(" [x] Received prop:\t%r" % properties)
    print(" [x] Received body:\t%r" % body)


cloud_channel.basic_consume(callback, e, no_ack=True)

print(" [*] Connecting to:\t%r" % cloud_queue_url)
print(" [*] Waiting for messages. To exit press CTRL+C")
cloud_channel.start_consuming()





