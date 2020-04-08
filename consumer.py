#!/usr/bin/python
# docker run -it --network host hikagenji/confluent-kafka-avro-python:latest python

import json
import time
import sys
from datetime import datetime
from confluent_kafka import Consumer

c = Consumer({
    'bootstrap.servers': 'localhost:29092',
    'group.id': 'testconsumer',
    'auto.offset.reset': 'earliest'
})

c.subscribe(['test'])

while True:
    msg = c.poll(1.0)

    if msg is None:
        continue
    if msg.error():
        print("Consumer error: {}".format(msg.error()))
        continue

    print('Received message: {}'.format(msg.value()))

c.close()