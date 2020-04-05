#!/usr/bin/python
# docker run -v /home/vagrant:/app hikagenji/confluent-kafka-avro-python:latest python app/avro-producer.py
# docker run -it -v /home/vagrant:/app hikagenji/confluent-kafka-avro-python:latest python


import random
import json
import threading
import time
import sys
from datetime import datetime
from confluent_kafka import avro
from confluent_kafka.avro import AvroProducer

key_schema_str = """
{
   "namespace": "indicator",
   "name": "key",
   "type": "record",
   "fields" : [
     {
       "name" : "id",
       "type" : "string",
       "default" : ""
     }
   ]
}
"""
key_schema = avro.loads(key_schema_str)

value_schema_str = """
{
   "namespace": "indicator",
   "name": "value",
   "type" : "record",
    "fields" : [{
        "name" : "timestamp",
        "type" : {
            "type" : "long",
            "logicalType" : "timestamp-millis"
        },
        "default" : -1
    },
	{
        "name" : "side",
        "type" : "int",
        "default" : 1
    },
	{
        "name" : "id",
        "type" : "string",
        "default" : ""
    }
	]
}
"""
value_schema = avro.loads(value_schema_str)

p = AvroProducer({
    'bootstrap.servers': 'localhost:29092',
        'queue.buffering.max.messages': 1000000, 
        'queue.buffering.max.ms': 1, 
        'log.connection.close': False,
    'schema.registry.url': 'http://localhost:8081'
    }, default_key_schema=key_schema,
       default_value_schema=value_schema)

def createOrders(n):
  return ["OD-" + str(i) for i in range(1, n)]

def delivery_callback(err, msg):
        if err:
            sys.stderr.write('%% Message failed delivery: %s\n' % err)
        else:
            sys.stderr.write('%% Message delivered to %s [%d] @ %d\n' %
                             (msg.topic(), msg.partition(), msg.offset()))

r = [p.produce(topic="parameter", key={"id": o}, 
     value={"timestamp": int(round(time.time() * 1000)), "id": o, "side": 1}, 
     callback=delivery_callback) for o in createOrders(10)]
     
p.flush()
  