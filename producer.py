#!/usr/bin/python
# docker run -it --network host hikagenji/confluent-kafka-avro-python:latest python

import json
import time
import sys
from datetime import datetime
from confluent_kafka import Producer

p = Producer({'bootstrap.servers': 'localhost:29092'})

def delivery_callback(err, msg):
        if err:
            sys.stderr.write('%% Message failed delivery: %s\n' % err)
        else:
            sys.stderr.write('%% Message delivered to %s [%d] @ %d\n' %
                             (msg.topic(), msg.partition(), msg.offset()))

for i in range(10):
  p.produce("test", json.dumps({"timestamp": int(round(time.time() * 1000)), "id": 'OD' + str(i), "side": 1}), callback=delivery_callback)
  p.poll(0)
     
p.flush()

# test
# docker exec -it broker bash
# /usr/bin/kafka-console-consumer --bootstrap-server localhost:29092 --topic order --from-beginning
  