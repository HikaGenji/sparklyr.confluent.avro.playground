# sparklyr.confluent.avro.playground

vagrant/docker-compose playground to test sparklyr.confluent.avro package

- contains kafka broker, zookeeper, schema-registry, rstudio, spark master and worker, zeppelin

- rstudio docker image comes with sparklyr.confluent.avro package built-in

- python avro producer:
  docker run --rm -it --network host -v /home/vagrant:/app hikagenji/confluent-kafka-avro-python:latest python /app/avro-producer.py
  
