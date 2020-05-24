library(sparklyr.confluent.avro)
library(sparklyr)
library(dplyr)

config <- spark_config()
config$sparklyr.shell.repositories <- "http://packages.confluent.io/maven/"
kafkaUrl <- "broker:9092"
schemaRegistryUrl <- "http://schema-registry:8081"
sc <- spark_connect(master = "spark://spark-master:7077", spark_home = "spark", config=config)

stream_read_kafka_avro(sc, kafka.bootstrap.servers=kafkaUrl, schema.registry.topic="parameter", startingOffsets="latest", schema.registry.url=schemaRegistryUrl) %>%
stream_write_kafka_avro(kafka.bootstrap.servers=kafkaUrl, schema.registry.topic="output", schema.registry.url=schemaRegistryUrl, keyCols="id", mode="update") 
s <- stream_read_kafka_avro(sc, kafka.bootstrap.servers=kafkaUrl, schema.registry.topic="output", startingOffsets="latest", schema.registry.url=schemaRegistryUrl, name="output")

# ok so this works

"select side, id from parameter" %>%
dbplyr::sql() %>%
tbl(sc, .) %>%
stream_watermark() %>%
group_by(id, time=window(timestamp, "5 minutes", "30 seconds"))%>%
summarise(n_new=n()) %>%
sdf_separate_column("time", into=c("start", "end")) %>%
select(-time) %>%
stream_write_kafka_avro(kafka.bootstrap.servers=kafkaUrl, schema.registry.topic="abcd", schema.registry.url=schemaRegistryUrl, keyCols="id", mode="update") 

# bug in stream_read_kafka: it stop the write whenever read is triggered

# ok this one works
s <- stream_read_kafka(sc, options=list(kafka.bootstrap.servers=kafkaUrl, subscribe="abcd"))

# but this one does not: as soon as we try to print s it stops the query

s <- stream_read_kafka_avro(sc, kafka.bootstrap.servers=kafkaUrl, schema.registry.topic="abcd",
                                startingOffsets="latest", schema.registry.url=schemaRegistryUrl, name="test") 








