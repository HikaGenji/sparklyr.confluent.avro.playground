library(sparklyr.confluent.avro)
library(sparklyr)
library(dplyr)

config <- spark_config()
config$sparklyr.shell.repositories <- "http://packages.confluent.io/maven/"
config$spark.executor.cores <- 4
kafkaUrl <- "broker:9092"
schemaRegistryUrl <- "http://schema-registry:8081"
sc <- spark_connect(master = "spark://spark-master:7077", spark_home = "spark", config=config, app_name="parameter")

stream_read_kafka_avro(sc, kafka.bootstrap.servers=kafkaUrl, schema.registry.topic="parameter", startingOffsets="latest", schema.registry.url=schemaRegistryUrl) %>%
stream_write_kafka_avro(kafka.bootstrap.servers=kafkaUrl, schema.registry.topic="output", schema.registry.url=schemaRegistryUrl, keyCols="id", mode="update") 
s <- stream_read_kafka_avro(sc, kafka.bootstrap.servers=kafkaUrl, schema.registry.topic="output", startingOffsets="latest", schema.registry.url=schemaRegistryUrl, name="output")

"select side, id from parameter" %>%
dbplyr::sql() %>%
tbl(sc, .) %>%
stream_watermark() %>%
group_by(id, time=window(timestamp, "5 minutes", "30 seconds"))%>%
summarise(n_new=n()) %>%
sdf_separate_column("time", into=c("start", "end")) %>%
select(-time) %>%
stream_write_kafka_avro(kafka.bootstrap.servers=kafkaUrl, schema.registry.topic="aggregate", schema.registry.url=schemaRegistryUrl, keyCols="id", mode="update") 

sc2 <- spark_connect(master = "spark://spark-master:7077", spark_home = "spark", config=config, app_name="aggreg")
s <- stream_read_kafka_avro(sc2, kafka.bootstrap.servers=kafkaUrl, schema.registry.topic="aggregate",
                                startingOffsets="latest", schema.registry.url=schemaRegistryUrl, name="agg") 
								
sc3 <- spark_connect(master = "local[*]", spark_home = "spark", config=config, app_name="aggreg3")
s <- stream_read_kafka_avro(sc3, kafka.bootstrap.servers=kafkaUrl, schema.registry.topic="aggregate",
                                startingOffsets="latest", schema.registry.url=schemaRegistryUrl, name="agg2") 
							








