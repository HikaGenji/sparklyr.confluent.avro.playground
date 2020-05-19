library(sparklyr.confluent.avro)
library(sparklyr)
library(dplyr)

config <- spark_config()
config$sparklyr.shell.repositories <- "http://packages.confluent.io/maven/"
kafkaUrl <- "broker:9092"
schemaRegistryUrl <- "http://schema-registry:8081"
sc <- spark_connect(master = "spark://spark-master:7077", spark_home = "spark", config=config)

stream_read_kafka_avro(sc, kafka.bootstrap.servers=kafkaUrl, schema.registry.topic="parameter", startingOffsets="earliest", schema.registry.url=schemaRegistryUrl)

"select side, id from parameter" %>%
dbplyr::sql() %>%
tbl(sc, .) %>%
stream_watermark() %>%
group_by(time=window(timestamp, "30 seconds", "5 seconds"))%>%
sdf_separate_column("time", into=c("start", "end")) %>%
select(-time) %>%
stream_write_kafka_avro(kafka.bootstrap.servers=kafkaUrl, schema.registry.topic="aggregate", schema.registry.url=schemaRegistryUrl) 

# sql style 'eager' returns an R dataframe
query <- 'select * aggregate'
res   <- DBI::dbGetQuery(sc, statement =query)

stream_read_kafka_avro(sc, kafka.bootstrap.servers=kafkaUrl, schema.registry.topic="aggregate", startingOffsets="earliest", schema.registry.url=schemaRegistryUrl) 











