library(sparklyr.confluent.avro)
library(sparklyr)
library(dplyr)

config <- spark_config()
config$sparklyr.shell.repositories <- "http://packages.confluent.io/maven/"
kafkaUrl <- "broker:9092"
schemaRegistryUrl <- "http://schema-registry:8081"
sc <- spark_connect(master = "spark://spark-master:7077", spark_home = "spark", config=config)

stream_read_kafka(sc, options=list(kafka.bootstrap.servers=kafkaUrl, subscribe="parameter")) %>%
sdf_register("parameter")

# ok so this works

"select value from parameter" %>%
dbplyr::sql() %>%
tbl(sc, .) %>%
mutate(value=as.character(value)) %>%
stream_watermark() %>%
group_by(time=window(timestamp, "5 minutes", "30 seconds"))%>%
summarise(n_new=n()) %>%
sdf_separate_column("time", into=c("start", "end")) %>%
select(-time) %>%
mutate(value=as.character(n_new))%>%
stream_write_kafka(options=list(kafka.bootstrap.servers=kafkaUrl, topic="aggregate")) 

# bug in stream_read_kafka: it stop the write whenever read is triggered

# ok this one works
s <- stream_read_kafka(sc, options=list(kafka.bootstrap.servers=kafkaUrl, subscribe="aggregate"))









