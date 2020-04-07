library(sparklyr)
library(dplyr)
library(stringr)

config <- spark_config()
config$sparklyr.shell.packages <- "org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.0-preview"

sc <- spark_connect("local", spark_home = "~/spark/spark-3.0.0-preview-bin-hadoop3.2", version="3.0.0-preview", config=config)

broker <- "broker:9092"

read_options <- list(kafka.bootstrap.servers = broker, subscribe = "test", startingOffsets="earliest")

stream_read_kafka(sc, options = read_options) %>%
spark_dataframe() %>%
stream_write_memory("test")

# try sql via DBI
res <- DBI::dbGetQuery(sc, statement ='select CAST(value as STRING) from test')
