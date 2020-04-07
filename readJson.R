library(sparklyr)
library(dplyr)
library(stringr)

config <- spark_config()
config$sparklyr.shell.packages <- "org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.0-preview"

sc <- spark_connect("local", spark_home = "~/spark/spark-3.0.0-preview-bin-hadoop3.2", version="3.0.0-preview", config=config)

broker <- "broker:9092"

read_options <- list(kafka.bootstrap.servers = broker, subscribe = "test", startingOffsets="earliest")

df <- stream_read_kafka(sc, options = read_options)

# dplyr style
df %>% 
mutate(v=as.character(value)) %>%
select(v)

# try sql via DBI on stream
df %>%
spark_dataframe() %>%
stream_write_memory("test")

res <- DBI::dbGetQuery(sc, statement ='select CAST(value as STRING) from test')

# invoke style
"select from_json(CAST(value as STRING), 'timestamp BIGINT, id STRING, side INT') as value from test" %>%
dbplyr::sql() %>%
tbl(sc, .) %>%
mutate(timestamp=value.timestamp, id=value.id, side=value.side) %>%
select(-value) %>%
group_by(id) %>%
summarise(n=count()) 

