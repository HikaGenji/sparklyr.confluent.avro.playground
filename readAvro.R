library(sparklyudf)
library(sparklyr)
library(dplyr)

config <- spark_config()
config$sparklyr.shell.packages <- "io.confluent:kafka-avro-serializer:5.4.1,io.confluent:kafka-schema-registry:5.4.1,org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.5,org.apache.spark:spark-avro_2.11:2.4.5,za.co.absa:abris_2.11:3.1.1"
config$sparklyr.shell.repositories <- "http://packages.confluent.io/maven/"

sc <- spark_connect(master = "local", spark_home = "spark", config=config)

df <- invoke_static(sc, "sparklyudf.Reader", "stream", "parameter")

df %>%
spark_dataframe() %>%
stream_write_memory("parameter")

# sql style 'eager'
query <- 'select data.timestamp from parameter'
res   <- DBI::dbGetQuery(sc, statement =query)

# dbplyr style 'lazy'
queery %>%
dbplyr::sql() %>%
tbl(sc, .)
