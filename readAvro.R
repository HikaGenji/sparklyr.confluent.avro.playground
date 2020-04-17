library(sparklyudf)
library(sparklyr)
library(dplyr)

config <- spark_config()
config$sparklyr.shell.repositories <-  "http://packages.confluent.io/maven/"
config$sparklyr.gateway.start.timeout <- 360
schemaRegistryUrl <- "http://localhost:8081"

sc <- spark_connect("spark://spark-master:7077", spark_home = "spark", config=config)

sparklyudf_register(sc, schemaRegistryUrl)

data.frame(name = "parameter") %>%
 copy_to(sc, .) %>%
 mutate(schema =getSchema(name))

invoke_static(sc, "sparklyudf.getSchema", "parameter")


read_options <- list(kafka.bootstrap.servers = "broker:9092",
                     subscribe = "parameter", startingOffsets="earliest")

stream_read_kafka(sc, options = read_options) %>%
spark_dataframe() %>%
stream_write_memory(name="parameter")

query <- "select deserialize(value) as msg from parameter"

res <- DBI::dbGetQuery(sc, statement = query)

query %>%
dbplyr::sql() %>%
tbl(sc, .)


stream_stop(stream)

