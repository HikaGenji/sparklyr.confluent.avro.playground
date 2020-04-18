library(sparklyudf)
library(sparklyr)
library(dplyr)

config <- spark_config()
config$sparklyr.shell.packages <- "org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.5"
# schemaRegistryUrl <- "http://localhost:8081"

sc <- spark_connect("spark://spark-master:7077", spark_home = "spark", config=config)

sparklyudf_register(sc)

data.frame(name = "parameter") %>%
 copy_to(sc, .) %>%
 mutate(schema =getSchema(name))

read_options <- list(kafka.bootstrap.servers = "broker:9092",
                     subscribe = "parameter", startingOffsets="earliest")

stream_read_kafka(sc, options = read_options) %>%
spark_dataframe() %>%
stream_write_memory(name="parameter")

query <- "select deserialize(value, 'parameter') as msg from parameter"

# eager sql style
res <- DBI::dbGetQuery(sc, statement = query)

# lazy sql style
query %>%
dbplyr::sql() %>%
tbl(sc, .)


stream_stop(stream)

