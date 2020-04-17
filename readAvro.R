library(sparklyudf)
library(sparklyr)

config <- spark_config()
config$sparklyr.shell.repositories <- "http://packages.confluent.io/maven/"
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

invoke_static(sc, "sparklyudf.getSchema", "fromAvro", .,
                  invoke_new(sc, "org.apache.spark.sql.Column", "value"),
				  value_schema_str )
 


# invoke style
expr <- str_interp("${value_schema_str}")

p <- parameter%>%
    spark_dataframe()


stream_stop(stream)

