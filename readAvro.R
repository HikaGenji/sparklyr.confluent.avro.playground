library(sparklyr)
library(dplyr)
library(stringr)

key_schema_str <- '
{
   "namespace": "indicator",
   "name": "key",
   "type": "record",
   "fields" : [
     {
       "name" : "id",
       "type" : "string",
       "default" : ""
     }
   ]
}
'

value_schema_str <- '
{
   "namespace": "indicator",
   "name": "value",
   "type" : "record",
    "fields" : [{
        "name" : "timestamp",
        "type" : {
            "type" : "long",
            "logicalType" : "timestamp-millis"
        },
        "default" : -1
    },
	{
        "name" : "side",
        "type" : "int",
        "default" : 1
    },
	{
        "name" : "id",
        "type" : "string",
        "default" : ""
    }
	]
}
'
config <- spark_config()
config$sparklyr.shell.packages <- "org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.0-preview,org.apache.spark:spark-avro_2.12:3.0.0-preview"
config$sparklyr.log.invoke <- "cat"

sc <- spark_connect("local", spark_home = "~/spark/spark-3.0.0-preview-bin-hadoop3.2", version="3.0.0-preview", config=config)

read_options <- list(kafka.bootstrap.servers = "localhost:29092",
                     subscribe = "parameter", startingOffsets="earliest")

stream_read_kafka(sc, options = read_options) %>%
spark_dataframe() %>%
stream_write_memory(name="parameter")

# invoke style
expr <- str_interp("${value_schema_str}")

p <- parameter%>%
    spark_dataframe()

c <-  invoke_static(sc, "org.apache.spark.sql.avro.functions", 
                   "from_avro",
                   invoke_static(sc, "org.apache.spark.sql.functions", "col", "value"),
                   expr)

p %>% 
invoke("withColumn", "structvalue", c) %>%
sdf_register() %>%
select("structvalue") %>%
collect()

stream_stop(stream)

# manage malformed records
m <- invoke_static(sc, "java.util.Collections", "singletonMap", "mode", "PERMISSIVE")



