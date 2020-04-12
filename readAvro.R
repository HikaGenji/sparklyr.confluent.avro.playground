library(sparklyr)
library(dplyr)
library(stringr)
library(sparkavroudf)

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
config$sparklyr.shell.packages <- "org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.1,org.apache.spark:spark-avro_2.11:2.4.1"
config$sparklyr.log.invoke <- "cat"

sc <- spark_connect("spark://spark-master:7077", spark_home = "spark/spark-2.4.1-bin-hadoop2.7", config=config)

read_options <- list(kafka.bootstrap.servers = "localhost:29092",
                     subscribe = "parameter", startingOffsets="earliest")

stream_read_kafka(sc, options = read_options) %>%
spark_dataframe() %>%
stream_write_memory(name="parameter")

# invoke style
expr <- str_interp("${value_schema_str}")

p <- parameter%>%
    spark_dataframe()


stream_stop(stream)

