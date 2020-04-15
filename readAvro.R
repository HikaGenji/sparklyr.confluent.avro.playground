library(sparklyr)
library(dplyr)
library(stringr)
library(sparkavroudf)

# this cannot work as confluent avro contains more than just payload
# solution is to implement confluent avro deserializer in a scala udf
# https://github.com/javierluraschi/sparklyudf
# https://stackoverflow.com/questions/48882723/integrating-spark-structured-streaming-with-the-confluent-schema-registry
# https://blog.engineering.publicissapient.fr/2018/10/25/spark-structured-streaming-avec-kafka-schema-registry/

Sys.setenv(JAVA_HOME = "/usr/lib/jvm/java-8-oracle")

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
config$sparklyr.shell.packages <- "org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.4"
config$sparklyr.log.invoke <- "cat"

sc <- spark_connect("spark://spark-master:7077", spark_home = "spark", config=config)

# test to_avro

invoke_static(sc, "sparkavroudf.AvroUtils", "toAvro", spark_dataframe(sdf_len(sc, 3)), invoke_new(sc, "org.apache.spark.sql.Column", "id")) %>% sdf_register()

read_options <- list(kafka.bootstrap.servers = "broker:9092",
                     subscribe = "parameter", startingOffsets="earliest")

stream_read_kafka(sc, options = read_options) %>%
spark_dataframe() %>%
stream_write_memory(name="parameter")

invoke_static(sc, "sparkavroudf.AvroUtils", "fromAvro", .,
                  invoke_new(sc, "org.apache.spark.sql.Column", "value"),
				  value_schema_str )
 


# invoke style
expr <- str_interp("${value_schema_str}")

p <- parameter%>%
    spark_dataframe()


stream_stop(stream)

