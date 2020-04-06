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
config[["sparklyr.shell.packages"]] <- c("org.apache.spark:spark-avro_2.12:3.0.0-preview", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.0-preview")

sc <- spark_connect("local", spark_home = "~/spark/spark-3.0.0-preview-bin-hadoop3.2", version="3.0.0-preview", config=config)

read_options <- list(kafka.bootstrap.servers = "localhost:29092",
                     subscribe = "parameter", startingOffsets="earliest")


# sql query on parameter stream
query <- str_interp('select from_avro(value, "parameter_value", \'${value_schema_str}\') as value from parameter')

query %>%
dbplyr::sql() %>%
tbl(sc, .)

# invoke style
expr <- str_interp("'${value_schema_str}'")

parameter%>%
spark_dataframe() %>%
invoke("select", invoke_static(sc, "org.apache.spark.sql.avro.functions", "from_avro", "value", expr), list())

stream_stop(stream)