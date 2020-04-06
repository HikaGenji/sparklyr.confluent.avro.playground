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
config[["sparklyr.shell.conf"]] <- "spark.driver.extraJavaOptions=-Djava.net.useSystemProxies=true"
config[["sparklyr.shell.packages"]] <- c("org.apache.spark:spark-sql-kafka-0-10_2.12:2.4.3" , "org.apache.spark:spark-avro_2.12:2.4.3")

sc <- spark_connect(master = "local", config = config)

read_options <- list(kafka.bootstrap.servers = "localhost:29092",
                     subscribe = "parameter", startingOffsets="earliest")

# get kafka data into spark
stream_read_kafka(sc, options = read_options) %>%
spark_dataframe() %>%
stream_write_memory(name="parameter")

parameter <- tbl(sc, "parameter")
head(parameter)

# get some stats
parameter %>%
spark_dataframe() %>%
invoke("describe", as.list(colnames(parameter))) %>%
sdf_register()

# using invoke
parameter%>%
spark_dataframe() %>%
invoke("select", "value", list()) %>%
collect()

# sql query on parameter stream
query <- str_interp('select from_avro(value, "parameter_value", \'${value_schema_str}\') as value from parameter')

query %>%
dbplyr::sql() %>%
tbl(sc, .)

# invoke style
expr <- str_interp("'${value_schema_str}'")

parameter%>%
spark_dataframe() %>%
invoke("select", invoke_static(sc, "org.apache.spark.sql.avro.functions", "from_avro", "value", expr), list()) %>%
collect()

stream_stop(stream)

# 'spark.sql.debug.maxToStringFields'
