// /home/rstudio/spark/spark-3.0.0-preview-bin-hadoop2.7/bin/spark-shell --conf "spark.driver.extraJavaOptions=-Djava.net.useSystemProxies=true" --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.0-preview,org.apache.spark:spark-avro_2.12:3.0.0-preview

import org.apache.spark.sql.avro.functions._

val key_schema_str = """
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
"""

val value_schema_str = """
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
"""

val df = spark
  .readStream
  .format("kafka")
  .option("kafka.bootstrap.servers", "localhost:29092")
  .option("subscribe", "parameter")
  .load()
  .select(
    from_avro($"key", key_schema_str).as("key"), 
    from_avro($"value", value_schema_str ).as("value"))
    
    
    