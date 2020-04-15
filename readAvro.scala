// /home/rstudio/spark/spark-3.0.0-preview-bin-hadoop3.2/bin/spark-shell --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.0-preview,org.apache.spark:spark-avro_2.12:3.0.0-preview

import org.apache.spark.sql.avro._

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

val df = spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", "broker:9092")
		.option("subscribe", "parameter")
		.option("startingOffsets", "earliest")
		.load()
		.select(from_avro($"value", value_schema_str ).as("value"))
		
val d = df.select(df.col("value.timestamp"), df.col("value.side"), df.col("value.id"))   

val query = df.writeStream.outputMode("append").format("console").start()

    
    