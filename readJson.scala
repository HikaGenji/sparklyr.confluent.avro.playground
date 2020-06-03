// /home/rstudio/spark/bin/spark-shell  --master spark://spark-master:7077 --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.5

import org.apache.spark._
import org.apache.spark.streaming._
import org.apache.spark.streaming.StreamingContext._

val df = spark.readStream.format("kafka").option("kafka.bootstrap.servers", "broker:9092").option("subscribe", "aggregate").load()
df.writeStream.format("console").start()