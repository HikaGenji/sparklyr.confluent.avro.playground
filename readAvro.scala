// /home/rstudio/spark/bin/spark-shell --repositories http://packages.confluent.io/maven/ --packages io.confluent:kafka-avro-serializer:5.4.1,io.confluent:kafka-schema-registry:5.4.1,org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.5,org.apache.spark:spark-avro_2.11:2.4.5,za.co.absa:abris_2.11:3.1.1

import java.util.Properties

import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, Dataset, Row}
import za.co.absa.abris.examples.utils.ExamplesUtils._
import org.apache.spark.sql.SparkSession
import za.co.absa.abris.avro.functions.from_confluent_avro

object ConfluentKafkaAvroReader {
  val PARAM_JOB_NAME = "job.name"
  val PARAM_JOB_MASTER = "job.master"
  val PARAM_PAYLOAD_AVRO_SCHEMA = "payload.avro.schema"
  val PARAM_LOG_LEVEL = "log.level"
  val PARAM_OPTION_SUBSCRIBE = "option.subscribe"
  val kafkaUrl = "broker:9092"
  val PARAM_EXAMPLE_SHOULD_USE_SCHEMA_REGISTRY = "example.should.use.schema.registry"
  val kafkaUrl = "broker:9092"
  
  def main(): Unit = {
    val properties = new Properties()
	properties.setProperty("job.name", "SampleJob")
	properties.setProperty("job.master", "local[*]")
	properties.setProperty("key.schema.id", "latest")
	properties.setProperty("value.schema.id", "latest")
	properties.setProperty("value.schema.naming.strategy", "topic.name")
	properties.setProperty("schema.name", "native_complete")
	properties.setProperty("schema.registry.topic", "parameter")
	properties.setProperty("option.subscribe", "parameter")
	properties.setProperty("schema.namespace", "all-types.test")
	properties.setProperty("key.schema.id", "latest")
	properties.setProperty("log.level", "ERROR")
	properties.setProperty("schema.registry.url", "http://schema-registry:8081")
	
	val spark = getSparkSession(properties, PARAM_JOB_NAME, PARAM_JOB_MASTER, PARAM_LOG_LEVEL)
	val schemaRegistryConfig = properties.getSchemaRegistryConfigurations("option.subscribe")
    val stream = spark.readStream.format("kafka").option("startingOffsets", "earliest").option("kafka.bootstrap.servers", kafkaUrl).addOptions(properties)
    val deserialized =   stream.load().select(from_confluent_avro(col("value"), schemaRegistryConfig) as 'data)
    deserialized.printSchema()
    deserialized.writeStream.format("console").option("truncate", "false").start().awaitTermination(1000)
  }
}