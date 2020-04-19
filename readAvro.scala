// https://github.com/hortonworks-spark/spark-schema-registry

/*
/home/rstudio/spark/bin/spark-shell --jars  /usr/local/lib/R/site-library/sparklyudf/java/spark-schema-registry-0.1-SNAPSHOT-jar-with-dependencies.jar --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.5,org.apache.spark:spark-avro_2.11:2.4.5
--class com.hortonworks.spark.registry.examples.<classname> \
spark-schema-registry-examples-0.1-SNAPSHOT.jar <schema-registry-url> \
<bootstrap-servers> <input-topic> <output-topic> <checkpoint-location>
*/

import org.apache.spark.sql.avro._

// val query = df.writeStream.outputMode("append").format("memory").queryName("test").start()

import java.util.UUID

import com.hortonworks.spark.registry.util._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.{OutputMode, Trigger}

/**
 * This example de-serializes the ouput produced by [[SchemaRegistryAvroExample]] and
 * prints the output to console. The schema is automatically infered by querying the schema
 * registry.
 *
 * Usage:
 * SchemaRegistryAvroReader <schema-registry-url> <bootstrap-servers> <input-topic> <checkpoint-location> [security.protocol]
 */
 
object SchemaRegistryAvroReader {

  def run(): Unit = {

    val schemaRegistryUrl = "http://schema-registry:8081/api/v1/"
    val bootstrapServers = "broker:9092"
    val topic = "parameter"
    val checkpointLocation =UUID.randomUUID.toString
    val securityProtocol ="PLAINTEXT"

    val spark = SparkSession.builder.appName("SchemaRegistryAvroReader").getOrCreate()

    val reader = spark.readStream.format("kafka").option("kafka.bootstrap.servers", bootstrapServers).option("subscribe", topic)

    val messages = reader.load()

    import spark.implicits._

    // the schema registry client config
    val config = Map[String, Object]("schema.registry.url" -> schemaRegistryUrl)

    // the schema registry config that will be implicitly passed
    implicit val srConfig: SchemaRegistryConfig = SchemaRegistryConfig(config)
	
	val df = messages.select(from_sr($"value", topic).alias("message"))

    // Read messages from kafka and deserialize.
    // This uses the schema registry schema associated with the topic.
    val df = messages.select(from_sr($"value", topic).alias("message"))

    // write the output to console
    // should produce events like {"driverId":14,"truckId":25,"miles":373}
    val query = df
      .writeStream
      .format("console")
      .trigger(Trigger.ProcessingTime(10000))
      .outputMode(OutputMode.Append())
      .start()

    query.awaitTermination()
  }

}

    
    