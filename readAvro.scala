// /home/rstudio/spark/bin/spark-shell --repositories http://packages.confluent.io/maven/ --packages io.confluent:kafka-avro-serializer:5.4.1,io.confluent:kafka-schema-registry:5.4.1,org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.5,org.apache.spark:spark-avro_2.11:2.4.5,za.co.absa:abris_2.11:3.1.1
// https://blog.engineering.publicissapient.fr/2017/09/27/spark-comprendre-et-corriger-lexception-task-not-serializable/package sparklyr.confluent.avro

import io.confluent.kafka.schemaregistry.client.rest.RestService
import io.confluent.kafka.serializers.KafkaAvroDeserializer
import org.apache.avro.Schema
import collection.JavaConverters._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Row

case class DeserializedFromKafkaRecord(key: String, value: String)
val schemaRegistryURL = "http://schema-registry:8081"
val kafkaUrl = "broker:9092"
val topicName = "parameter"
val subjectKeyName = topicName + "-key"
val subjectValueName = topicName + "-value"
val restService = new RestService(schemaRegistryURL)
val keyRestResponseSchema = restService.getLatestVersion(subjectKeyName)
val valueRestResponseSchema = restService.getLatestVersion(subjectValueName)
val parser = new Schema.Parser
val topicKeyAvroSchema: Schema = parser.parse(keyRestResponseSchema.getSchema)
val topicValueAvroSchema: Schema = parser.parse(valueRestResponseSchema.getSchema)
val props = Map("schema.registry.url" -> schemaRegistryURL)
val spark: SparkSession = SparkSession.builder().appName("KafkaConsumerAvro").getOrCreate()
var keyDeserializer= new KafkaAvroDeserializer
keyDeserializer.configure(props.asJava, true)
var valueDeserializer= new KafkaAvroDeserializer
keyDeserializer.configure(props.asJava, false)
val rawTopicMessageDF = spark.readStream.format("kafka").option("kafka.bootstrap.servers", kafkaUrl).option("subscribe", topicName).option("startingOffsets", "earliest").load()

class Deserializer(var k: Schema, var v: Schema, var kd: KafkaAvroDeserializer, var vd: KafkaAvroDeserializer) {
  def deserialize(topicName:String, row: Row): DeserializedFromKafkaRecord = {
    val deserializedKeyString = kd.deserialize(topicName, row.getAs[Array[Byte]]("key"), k).toString
    val deserializedValueString = vd.deserialize(topicName, row.getAs[Array[Byte]]("value"), v).toString
    DeserializedFromKafkaRecord(deserializedKeyString, deserializedValueString)
  }
}

val kafkaAvroDeserializer = new Deserializer(topicKeyAvroSchema, topicValueAvroSchema, keyDeserializer, valueDeserializer)

object DeserializerWrapper {
  val deserializer = kafkaAvroDeserializer
}

//instantiate the SerDe classes if not already, then deserialize!
val deserializedTopicMessageDS = rawTopicMessageDF.map{
  row => DeserializerWrapper.deserializer.deserialize(topicName, row)
}

val deserializedDSOutputStream = deserializedTopicMessageDS.writeStream.outputMode("append").format("console").option("truncate", false).start()

