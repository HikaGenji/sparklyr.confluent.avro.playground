// /home/rstudio/spark/bin/spark-shell --repositories http://packages.confluent.io/maven/ --packages org.apache.kafka:kafka_2.11:5.4.1-ce,io.confluent:kafka-avro-serializer:5.4.1,io.confluent:kafka-schema-registry:5.4.1,org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.5,org.apache.spark:spark-avro_2.11:2.4.5
// https://blog.engineering.publicissapient.fr/2017/09/27/spark-comprendre-et-corriger-lexception-task-not-serializable/package sparklyr.confluent.avro

import io.confluent.kafka.schemaregistry.client.rest.RestService
import io.confluent.kafka.serializers.KafkaAvroDeserializer
import org.apache.avro.Schema
import collection.JavaConverters._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Row
import io.confluent.kafka.serializers.{KafkaAvroDecoder, AbstractKafkaAvroSerDeConfig, KafkaAvroDeserializerConfig}
import java.util.Properties
import org.apache.avro.generic.GenericData

// another example
// https://github.com/jaceklaskowski/spark-structured-streaming-book/issues/1


val kafkaUrl = "broker:9092"
val schemaRegistryURL = "http://schema-registry:8081"
val topic = "parameter"
val subjectValueName = topic + "-value"

val kafkaParams = Map[String, String](
"kafka.bootstrap.servers" -> kafkaUrl,
"key.deserializer" -> "KafkaAvroDeserializer",
"value.deserializer" -> "KafkaAvroDeserializer",
"group.id" -> "structured-kafka",
"auto.offset.reset" -> "earliest",
"failOnDataLoss"-> "false",
"schema.registry.url" -> schemaRegistryURL
)

object MyDeserializerWrapper {
  val props = new Properties()
  props.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryURL)
  props.put(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, "true")
  val vProps = new kafka.utils.VerifiableProperties(props)
  val deser = new KafkaAvroDecoder(vProps)
  val avro_schema = new RestService(schemaRegistryURL).getLatestVersion(subjectValueName)
  val messageSchema = new Schema.Parser().parse(avro_schema.getSchema)
}

case class DeserializedFromKafkaRecord( value: String)

val spark: SparkSession = SparkSession.builder().appName("KafkaConsumerAvro").getOrCreate()

val df = spark.readStream.format("kafka").option("subscribe", topic).options(kafkaParams).load().map( x=>{
  DeserializedFromKafkaRecord(MyDeserializerWrapper.deser.fromBytes(x.getAs[Array[Byte]]("value"), MyDeserializerWrapper.messageSchema).asInstanceOf[GenericData.Record].toString)
})

df.writeStream.outputMode("append").format("console").option("truncate", false).start()
