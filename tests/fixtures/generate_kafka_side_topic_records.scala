/**
 * Spark 3.5 `spark-shell -i` fixture producer for Java side-topic validation.
 *
 * This script only creates Kafka side-topic test data. The Java checker itself
 * is exercised by `spark-submit` from the root fixture runner.
 */
import java.io.{ByteArrayOutputStream, BufferedWriter, OutputStreamWriter}
import java.nio.charset.StandardCharsets
import java.util.{Collections, Properties}

import org.apache.avro.Schema
import org.apache.avro.file.DataFileWriter
import org.apache.avro.generic.{GenericData, GenericDatumWriter, GenericRecord}
import org.apache.hadoop.fs.Path
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord, RecordMetadata}

val FixturePrefix = "[recon-side-fixtures]"

/** Reads `recon.*` or `spark.recon.*` fixture configuration. */
def confOption(key: String): Option[String] = {
  val aliases = Seq(key, s"spark.$key").distinct

  def fromSparkConf(candidate: String): Option[String] =
    scala.util.Try(spark.conf.get(candidate)).toOption.map(_.trim).filter(_.nonEmpty)

  def fromSparkShellCommand(candidate: String): Option[String] = {
    val command = Option(System.getProperty("sun.java.command")).getOrElse("")
    val tokens = command.split("\\s+").toSeq.filter(_.nonEmpty)
    val pairPrefix = candidate + "="
    val inlinePrefix = "--conf=" + candidate + "="

    tokens.sliding(2).collectFirst {
      case Seq("--conf", value) if value.startsWith(pairPrefix) =>
        value.substring(pairPrefix.length)
    }.orElse {
      tokens.collectFirst {
        case value if value.startsWith(inlinePrefix) =>
          value.substring(inlinePrefix.length)
      }
    }.map(_.trim).filter(_.nonEmpty)
  }

  aliases.view.flatMap(candidate => fromSparkConf(candidate).orElse(fromSparkShellCommand(candidate))).headOption
}

val bootstrapServers = confOption("recon.kafkaBootstrapServers").getOrElse {
  Console.err.println(s"$FixturePrefix missing recon.kafkaBootstrapServers")
  System.exit(2)
  ""
}
val sourceTopic = confOption("recon.fixtureSourceTopic").orElse(confOption("recon.sourceTopic")).getOrElse("orders")
val canaryTopic = confOption("recon.canaryTopic").getOrElse("orders-canary")
val deadLetterTopic = confOption("recon.deadLetterTopic").getOrElse("orders-dlq")
val deadLetterOnlyTopic = confOption("recon.fixtureDeadLetterOnlyTopic").getOrElse("orders-dlq-only")
val badCanaryTopic = confOption("recon.fixtureBadCanaryTopic").getOrElse("orders-bad-canary")
val manifestPath = confOption("recon.sideTopicManifestPath").getOrElse("/tmp/recon-side-topic-records.tsv")

val canarySchemaText =
  """{
    |"type":"record",
    |"name":"CanaryRecord",
    |"fields":[
    |{"name":"sourceKey","type":["null","string"],"default":null},
    |{"name":"sourceValue","type":["null","string"],"default":null},
    |{"name":"sourceHeaders","type":{"type":"map","values":"string"}},
    |{"name":"sourceTopic","type":"string"},
    |{"name":"sourcePartition","type":"int"},
    |{"name":"sourceOffset","type":"long"},
    |{"name":"sourceKafkaTimestamp","type":"long"}
    |]
    |}""".stripMargin

val deadLetterSchemaText =
  """{
    |"type":"record",
    |"name":"DeadLetterRecord",
    |"fields":[
    |{"name":"sourceKey","type":["null","string"],"default":null},
    |{"name":"sourceValue","type":["null","string"],"default":null},
    |{"name":"sourceHeaders","type":{"type":"map","values":"string"}},
    |{"name":"sourceTopic","type":"string"},
    |{"name":"sourcePartition","type":"int"},
    |{"name":"sourceOffset","type":"long"},
    |{"name":"sourceKafkaTimestamp","type":"long"},
    |{"name":"failureEventId","type":["null","string"],"default":null},
    |{"name":"reasonMsg","type":["null","string"],"default":null},
    |{"name":"exception","type":["null","string"],"default":null}
    |]
    |}""".stripMargin

case class SideFixtureRecord(
  topic: String,
  kind: String,
  recordSourceTopic: String,
  sourcePartition: Int,
  sourceOffset: Long,
  purpose: String,
  failureEventId: Option[String] = None,
  reasonMsg: Option[String] = None,
  exceptionName: Option[String] = None
)

def avroPayload(recordSpec: SideFixtureRecord): Array[Byte] = {
  val deadLetter = recordSpec.kind == "dead_letter"
  val schema = new Schema.Parser().parse(if (deadLetter) deadLetterSchemaText else canarySchemaText)
  val record = new GenericData.Record(schema)
  record.put("sourceKey", s"${recordSpec.recordSourceTopic}-${recordSpec.sourcePartition}-${recordSpec.sourceOffset}")
  record.put("sourceValue", s"value-${recordSpec.sourceOffset}")
  record.put("sourceHeaders", Collections.emptyMap[String, String]())
  record.put("sourceTopic", recordSpec.recordSourceTopic)
  record.put("sourcePartition", recordSpec.sourcePartition)
  record.put("sourceOffset", recordSpec.sourceOffset)
  record.put("sourceKafkaTimestamp", 1710000000000L + recordSpec.sourceOffset)
  if (deadLetter) {
    record.put("failureEventId", recordSpec.failureEventId.orNull)
    record.put("reasonMsg", recordSpec.reasonMsg.orNull)
    record.put("exception", recordSpec.exceptionName.orNull)
  }

  val output = new ByteArrayOutputStream()
  val datumWriter = new GenericDatumWriter[GenericRecord](schema)
  val writer = new DataFileWriter[GenericRecord](datumWriter)
  writer.create(schema, output)
  writer.append(record)
  writer.close()
  output.toByteArray
}

val props = new Properties()
props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers)
props.put(ProducerConfig.CLIENT_ID_CONFIG, "recon-side-topic-fixture-producer")
props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArraySerializer")
props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArraySerializer")
props.put(ProducerConfig.ACKS_CONFIG, "all")
props.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, "10000")
props.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, "30000")

val records = Seq(
  SideFixtureRecord(canaryTopic, "canary", sourceTopic, 0, 1L, "canary_only_and_combined_match"),
  SideFixtureRecord(canaryTopic, "canary", sourceTopic, 9, 7L, "false_partition_ignored"),
  SideFixtureRecord(canaryTopic, "canary", "payments", 0, 1L, "false_source_topic_ignored"),
  SideFixtureRecord(
    deadLetterTopic,
    "dead_letter",
    sourceTopic,
    0,
    2L,
    "combined_dead_letter_match",
    Some("evt-combined-2"),
    Some("combined failure"),
    Some("IllegalStateException")
  ),
  SideFixtureRecord(
    deadLetterTopic,
    "dead_letter",
    "payments",
    0,
    2L,
    "false_dead_letter_source_topic_ignored",
    Some("evt-payments-2"),
    Some("wrong source topic"),
    Some("RuntimeException")
  ),
  SideFixtureRecord(
    deadLetterOnlyTopic,
    "dead_letter",
    sourceTopic,
    0,
    1L,
    "dead_letter_only_match",
    Some("evt-only-1"),
    Some("dead letter only failure"),
    Some("IllegalArgumentException")
  )
)

val producer = new KafkaProducer[Array[Byte], Array[Byte]](props)
val producedRows = scala.collection.mutable.ArrayBuffer[String]()
producedRows += "topic\tkind\tsource_topic\tsource_partition\tsource_offset\tpurpose\tkafka_partition\tkafka_offset\tpayload_bytes"

try {
  records.foreach { recordSpec =>
    val key = s"${recordSpec.recordSourceTopic}-${recordSpec.sourcePartition}-${recordSpec.sourceOffset}-${recordSpec.purpose}"
      .getBytes(StandardCharsets.UTF_8)
    val payload = avroPayload(recordSpec)
    val metadata: RecordMetadata = producer.send(new ProducerRecord[Array[Byte], Array[Byte]](
      recordSpec.topic,
      key,
      payload
    )).get()
    producedRows += Seq(
      recordSpec.topic,
      recordSpec.kind,
      recordSpec.recordSourceTopic,
      recordSpec.sourcePartition.toString,
      recordSpec.sourceOffset.toString,
      recordSpec.purpose,
      metadata.partition().toString,
      metadata.offset().toString,
      payload.length.toString
    ).mkString("\t")
  }

  val badPayload = "not-an-avro-object-container".getBytes(StandardCharsets.UTF_8)
  val badMetadata = producer.send(new ProducerRecord[Array[Byte], Array[Byte]](
    badCanaryTopic,
    "bad-canary".getBytes(StandardCharsets.UTF_8),
    badPayload
  )).get()
  producedRows += Seq(
    badCanaryTopic,
    "invalid",
    sourceTopic,
    "0",
    "1",
    "undecodable_payload",
    badMetadata.partition().toString,
    badMetadata.offset().toString,
    badPayload.length.toString
  ).mkString("\t")
} finally {
  producer.flush()
  producer.close()
}

val manifest = new Path(manifestPath)
val fs = manifest.getFileSystem(spark.sparkContext.hadoopConfiguration)
if (fs.exists(manifest)) {
  fs.delete(manifest, false)
}
val out = fs.create(manifest, true)
val writer = new BufferedWriter(new OutputStreamWriter(out, StandardCharsets.UTF_8))
try {
  producedRows.foreach { row =>
    writer.write(row)
    writer.newLine()
  }
} finally {
  writer.close()
}

println(s"$FixturePrefix bootstrap_servers=$bootstrapServers")
println(s"$FixturePrefix source_topic=$sourceTopic")
println(s"$FixturePrefix canary_topic=$canaryTopic")
println(s"$FixturePrefix dead_letter_topic=$deadLetterTopic")
println(s"$FixturePrefix dead_letter_only_topic=$deadLetterOnlyTopic")
println(s"$FixturePrefix bad_canary_topic=$badCanaryTopic")
println(s"$FixturePrefix record_count=${records.size + 1}")
println(s"$FixturePrefix manifest=$manifestPath")
