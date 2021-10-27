/**
 * (C) Copyright IBM Corp. 2021
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package org.alvearie.hri.flink.core

import java.util.Properties
import java.util.concurrent.TimeUnit

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.node.ObjectNode
import org.alvearie.hri.api.{BatchLookup, BatchNotification, InvalidRecord}
import org.alvearie.hri.flink.core.serialization._
import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.api.common.state.{MapStateDescriptor, StateTtlConfig}
import org.apache.flink.api.common.time.Time
import org.apache.flink.api.common.typeinfo.{BasicTypeInfo, TypeInformation}
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.environment.CheckpointConfig
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction
import org.apache.flink.streaming.api.scala.{OutputTag, StreamExecutionEnvironment}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer.Semantic
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer, FlinkKafkaProducer}
import org.slf4j.LoggerFactory

/**
 * The standard constructor for Validation Jobs.
 * Please use this constructor for all production use-cases. Other constructors are for testing only.
 *
 * @param inputTopic the Kafka topic to get data records from
 * @param brokers    this list of Kafka brokers
 * @param password   the Kafka API password
 * @param validator  the instance to use to validate records
 * @param mgmtApiUrl the base url of the HRI Management API. Should end in '/hri'
 * @param mgmtClientId the client Id to use with the HRI Management API.
 * @param mgmtClientSecret the client secret to use with the HRI Management API.
 * @param oauthServiceBaseUrl the base URL of the OAuth service to request access tokens from.
 * @param batchCompletionDelay the amount of time to wait in milliseconds before completing a batch.
 */
class BaseValidationJob(
                       inputTopic: String,
                       brokers: Array[String],
                       password: String,
                       validator: Validator,
                       mgmtApiUrl: String,
                       mgmtClientId: String,
                       mgmtClientSecret: String,
                       mgmtClientAudience: String,
                       oauthServiceBaseUrl: String,
                       batchCompletionDelay: Long = 300000
                       ) {
  private val log = LoggerFactory.getLogger(this.getClass)
  private val RequiredInputTopicPrefix: String = "ingest."
  private val RequiredInputTopicSuffix: String = ".in"
  if (!inputTopic.startsWith(RequiredInputTopicPrefix)) {
    throw new TopicNamingException("The Input Topic Name " + inputTopic
      + " is invalid. It must start with \"" + RequiredInputTopicPrefix + "\"")
  }
  if (!inputTopic.endsWith(RequiredInputTopicSuffix)) {
    throw new TopicNamingException("The Input Topic Name " + inputTopic
      + " is invalid. It must end with \"" + RequiredInputTopicSuffix + "\"")
  }
  private val topicBase = inputTopic.split('.').init
  if (topicBase.length < 3) {
    throw new TopicNamingException("The Input Topic Name " + inputTopic
      + s" is invalid. It must have 4 elements separated by a '.' but only had ${topicBase.length+1}. "
      + "Expected pattern: 'ingest.<tenant-id>.<data-integrator-id>[.optional-qualifier].in'.")
  }
  private val outputTopic = BaseValidationJob.getOutputTopic(topicBase)
  private val invalidTopic = BaseValidationJob.getInvalidTopic(topicBase)
  private val notificationTopic = BaseValidationJob.getNotificationTopic(topicBase)
  private val tenantId = BaseValidationJob.getTenantId(topicBase)

  private val props = BaseValidationJob.createKafkaProperties(brokers, password, inputTopic, outputTopic)
  private val notificationProps = BaseValidationJob.createKafkaProperties(brokers, password, inputTopic, outputTopic)
  // For the Notification Kafka Consumer/Source, set a small fetch timeout so that the Batch Notifications are always
  // received before the HRI records.
  notificationProps.put("fetch.max.wait.ms", "150")

  // controls whether the Tracker sink calls the HRI Management API to complete or fail batches,
  // or posts Batch Notification messages directly to the notification topic
  private var useMgmtApi = true

  //Define Sources(Input) & Sinks(Output) at Class level
  private var notificationMsgSourceOpt: Option[RichParallelSourceFunction[NotificationRecord]] = None
  private var dataRecordsSourceOpt: Option[RichParallelSourceFunction[HriRecord]] = None
  private var validRecordsSinkOpt: Option[RichSinkFunction[HriRecord]] = None
  private var invalidRecordsSinkOpt: Option[RichSinkFunction[InvalidRecord]] = None
  private var recordCountSinkOpt: Option[RichSinkFunction[NotificationRecord]] = None
  private var mgmtClientOpt: Option[BatchLookup] = None

  /**
   * Creates a ValidationJob that does not use the HRI Management API to complete or fail batches.
   * Instead it posts Batch notifications directly to the '*.notification' topic.
   * This constructor is only intended to be used for functional testing without the HRI Management API.
   *
   * @param inputTopic the Kafka topic to get data records from
   * @param brokers    this list of Kafka brokers
   * @param password   the Kafka API password
   * @param validator  the instance to use to validate records
   * @param batchCompletionDelay the amount of time to wait in milliseconds before completing a batch.
   */
  def this(inputTopic: String,
           brokers: Array[String],
           password: String,
           validator: Validator,
           batchCompletionDelay: Long) {
    this(inputTopic, brokers, password, validator, null, null, null, null, null, batchCompletionDelay)
    this.useMgmtApi = false
  }

  /**
   * Creates a ValidationJob with custom sources and sinks.
   * This constructor is only intended to be used for local unit testing.
   *
   * @param validator       the instance to use to validate records
   * @param notificationSrc source for Batch Notifications
   * @param dataRecsSrc     source for data records
   * @param validRecsSink   sink for valid records
   * @param invalidRecsSink sink for invalid records
   * @param recCountSink    sink for completed and failed batches
   * @param mgmtClient      mgmt client to check batch status
   * @param batchCompletionDelay the amount of time to wait in milliseconds before completing a batch.
   */
  def this(validator: Validator,
           notificationSrc: RichParallelSourceFunction[NotificationRecord],
           dataRecsSrc: RichParallelSourceFunction[HriRecord],
           validRecsSink: RichSinkFunction[HriRecord],
           invalidRecsSink: RichSinkFunction[InvalidRecord],
           recCountSink: RichSinkFunction[NotificationRecord],
           mgmtClient: BatchLookup,
           batchCompletionDelay: Long
          ) {
    // pass in default values, so initialization doesn't fail
    this("ingest.tenant.da.in", Array[String](), "password", validator, null, null, null, null, null, batchCompletionDelay)
    this.mgmtClientOpt = Some(mgmtClient)
    this.notificationMsgSourceOpt = Some(notificationSrc)
    this.dataRecordsSourceOpt = Some(dataRecsSrc)
    this.validRecordsSinkOpt = Some(validRecsSink)
    this.invalidRecordsSinkOpt = Some(invalidRecsSink)
    this.recordCountSinkOpt = Some(recCountSink)
    this.useMgmtApi = false
  }

  def startJob(jobNamePrefix: String = "HRI Validation"): Unit = {
    val jobName = s"$jobNamePrefix: $inputTopic -> $outputTopic"
    log.info("Starting Validation job {}, for tenant {}, useMgmtApi {}", jobName, tenantId, useMgmtApi.toString)
    val notificationDescriptor = BaseValidationJob.NOTIFICATION_DESCRIPTOR
    notificationDescriptor.enableTimeToLive(BaseValidationJob.getStateTtlConfig)

    // these help Scala infer types
    implicit val jsonRecordTypeInfo: TypeInformation[NotificationRecord] = TypeInformation.of(classOf[NotificationRecord])
    implicit val hriRecordTypeInfo: TypeInformation[HriRecord] = TypeInformation.of(classOf[HriRecord])
    implicit val invalidRecordTypeInfo: TypeInformation[InvalidRecord] = TypeInformation.of(classOf[InvalidRecord])
    implicit val objectNodeTypeInfo: TypeInformation[ObjectNode] = TypeInformation.of(classOf[ObjectNode])
    implicit val jsonNodeTypeInfo: TypeInformation[JsonNode] = TypeInformation.of(classOf[JsonNode])
    implicit val stringTypeInfo: TypeInformation[String] = TypeInformation.of(classOf[String])

    val env = BaseValidationJob.getEnv

    // define notification message input stream
    val notificationsStream = env
      .addSource(notificationMsgSourceOpt.getOrElse(getNotificationSource(notificationProps)))
      .name("Notification Message Input Stream")
      .uid(s"$jobName:notifications")
      .setBufferTimeout(10) // set a short buffer timeout so that Notification records are sent very quickly
      .setParallelism(1) // only one instance is needed to read and broadcast the notifications
    val notificationBroadcastStream = notificationsStream
      .broadcast(notificationDescriptor)

    val invalidOutputTag: OutputTag[InvalidRecord] = new OutputTag[InvalidRecord]("invalid")
    val countsOutputTag: OutputTag[ObjectNode] = new OutputTag[ObjectNode]("counts")

    // define data records input stream
    val dataStream = env
      .addSource(dataRecordsSourceOpt.getOrElse(getDataRecsSource(props)))
      .name("Data Records Input Stream")
      .uid(s"$jobName:input")
      .connect(notificationBroadcastStream)
      .process(getValidationProcessFunction(notificationDescriptor, invalidOutputTag, countsOutputTag))
      .name("Validation")

    // define valid records output stream
    val validStream = dataStream
      .addSink(validRecordsSinkOpt.getOrElse(getValidRecsSink(props)))
      .name("Valid Records Output Stream")
      .uid(s"$jobName:out")


    // define invalid records output stream
    val invalidStream = dataStream.getSideOutput(invalidOutputTag)
      .addSink(invalidRecordsSinkOpt.getOrElse(getInvalidRecsSink(props)))
      .name("Invalid Records Output Stream")
      .uid(s"$jobName:invalid")

    // define record counts stream
    // totalParallel is the parallelism set for the job.  This sets the Tracker and associated sink parallelism to
    // a fraction of job's parallelism.
    val totalParallel = env.getParallelism
    val parallelFraction = 0.25
    val parallelism = if (totalParallel * parallelFraction > 1) math.floor(totalParallel * parallelFraction).toInt else 1

    // define record counts stream
    val countsStream = dataStream.getSideOutput(countsOutputTag)
      .keyBy(_.get("batchId").asText())
      .connect(notificationsStream.keyBy(_.value.getId))
      .process(new Tracker(batchCompletionDelay))
      .name("Tracker")
      .uid(s"$jobName:tracker")
      .setParallelism(parallelism)
      .addSink(recordCountSinkOpt.getOrElse(getRecordCountSink(props)))
      .setParallelism(parallelism)
      .name("Batch Status")
      .uid(s"$jobName:tracker-sink")

    env.execute(jobName)
  }

  private def getNotificationSource(props: Properties): RichParallelSourceFunction[NotificationRecord] = {
    new FlinkKafkaConsumer[NotificationRecord](
      notificationTopic,
      new NotificationDeserializer(),
      props
    )
  }

  private def getDataRecsSource(props: Properties): RichParallelSourceFunction[HriRecord] = {
    new FlinkKafkaConsumer[HriRecord](
      inputTopic,
      new HriRecordDeserializer(),
      props
    )
  }

  private[core] def getValidationProcessFunction(notificationDescriptor: MapStateDescriptor[String, BatchNotification],
                                           invalidOutputTag: OutputTag[InvalidRecord],
                                           countsOutputTag: OutputTag[ObjectNode]): ValidationProcessFunction = {
    if (mgmtClientOpt.isDefined) { //for testing
      return new TestValidationProcessFunction(notificationDescriptor, invalidOutputTag, countsOutputTag,
        validator, tenantId, mgmtClientOpt.get)
    } else if (useMgmtApi) { //constructs validationProcessFunction with mgmtClient using passed in url and key
      return new ValidationProcessFunction(notificationDescriptor, invalidOutputTag, countsOutputTag,
        validator, tenantId, mgmtApiUrl, mgmtClientId, mgmtClientSecret, mgmtClientAudience, oauthServiceBaseUrl)
    } else { //constructs validationProcessFunction without a mgmtClient
      return new ValidationProcessFunction(notificationDescriptor, invalidOutputTag, countsOutputTag, validator)
    }
  }

  private def getValidRecsSink(props: Properties): RichSinkFunction[HriRecord] = {
    new FlinkKafkaProducer[HriRecord](
      outputTopic,
      new HriRecordSerializer(outputTopic),
      props,
      Semantic.EXACTLY_ONCE
    )
  }

  private def getInvalidRecsSink(props: Properties): RichSinkFunction[InvalidRecord] = {
    new FlinkKafkaProducer[InvalidRecord](
      invalidTopic,
      new InvalidRecordSerializer(invalidTopic),
      props,
      Semantic.EXACTLY_ONCE
    )
  }

  // This gets the Sink for completing or failing batches. When `useMgmtApi` is true, this returns the HRI Management
  // API Sink. When false this returns a Kakfa Sink that posts messages directly to the Notification topic, which is
  // used for functional testing of the Validation Jobs without the HRI Management API.
  def getRecordCountSink(props: Properties): SinkFunction[NotificationRecord] = {
    if (useMgmtApi) {
      log.info("Creating MgmtApiSink({}) for Tracker output", mgmtApiUrl)
      return new MgmtApiSink(tenantId, mgmtApiUrl, mgmtClientId, mgmtClientSecret, mgmtClientAudience, oauthServiceBaseUrl)
    } else {
      log.info("Creating KafkaProducer({}) for Tracker output", notificationTopic)
      return new FlinkKafkaProducer[NotificationRecord](
        notificationTopic,
        new NotificationSerializer(notificationTopic),
        props,
        Semantic.EXACTLY_ONCE
      )
    }
  }

}

object BaseValidationJob {
  val NOTIFICATION_DESCRIPTOR = new MapStateDescriptor[String, BatchNotification](
    "NotificationsBroadcastState",
    BasicTypeInfo.STRING_TYPE_INFO,
    TypeInformation.of(classOf[BatchNotification])
  )

  def getOutputTopic(topicBase: Array[String]) : String = {
    topicBase :+ "out" mkString "."
  }

  def getInvalidTopic(topicBase: Array[String]): String = {
    topicBase :+ "invalid" mkString "."
  }

  def getNotificationTopic(topicBase: Array[String]): String = {
    topicBase :+ "notification" mkString "."
  }

  def getTenantId(topicBase: Array[String]): String = {
    topicBase(1)
  }

  def createKafkaProperties (brokers: Array[String], password:String,
                             propInputTopic:String, outputTopic:String) : Properties = {
    var props:Properties = null

    props = new Properties
    props.put("bootstrap.servers", brokers.mkString(","))
    if (password != null) {
      props.put("sasl.jaas.config", s"""org.apache.kafka.common.security.plain.PlainLoginModule required username="token" password="$password";""")
      props.put("security.protocol", "SASL_SSL")
      props.put("sasl.mechanism", "PLAIN")
      props.put("ssl.protocol", "TLSv1.2")
      props.put("ssl.enabled.protocols", "TLSv1.2")
      props.put("ssl.endpoint.identification.algorithm", "HTTPS")
      props.put("auto.offset.reset", "latest")
    }

    props.put("group.id", s"hri-validation-$propInputTopic-$outputTopic")
    props.put("compression.type", "zstd")
    // 900000 (15 min) is the default Broker max timeout (transaction.max.timeout.ms). Flink defaults the client timeout
    //to 3600000 (1 hour) when checkpointing is enabled, which causes a Kafka Exception because it's larger than the max.
    //So use the default max value.
    props.put("transaction.timeout.ms", "900000")

    // Event Streams has a 1 MB message size limit. A message is a collection of records sent together.
    // So we need to prevent the Kafka Producers from trying to put too many records in a message. There isn't a direct
    // config setting for this. This Kafka ticket documents the problem: https://issues.apache.org/jira/browse/KAFKA-4169
    //
    // 'batch.size' is a soft limit on the size of requests. For records larger than this no batching is performed. I
    // believe this property is before compression, but the documentation isn't clear. This should prevent requests from
    // getting too big due to batching.
    //
    // 'max.request.size' is a hard limit on the size of requests and is before compression. The default is 1MB, so this
    // is increased to allow larger messages through.
    props.put("batch.size", "16384") // 16 KB
    props.put("max.request.size", "10485760") // 10 MB

    props
  }

  def getStateTtlConfig : StateTtlConfig = {
    StateTtlConfig
      .newBuilder(Time.days(7))
      .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
      .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
      .cleanupFullSnapshot()
      .build
  }

  def getEnv: StreamExecutionEnvironment = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    // start a checkpoint every 1 second
    // The checkpoint interval also determines how often the Kafka Producers commit transactions. Producers have to
    // commit all the records they process between checkpoints as a single transaction. So we want a short interval.
    env.enableCheckpointing(1000)

    // set mode to exactly-once
    env.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)

    // Don't set a minimum pause between checkpoints
    //env.getCheckpointConfig.setMinPauseBetweenCheckpoints(100)

    // checkpoints have to complete within 2 min, or are discarded
    env.getCheckpointConfig.setCheckpointTimeout(120000)

    // allow only one checkpoint to be in progress at the same time
    env.getCheckpointConfig.setMaxConcurrentCheckpoints(1)

    // enable externalized checkpoints which are deleted after job cancellation
    env.getCheckpointConfig.enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.DELETE_ON_CANCELLATION)

    /**
     * Failure Rate Restart Strategy
     * restarting the job after failure for 10 times every 5 minutes
     * https://ci.apache.org/projects/flink/flink-docs-release-1.10/dev/task_failure_recovery.html#fallback-restart-strategy
    */
    env.setRestartStrategy(RestartStrategies.failureRateRestart(
      10, // max failures per time interval
      Time.of(5, TimeUnit.MINUTES), //time interval for measuring failure rate
      Time.of(10, TimeUnit.SECONDS) //delay between 2 consecutive restart attempts
    ))

    env
  }
}
