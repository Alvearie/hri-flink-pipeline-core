/**
 * (C) Copyright IBM Corp. 2021
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package org.alvearie.hri.flink.core

import com.fasterxml.jackson.databind.node.ObjectNode
import org.alvearie.hri.api.{BatchLookup, InvalidRecord, MapBatchLookup}
import org.alvearie.hri.flink.core.jobtest.TestValidator
import org.alvearie.hri.flink.core.serialization.HriRecord
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.streaming.api.scala.OutputTag
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers._

class BaseValidationJobPropertiesTest extends AnyFunSuite {

  val TopicBase = "ingest.tenant1.da2"
  val TestInputTopic: String = TopicBase + ".in"
  val TestTopicArray: Array[String] = TestInputTopic.split('.').init
  val TestPassword = "FakePassword"
  val TestBrokers: Array[String] = Array[String]("broker1:9092","broker2:9092")
  val TestInputTopic2 = "ingest.22.da3.in"

  implicit val hriRecordTypeInfo: TypeInformation[HriRecord] = TypeInformation.of(classOf[HriRecord])
  implicit val invalidRecordTypeInfo: TypeInformation[InvalidRecord] = TypeInformation.of(classOf[InvalidRecord])
  implicit val objectNodeTypeInfo: TypeInformation[ObjectNode] = TypeInformation.of(classOf[ObjectNode])

  test("it should get the correct output topic") {
    val topicName:String = BaseValidationJob.getOutputTopic(TestTopicArray)
    topicName should equal("ingest.tenant1.da2.out")
  }

  test("it should get the correct invalid topic") {
    val topicName = BaseValidationJob.getInvalidTopic(TestTopicArray)
    topicName should equal("ingest.tenant1.da2.invalid")
  }

  test("it should get the correct notification topic") {
    val topicName = BaseValidationJob.getNotificationTopic(TestTopicArray)
    topicName should equal("ingest.tenant1.da2.notification")
  }

  test("it should return correct Job Properties") {
    val outputTopic:String = BaseValidationJob.getOutputTopic(TestInputTopic2.split('.').init)

    val baseJob = new BaseValidationJob(TestInputTopic2, TestBrokers, TestPassword, (_ => (true,null)) : Validator, null, null, null, null, null, 100)

    val props = BaseValidationJob.createKafkaProperties(TestBrokers, TestPassword,
      TestInputTopic2, outputTopic)

    val expectedJassConfig = """org.apache.kafka.common.security.plain.PlainLoginModule required username="token" password="""" + TestPassword + "\";"
    props.getProperty("sasl.jaas.config") should equal(expectedJassConfig)
    props.getProperty("security.protocol") should equal("SASL_SSL")
    props.getProperty("sasl.mechanism") should equal("PLAIN")
    props.getProperty("ssl.protocol") should equal("TLSv1.2")
    props.getProperty("ssl.enabled.protocols") should equal("TLSv1.2")
    props.getProperty("ssl.endpoint.identification.algorithm") should equal("HTTPS")
    val expectedGroupId = "hri-validation-" + TestInputTopic2 + "-" + outputTopic
    props.getProperty("group.id") should equal(expectedGroupId)

    props.getProperty("batch.size") should equal("16384")
    props.getProperty("max.request.size") should equal("10485760")
    props.getProperty("compression.type") should equal("zstd")
    props.getProperty("transaction.timeout.ms") should equal("900000")
  }

  test("it should throw TopicNamingException for not starting with `ingest.`") {
    val invalidInputTopic = "porcupine-bonobo"
    val fakePassword = "fakePassW0rd"

    val caught = intercept[TopicNamingException] {
      new BaseValidationJob(invalidInputTopic,
        null, fakePassword, new TestValidator(), 100)
    }
    val exMsg = caught.getMessage
    exMsg should startWith("The Input Topic Name")
    exMsg should include (invalidInputTopic)
    exMsg should include ("It must start with \"ingest.\"")
  }

  test("it should throw TopicNamingException for not ending `.in`") {
    val invalidInputTopic = "ingest.porcupine-bonobo"
    val fakePassword = "fakePassW0rd"

    val caught = intercept[TopicNamingException] {
      new BaseValidationJob(invalidInputTopic,
        null, fakePassword, new TestValidator(), 100)
    }
    val exMsg = caught.getMessage
    exMsg should startWith("The Input Topic Name")
    exMsg should include (invalidInputTopic)
    exMsg should include ("It must end with \".in\"")
  }

  test("it should throw TopicNamingException for not having enough `.`") {
    val invalidInputTopic = "ingest.tenant.in"
    val fakePassword = "fakePassW0rd"

    val caught = intercept[TopicNamingException] {
      new BaseValidationJob(invalidInputTopic,
        null, fakePassword, new TestValidator(), 100)
    }
    val exMsg = caught.getMessage
    exMsg should startWith("The Input Topic Name")
    exMsg should include (invalidInputTopic)
    exMsg should include ("It must have 4 elements separated by a '.'")
  }

  test("getRecordCountSink() should return a HRI MgmtApiSink when calling the default constructor") {
    val baseUrl = "https://mydomain.com/hri"
    val clientId = "myClientId"
    val clientSecret = "myClientSecret"
    val audience = "myAudience"
    val oauthServiceBaseUrl = "https://oauthdomain.com/hri"
    val baseJob = new BaseValidationJob(TestInputTopic, TestBrokers, TestPassword, (_ => (true,null)) : Validator, baseUrl, clientId, clientSecret, audience, oauthServiceBaseUrl, 100)

    val sink = baseJob.getRecordCountSink(null)
    sink shouldBe a[MgmtApiSink]
    sink.asInstanceOf[MgmtApiSink].mgmtClient.baseUri should equal(baseUrl)
    sink.asInstanceOf[MgmtApiSink].mgmtClient.clientId should equal(clientId)
    sink.asInstanceOf[MgmtApiSink].mgmtClient.clientSecret should equal(clientSecret)
    sink.asInstanceOf[MgmtApiSink].mgmtClient.oauthServiceBaseUrl should equal(oauthServiceBaseUrl)
  }

  test("getRecordCountSink() should return a KafkaSink when calling the integration test constructor") {
    val baseUrl = "https://mydomain.com/hri"
    val apikey = "myApiKey"
    val baseJob = new BaseValidationJob(TestInputTopic, TestBrokers, TestPassword, (_ => (true,null)) : Validator, 100)

    val sink = baseJob.getRecordCountSink(
      BaseValidationJob.createKafkaProperties(TestBrokers, TestPassword, TestInputTopic, "outputTopic"))
    sink shouldBe a[FlinkKafkaProducer[_]]
  }

  test("gets optinal client if defined to construct test validation process function") {
    val notificationDescriptor = BaseValidationJob.NOTIFICATION_DESCRIPTOR
    val invalidOutputTag: OutputTag[InvalidRecord] = new OutputTag[InvalidRecord]("invalid")
    val countsOutputTag: OutputTag[ObjectNode] = new OutputTag[ObjectNode]("counts")
    val mgmtClient: BatchLookup = new MapBatchLookup(Seq())
    val baseJob = new BaseValidationJob((_ => (true,null)) : Validator, null, null, null, null, null, mgmtClient, 100)
    val validationProcessFunction = baseJob.getValidationProcessFunction(notificationDescriptor, invalidOutputTag, countsOutputTag)
    validationProcessFunction shouldBe a[TestValidationProcessFunction]
  }

  test("constructs validation process function with mgmt client using url and key") {
    val notificationDescriptor = BaseValidationJob.NOTIFICATION_DESCRIPTOR
    val invalidOutputTag: OutputTag[InvalidRecord] = new OutputTag[InvalidRecord]("invalid")
    val countsOutputTag: OutputTag[ObjectNode] = new OutputTag[ObjectNode]("counts")
    val baseJob = new BaseValidationJob(TestInputTopic, TestBrokers, TestPassword, (_ => (true,null)) : Validator, "mgmtApiUrl", "mgmtClientId", "mgmtClientSecret", "audience", "https://oauthdomain.com/hri", 100)
    val validationProcessFunction = baseJob.getValidationProcessFunction(notificationDescriptor, invalidOutputTag, countsOutputTag)
    assert(validationProcessFunction.getUseMgmtApi())
  }

  test("constructs validation process function without mgmt client") {
    val notificationDescriptor = BaseValidationJob.NOTIFICATION_DESCRIPTOR
    val invalidOutputTag: OutputTag[InvalidRecord] = new OutputTag[InvalidRecord]("invalid")
    val countsOutputTag: OutputTag[ObjectNode] = new OutputTag[ObjectNode]("counts")
    val baseJob = new BaseValidationJob(TestInputTopic, TestBrokers, TestPassword, (_ => (true,null)) : Validator, 100)
    val validationProcessFunction = baseJob.getValidationProcessFunction(notificationDescriptor, invalidOutputTag, countsOutputTag)
    assert(!validationProcessFunction.getUseMgmtApi())
  }
}
