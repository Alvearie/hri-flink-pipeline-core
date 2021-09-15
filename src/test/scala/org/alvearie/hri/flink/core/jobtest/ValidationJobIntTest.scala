/**
 * (C) Copyright IBM Corp. 2021
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package org.alvearie.hri.flink.core.jobtest

import java.nio.charset.StandardCharsets
import java.util

import org.alvearie.hri.api.{BatchNotification, InvalidRecord, MapBatchLookup}
import org.alvearie.hri.flink.core.{BaseValidationJob, TestHelper}
import org.alvearie.hri.flink.core.serialization.{HriRecord, NotificationRecord}
import org.alvearie.hri.flink.core.jobtest.sources.{HriTestRecsSourceFunction, NotificationSourceFunction, TestRecordHeaders}
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.test.streaming.runtime.util.TestListResultSink
import org.scalatest.BeforeAndAfter
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers._
import org.apache.flink.test.util.MiniClusterWithClientResource
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration
import org.apache.kafka.common.header.{Header, Headers}

import scala.collection.mutable
import scala.collection.JavaConverters._

/*
 * An End-to-End "Integration" Test using ScalaTest of the full Validation Job functionality.
 * Uses the Flink Framework Unit Test support of the MiniClusterWithClientResource ->
 *   @See https://ci.apache.org/projects/flink/flink-docs-stable/dev/stream/testing.html#junit-rule-miniclusterwithclientresource)
 */
class ValidationJobIntTest extends AnyFlatSpec with BeforeAndAfter {


  private var SlotsPerTaskMgr = 2
  val flinkCluster = new MiniClusterWithClientResource(new MiniClusterResourceConfiguration.Builder()
    .setNumberSlotsPerTaskManager(SlotsPerTaskMgr)
    .setNumberTaskManagers(1)
    .build)

  private val DefaultJobNamePrefix = "End-to-End Validation Test"
  private val DefaultTestNotificationKey = "testNotification01"
  private val HriRecordKeyRoot = "hriRec"
  private val DefaultHriRecordKey = HriRecordKeyRoot + "01"
  private val HriRecTwoKey = HriRecordKeyRoot + "02"
  private val DefaultJsonParseErrorMsgStart = "com.fasterxml.jackson.core.JsonParseException: Unrecognized token"
  private val DefaultTopic = "defaultTopic"
  private val DefaultPartition = 1
  private val DefaultOffset = 1234L

  private val testValidator = new TestValidator()

  before {
    flinkCluster.before()
  }

  after {
    flinkCluster.after()
  }

  //**Happy Path Pipeline Test with 1 valid record & sendCompleted notification sent in after that
  "BaseValidationJob pipeline" should "send One Valid HriRecord to the valid records output stream with 2 valid records output (With 2 Task Slots) and receive one send Completed Notification" in {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(2)

    // Test Input sources (in place of Kafka Topic sources)
    val validRecsSink = new TestListResultSink[HriRecord]
    val invalidRecsSink = new TestListResultSink[InvalidRecord]
    val recCountSink = new TestListResultSink[NotificationRecord]

    val totalNumOfRecordsToSend = 1
    val expectedRecordCount = totalNumOfRecordsToSend * SlotsPerTaskMgr
    val testHeaders = TestHelper.getDefaultTestHeaders()
    val testNotifications = getTestNotifications(testHeaders, expectedRecordCount)
    val batchLookup = new MapBatchLookup(testNotifications.get.map(_.value))

    val notificationSrc = new NotificationSourceFunction(testNotifications, 400)
    val testHriRecords = getOneValidRHriRecords(testHeaders)
    val testRecsSrc = new HriTestRecsSourceFunction(testHriRecords, 200)

    val validationJob = new BaseValidationJob(testValidator, notificationSrc,
      testRecsSrc, validRecsSink, invalidRecsSink, recCountSink, batchLookup, 100)

    validationJob.startJob(DefaultJobNamePrefix)

    //verify Output Message in ValidRecs - 1 valid Rec
    val validRecsList = validRecsSink.getResult()
    verifyDefaultValidRecOutput(testHeaders, validRecsList, SlotsPerTaskMgr)

    //verify recCount updated
    val batchId = TestHelper.DefaultTestBatchId
    val countResultList = recCountSink.getResult()
    countResultList should have size 1
    val recCountNotification = countResultList.get(0)
    recCountNotification.value.getExpectedRecordCount() should equal (expectedRecordCount)
    recCountNotification.value.getActualRecordCount() should equal (expectedRecordCount)
    recCountNotification.value.getInvalidRecordCount() should equal (0)
    recCountNotification.value.getStatus should equal(BatchNotification.Status.COMPLETED)
    recCountNotification.value.getId should equal (batchId)

    val invalidRecsList = invalidRecsSink.getResult()
    invalidRecsList should have size 0
  }

  //**Happy Path Pipeline Test #2: sends in:
  //1. Start notification,
  //2. Sends in 1 valid record,
  //3. 1 Invalid Hri Rec
  //  NO other notifications!
  "(With 2 Task Slots)  BaseValidationJob pipeline" should "send 2 valid & 2 invalid HriRecord to the corresponding valid & invalid output sinks" in {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(2)

    // Test Input sources (in place of Kafka Topic sources)
    val validRecsSink = new TestListResultSink[HriRecord]
    val invalidRecsSink = new TestListResultSink[InvalidRecord]
    val recCountSink = new TestListResultSink[NotificationRecord]

    val testHeaders = TestHelper.getDefaultTestHeaders()
    val startedNotification = TestHelper.createTestNotification(testHeaders, DefaultTestNotificationKey.getBytes,
      TestHelper.DefaultTestBatchId, 2, BatchNotification.Status.STARTED)
    val testNotifications = Some(Seq(startedNotification))
    val batchLookup = new MapBatchLookup(Seq(startedNotification.value))
    val notificationSrc = new NotificationSourceFunction(testNotifications)
    val testHriRecords = getOneValidOneInvalidHriRecords(testHeaders, HriRecTwoKey)
    val testRecsSrc = new HriTestRecsSourceFunction(testHriRecords)

    val validationJob = new BaseValidationJob(testValidator, notificationSrc,
      testRecsSrc, validRecsSink, invalidRecsSink, recCountSink, batchLookup, 100)

    validationJob.startJob(DefaultJobNamePrefix)

    //NOTE: ExpectedValidRecOutputCount = 1 valid Rec sent * # of SlotsPeTaskManager
    val validRecsList = validRecsSink.getResult
    verifyDefaultValidRecOutput(testHeaders, validRecsList, SlotsPerTaskMgr)

    val invalidRecsList = invalidRecsSink.getResult
    val invalidRecsSent = 1
    val expectedInvalidOutputCount = invalidRecsSent * SlotsPerTaskMgr
    invalidRecsList should have size expectedInvalidOutputCount

    val invalidRecord = invalidRecsList.get(0)
    val sentInvalidRecord = testHriRecords.get(1)
    invalidRecord.getBatchId should be (extractBatchId(sentInvalidRecord.headers))
    invalidRecord.getTopic should equal(sentInvalidRecord.topic)
    invalidRecord.getFailure should startWith(DefaultJsonParseErrorMsgStart)

    //record Count Notification output Sink should be empty - no messages
    val countResultList = recCountSink.getResult
    countResultList should have size 0

  }

  //send in Start notification and 3 invalid input records (0 valid records)
  "(With 2 Task Slots) BaseValidationJob pipeline" should "send (3) Failed Batch notification message to recordCount Output for 6 Invalid input records" in {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(2)

    val invalidRecordsSendCount = 3
    val expectedOutputRecCount = invalidRecordsSendCount * SlotsPerTaskMgr
    val failedBatchThreshold = 6

    // Test Input sources (in place of Kafka Topic sources)
    val validRecsSink = new TestListResultSink[HriRecord]
    val invalidRecsSink = new TestListResultSink[InvalidRecord]
    val recCountSink = new TestListResultSink[NotificationRecord]

    val testHeaders = TestHelper.getDefaultTestHeaders()
    val startedNotification = TestHelper.createTestNotification(testHeaders, DefaultTestNotificationKey.getBytes,
      TestHelper.DefaultTestBatchId, expectedOutputRecCount, BatchNotification.Status.STARTED, failedBatchThreshold)
    val testNotifications = Some(Seq(startedNotification))
    val batchLookup = new MapBatchLookup(Seq(startedNotification.value))
    val notificationSrc = new NotificationSourceFunction(testNotifications)

    val keyThree = "hriRec03"
    val testHriRecs = getThreeInvalidHriRecords(testHeaders, HriRecTwoKey,
      keyThree)
    val testRecsSrc = new HriTestRecsSourceFunction(testHriRecs, 150L)

    val validationJob = new BaseValidationJob(testValidator, notificationSrc,
      testRecsSrc, validRecsSink, invalidRecsSink, recCountSink, batchLookup, 100)

    validationJob.startJob(DefaultJobNamePrefix)
    val validRecsList = validRecsSink.getResult
    validRecsList should have size 0

    //verify recordCount (notification msg) output
    val batchId = TestHelper.DefaultTestBatchId
    val countResultList = recCountSink.getResult
    countResultList should have size 1
    val recCountNotification = countResultList.get(0)
    recCountNotification.value.getExpectedRecordCount() should equal (expectedOutputRecCount)
    recCountNotification.value.getActualRecordCount() should equal (expectedOutputRecCount)
    recCountNotification.value.getInvalidRecordCount() should equal (failedBatchThreshold)
    recCountNotification.value.getId should equal (batchId)
    recCountNotification.value.getStatus should equal(BatchNotification.Status.FAILED)

    //verify invalid recs output
    val invalidRecsList = invalidRecsSink.getResult
    invalidRecsList should have size expectedOutputRecCount

    for (index <- 0 to 2) {
      val invalidRecord = invalidRecsList.get(index)
      val sentInvalidRecord = testHriRecs.get(index)
      invalidRecord.getBatchId should be (extractBatchId(sentInvalidRecord.headers))
      invalidRecord.getTopic should equal(sentInvalidRecord.topic)
      invalidRecord.getFailure should startWith(DefaultJsonParseErrorMsgStart)
    }

    // Verify InvalidRecord failure message
    invalidRecsList.get(0).getFailure should startWith(DefaultJsonParseErrorMsgStart)
    invalidRecsList.get(2).getFailure should startWith(DefaultJsonParseErrorMsgStart)
    invalidRecsList.get(4).getFailure should startWith(DefaultJsonParseErrorMsgStart)
  }

  //1. send in Start notification,
  //2. send in 2 valid HRI Recs, Pause
  //3. then send batchTerminated message; pause then send another valid Rec
  "(With 2 Task Slots) BaseValidationJob pipeline" should "ignore processing a valid HRI Record after Terminated notification message" in {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(2)

    // Test Input sources (in place of Kafka Topic sources)
    val validRecsSink = new TestListResultSink[HriRecord]
    val invalidRecsSink = new TestListResultSink[InvalidRecord]
    val recCountSink = new TestListResultSink[NotificationRecord]

    val hriRecsToSendCount = 2
    val expectedOutputRecCount = hriRecsToSendCount * SlotsPerTaskMgr
    val defaultTestHeaders = TestHelper.getDefaultTestHeaders()
    val notificationKey = DefaultTestNotificationKey.getBytes
    val startedNotification = TestHelper.createTestNotification(defaultTestHeaders,
      notificationKey, TestHelper.DefaultTestBatchId, expectedOutputRecCount, BatchNotification.Status.STARTED)
    val terminateNotification = TestHelper.createTestNotification(defaultTestHeaders,
      notificationKey, TestHelper.DefaultTestBatchId, expectedOutputRecCount, BatchNotification.Status.TERMINATED)
    val testNotifications = Some(Seq(startedNotification, terminateNotification))

    // only passing in started so values in map aren't overwritten
    val batchLookup = new MapBatchLookup(Seq(startedNotification.value))

    val notificationSrc = new NotificationSourceFunction(testNotifications,125L, 0L)

    val twoHriRecs = getTwoValidHriRecords(defaultTestHeaders, HriRecTwoKey)
    //Add 3rd HRI Record msg - Delayed until after Terminated Notification msg sent
    val hriRecKeyThree = "hriRec03"
    val hriDelayTestHeaders = TestHelper.getTestHeadersWithDelayHeader(400L)
    val hriRecThree = TestHelper.createOneValidHriRecord(hriDelayTestHeaders,
      hriRecKeyThree.getBytes(StandardCharsets.UTF_8), TestHelper.ValidJsonTwo)
    val testHriRecords = twoHriRecs :+ hriRecThree
    testHriRecords should have size(3)
    val hriRecsOpt = Some(testHriRecords.toSeq)
    val testRecsSrc = new HriTestRecsSourceFunction(hriRecsOpt, 20)

    val validationJob = new BaseValidationJob(testValidator, notificationSrc,
      testRecsSrc, validRecsSink, invalidRecsSink, recCountSink, batchLookup, 100)

    validationJob.startJob(DefaultJobNamePrefix)

    //verify valid Recs
    val validRecsList = validRecsSink.getResult
    validRecsList should have size expectedOutputRecCount
    val validRec1 = validRecsList.get(0)
    val outputKey1 = (validRec1.key.map(_.toChar)).mkString
    val hriRecKeyMatcher = HriRecordKeyRoot + "0"
    outputKey1 should startWith(hriRecKeyMatcher)
    validRec1.headers.toArray should have size (defaultTestHeaders.toArray.size)
    val validRec2 = validRecsList.get(2)
    val outputKey2 = (validRec2.key.map(_.toChar)).mkString
    outputKey2 should startWith(hriRecKeyMatcher)
    val headersArr = validRec2.headers.toArray
    validRec2.headers.toArray should have size (defaultTestHeaders.toArray.size)
    verifyDefaultHeadersMatch(defaultTestHeaders.toArray.size, headersArr)

    //verify 0 invalid recs
    val invalidRecsList = invalidRecsSink.getResult
    invalidRecsList should have size 0

  }

  private def verifyDefaultValidRecOutput(testHeaders: TestRecordHeaders, validRecsList: util.List[HriRecord],
                                          slotsPerMgr: Int) = {
    val validRecsCount = 1
    validRecsList should have size (slotsPerMgr * validRecsCount)  //Record throughput is multiplied by slotsPerTaskManager
    val outputKey = (validRecsList.get(0).key.map(_.toChar)).mkString
    outputKey should equal(DefaultHriRecordKey)
    val headersArr = validRecsList.get(0).headers.toArray
    verifyDefaultHeadersMatch(testHeaders.toArray.size, headersArr)
  }

  private def verifyDefaultHeadersMatch(expectedHeadersSize: Int, headersArr: Array[Header]) = {
    headersArr should have size (expectedHeadersSize)
    headersArr.last.key() should equal("passThru")
    headersArr.last.value() should equal(TestHelper.PassThruHeaderVal.getBytes(StandardCharsets.UTF_8))
  }

  private def getTestNotifications(testHeaders: TestRecordHeaders, expectedRecordCount: Int): Option[Seq[NotificationRecord]] = {
    val testNotificationKey = DefaultTestNotificationKey
    val startedNotification = TestHelper.createTestNotification(testHeaders, testNotificationKey.getBytes,
      TestHelper.DefaultTestBatchId, expectedRecordCount, BatchNotification.Status.STARTED)
    val completedNotification = TestHelper.createTestNotification(testHeaders, testNotificationKey.getBytes,
      TestHelper.DefaultTestBatchId, expectedRecordCount, BatchNotification.Status.SEND_COMPLETED)
    Some(Seq(startedNotification, completedNotification))
  }

  private def getOneValidRHriRecords(testHeaders: TestRecordHeaders): Option[Seq[HriRecord]] = {
    val oneHriRec = TestHelper.createOneValidHriRecord(testHeaders,
      DefaultHriRecordKey.getBytes(StandardCharsets.UTF_8), TestHelper.ValidJsonOne)
    Some(Seq(oneHriRec))
  }

  private def getTwoValidHriRecords(testHeaders: TestRecordHeaders, recTwoKey:String): mutable.Seq[HriRecord] = {

    val hriRecOne = TestHelper.createOneValidHriRecord(testHeaders,
      DefaultHriRecordKey.getBytes(StandardCharsets.UTF_8), TestHelper.ValidJsonOne)
    val hriRecTwo = TestHelper.createOneValidHriRecord(testHeaders,
      recTwoKey.getBytes(StandardCharsets.UTF_8), TestHelper.ValidJsonTwo)
    collection.mutable.Seq(hriRecOne, hriRecTwo)
  }

  private def getOneValidOneInvalidHriRecords(testHeaders: TestRecordHeaders, key2Str: String): Option[Seq[HriRecord]] = {
    val key = DefaultHriRecordKey.getBytes(StandardCharsets.UTF_8)
    val validHriRec = TestHelper.createOneValidHriRecord(testHeaders, key, TestHelper.ValidJsonOne)
    val key2 = key2Str.getBytes(StandardCharsets.UTF_8)
    val invalidHriRec = new HriRecord(testHeaders, key2,
      TestHelper.InvalidHriBody1.getBytes(StandardCharsets.UTF_8), DefaultTopic, DefaultPartition, DefaultOffset)
    Some(Seq(validHriRec, invalidHriRec))
  }

  private def getThreeInvalidHriRecords(testHeaders: TestRecordHeaders, key2Str: String,
                                       key3Str: String):Option[Seq[HriRecord]] = {
    val key1 = DefaultHriRecordKey.getBytes(StandardCharsets.UTF_8)
    val key2 = key2Str.getBytes(StandardCharsets.UTF_8)
    val key3 = key3Str.getBytes(StandardCharsets.UTF_8)

    val invalidHriRec1 = new HriRecord(testHeaders, key1,
      TestHelper.InvalidHriBody1.getBytes(StandardCharsets.UTF_8), DefaultTopic, DefaultPartition, DefaultOffset)
    val invalidHriRec2 = new HriRecord(testHeaders, key2,
      TestHelper.InvalidHriBody2.getBytes(StandardCharsets.UTF_8), DefaultTopic, DefaultPartition, DefaultOffset)
    val invalidHriRec3 = new HriRecord(testHeaders, key3,
      TestHelper.InvalidHriBody3.getBytes(StandardCharsets.UTF_8), DefaultTopic, DefaultPartition, DefaultOffset)

    Some(Seq(invalidHriRec1, invalidHriRec2, invalidHriRec3))
  }

  def extractBatchId(headers: Headers): String = {
    if (headers != null) return headers.asScala.find(_.key() == "batchId")
      .map(h => new String(h.value(), StandardCharsets.UTF_8))
      .getOrElse(null)
    null
  }

}
