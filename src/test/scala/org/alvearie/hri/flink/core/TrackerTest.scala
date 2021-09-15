/**
 * (C) Copyright IBM Corp. 2021
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package org.alvearie.hri.flink.core

import java.time.OffsetDateTime

import com.fasterxml.jackson.databind.node.{JsonNodeFactory, ObjectNode}
import org.alvearie.hri.api.BatchNotification
import org.alvearie.hri.flink.core.BaseValidationJob.getStateTtlConfig
import org.alvearie.hri.flink.core.serialization.NotificationRecord
import org.apache.flink.api.common.state.ValueStateDescriptor
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.functions.KeySelector
import org.apache.flink.streaming.util.ProcessFunctionTestHarnesses
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers._

class TrackerTest extends AnyFunSuite{

  private val DefaultTestBatchId = "7902"
  private val DefaultBatchName = "Some-Test-Batch120"
  private val DefaultDataType = "claims"
  private val DefaultTopic = "ingest.1.in"
  private val DefaultStartDate = "2020-05-05T21:02:23Z"
  private val DefaultEndDate = "2020-05-06T13:02:44Z"
  private val keySelector1:KeySelector[ObjectNode, String] = (o: ObjectNode) => o.get("batchId").asText()
  private val keySelector2:KeySelector[NotificationRecord, String] = (o: NotificationRecord) => o.value.getId
  private val BatchDescriptor = new ValueStateDescriptor[BatchState](
    "batchStatus",
    TypeInformation.of(classOf[BatchState])
  )

  BatchDescriptor.enableTimeToLive(getStateTtlConfig)

  val timeout: Long = 300000

  test("it should update Valid State count for valid inputs") {
    val isValid = true
    val testNode = createTestNodeWithBatchId(isValid)

    val tracker: Tracker = new Tracker()
    val testHarness = ProcessFunctionTestHarnesses
                      .forKeyedCoProcessFunction(tracker, keySelector1, keySelector2, TypeInformation.of(classOf[String]))

    testHarness.processElement1(testNode, 1)
    testHarness.processElement1(testNode, 3)
    testHarness.processElement1(testNode, 5)

    val testOutput = testHarness.extractOutputValues()
    testOutput should have size 0

    val batchStatus = tracker.getRuntimeContext.getState(BatchDescriptor)
    val validCount = batchStatus.value().valid
    val invalidCount = batchStatus.value().invalid
    validCount should equal (3)
    invalidCount should equal (0)
  }

  test("it should update State count for only invalid inputs") {
    val isValid = false
    val testNode = createTestNodeWithBatchId(isValid)

    val tracker: Tracker = new Tracker()
    val testHarness = ProcessFunctionTestHarnesses
                      .forKeyedCoProcessFunction(tracker, keySelector1, keySelector2, TypeInformation.of(classOf[String]))
    testHarness.processElement1(testNode, 2)
    testHarness.processElement1(testNode, 5)

    val batchStatus = tracker.getRuntimeContext.getState(BatchDescriptor)
    val validCount = batchStatus.value().valid
    val invalidCount = batchStatus.value().invalid
    validCount should equal (0)
    invalidCount should equal (2)
  }

  test("it should update State count for Both valid & invalid inputs") {
    val validTestNode = createTestNodeWithBatchId(true)
    val invalidTestNode = createTestNodeWithBatchId(false)

    val tracker: Tracker = new Tracker()
    val testHarness = ProcessFunctionTestHarnesses
                      .forKeyedCoProcessFunction(tracker, keySelector1, keySelector2, TypeInformation.of(classOf[String]))
    testHarness.processElement1(validTestNode, 1)
    testHarness.processElement1(invalidTestNode, 3)
    testHarness.processElement1(validTestNode, 5)
    testHarness.processElement1(invalidTestNode, 7)
    testHarness.processElement1(invalidTestNode, 9)
    testHarness.processElement1(invalidTestNode, 12)

    val testOutput = testHarness.extractOutputValues()
    testOutput should have size 0

    val batchStatus = tracker.getRuntimeContext.getState(BatchDescriptor)
    val validCount = batchStatus.value().valid
    val invalidCount = batchStatus.value().invalid
    validCount should equal (2)
    invalidCount should equal (4)
  }

  test("For invalid inputs where eq Max invalid threshold, it should send failed notification message") {
    val invalidTestNode = createTestNodeWithBatchId(false)

    val tracker: Tracker = new Tracker()
    val testHarness = ProcessFunctionTestHarnesses
                      .forKeyedCoProcessFunction(tracker, keySelector1, keySelector2, TypeInformation.of(classOf[String]))

    val expectedRecordCount = 100
    val notificationTestMsg = createTestBatchNotificationObject(DefaultTestBatchId,
                              expectedRecordCount, BatchNotification.Status.SEND_COMPLETED)
    testHarness.processElement2(new NotificationRecord(null, null, notificationTestMsg), 20)


    testHarness.processElement1(invalidTestNode, 1)
    testHarness.processElement1(invalidTestNode, 3)
    testHarness.processElement1(invalidTestNode, 5)
    testHarness.processElement1(invalidTestNode, 8)
    testHarness.processElement1(invalidTestNode, 11)

    val batchStatus = tracker.getRuntimeContext.getState(BatchDescriptor)
    val validCount = batchStatus.value().valid
    val invalidCount = batchStatus.value().invalid
    validCount should equal (0)
    invalidCount should equal (5)

    val testOutputVals = testHarness.extractOutputValues()
    testOutputVals should have size 1
    val outputMsg = testOutputVals.get(0).value
    validateExpectedNotificationOutput(outputMsg, BatchNotification.Status.FAILED, expectedRecordCount, 5, 5,
      s"Failing Batch: ${DefaultTestBatchId}, too many invalid records invalidCount: ${invalidCount} == invalidThreshold: 5")
  }

  test("For invalid inputs greater than Max invalid threshold, it should send only one failed notification message") {
    val invalidTestNode = createTestNodeWithBatchId(false)

    val tracker: Tracker = new Tracker()
    val testHarness = ProcessFunctionTestHarnesses
                      .forKeyedCoProcessFunction(tracker, keySelector1, keySelector2, TypeInformation.of(classOf[String]))

    val expectedRecordCount = 10
    val notificationTestMsg = createTestBatchNotificationObject(DefaultTestBatchId,
                              expectedRecordCount, BatchNotification.Status.SEND_COMPLETED)
    testHarness.processElement2(new NotificationRecord(null, null, notificationTestMsg), 20)


    val count = 1
    for (count <- count to expectedRecordCount){
      testHarness.processElement1(invalidTestNode, count * 10)
    }

    val batchStatus = tracker.getRuntimeContext.getState(BatchDescriptor)
    val validCount = batchStatus.value().valid
    val invalidCount = batchStatus.value().invalid
    val failed = batchStatus.value().isFinished
    validCount should equal (0)
    invalidCount should equal (expectedRecordCount)
    failed should equal (true)

    val testOutputVals = testHarness.extractOutputValues()
    testOutputVals should have size 1
    val outputMsg = testOutputVals.get(0).value
    // Note the notification is produced when the invalid threshold is reached, so the actual record count equal the
    // threshold at that point in time in this test case
    validateExpectedNotificationOutput(outputMsg, BatchNotification.Status.FAILED, expectedRecordCount, 5, 5,
      s"Failing Batch: ${DefaultTestBatchId}, too many invalid records invalidCount: 5 == invalidThreshold: 5")
  }

  test("When Processor has status sendCompleted And expectedCount == totalProcessedCount, " +
    "processElement1 should send BatchComplete notification message") {

    val tracker: Tracker = new Tracker()
    val testHarness = ProcessFunctionTestHarnesses
                      .forKeyedCoProcessFunction(tracker, keySelector1, keySelector2, TypeInformation.of(classOf[String]))

    val expectedRecordCount = 12
    val notificationTestMsg = createTestBatchNotificationObject(DefaultTestBatchId,
                              expectedRecordCount, BatchNotification.Status.SEND_COMPLETED)
    testHarness.processElement2(new NotificationRecord(null, null, notificationTestMsg), 20)

    //8 calls with isValid = true; 3 calls isValid = false
    val validTestNode = createTestNodeWithBatchId(true)
    val invalidTestNode = createTestNodeWithBatchId(false)
    testHarness.processElement1(validTestNode, 1)
    testHarness.processElement1(validTestNode, 3)
    testHarness.processElement1(validTestNode, 4)
    testHarness.processElement1(validTestNode, 6)
    testHarness.processElement1(validTestNode, 9)
    testHarness.processElement1(validTestNode, 11)
    testHarness.processElement1(validTestNode, 14)
    testHarness.processElement1(validTestNode, 16)

    testHarness.processElement1(invalidTestNode, 30)
    testHarness.processElement1(invalidTestNode, 35)
    testHarness.processElement1(invalidTestNode, 37)

    //final call with valid = true (8 + 3 + 1 = 12)
    testHarness.processElement1(validTestNode, 40)

    //trigger processing time timer by advancing the processing time of the operator directly
    testHarness.setProcessingTime(timeout + 500)

    val testOutputVals = testHarness.extractOutputValues()
    testOutputVals should have size 1
    val outputMsg = testOutputVals.get(0).value
    validateExpectedNotificationOutput(outputMsg, BatchNotification.Status.COMPLETED, expectedRecordCount, expectedRecordCount, 3, null)
    outputMsg.getDataType should equal(DefaultDataType)
  }

  test("When Processor has status sendComplete And expectedCount == totalProcessedCount, " +
    "processElement2 should send BatchComplete notification message") {

    val tracker: Tracker = new Tracker()
    val testHarness = ProcessFunctionTestHarnesses
                      .forKeyedCoProcessFunction(tracker, keySelector1, keySelector2, TypeInformation.of(classOf[String]))

    //Add the values to Valid + invalid Counts
    val expectedRecordCount = 8
    val validTestNode = createTestNodeWithBatchId(true)
    val invalidTestNode = createTestNodeWithBatchId(false)
    var count = 1
    for (count <- count to expectedRecordCount - 2){
      testHarness.processElement1(validTestNode, count + 2)
    }
    testHarness.processElement1(invalidTestNode, 15)
    testHarness.processElement1(invalidTestNode, 18)

    val notificationTestMsg = createTestBatchNotificationObject(DefaultTestBatchId,
      expectedRecordCount, BatchNotification.Status.SEND_COMPLETED)
    testHarness.processElement2(new NotificationRecord(null, null, notificationTestMsg), 20)

    //trigger processing time timer by advancing the processing time of the operator directly
    testHarness.setProcessingTime(timeout + 500)

    val testOutputVals = testHarness.extractOutputValues()
    testOutputVals should have size 1
    val outputMsg = testOutputVals.get(0).value
    validateExpectedNotificationOutput(outputMsg, BatchNotification.Status.COMPLETED, expectedRecordCount, expectedRecordCount, 2, null)
    outputMsg.getDataType should equal(DefaultDataType)
  }

  test("When Processor status == sendComplete But expectedCount < totalProcessedCount, " +
    "BatchComplete Notification msg should Not be sent") {

    val tracker: Tracker = new Tracker()
    val testHarness = ProcessFunctionTestHarnesses
                      .forKeyedCoProcessFunction(tracker, keySelector1, keySelector2, TypeInformation.of(classOf[String]))

    val expectedRecordCount = 100
    val notificationTestMsg = createTestBatchNotificationObject(DefaultTestBatchId,
                              expectedRecordCount, BatchNotification.Status.SEND_COMPLETED)
    testHarness.processElement2(new NotificationRecord(null, null, notificationTestMsg), 20)

    //Send in 6 total records (With Only 1 Invalid)
    val validTestNode = createTestNodeWithBatchId(true)
    val invalidTestNode = createTestNodeWithBatchId(false)
    var count = 1
    for (count <- count to 5){
      testHarness.processElement1(validTestNode, count + 2)
    }
    testHarness.processElement1(invalidTestNode, 15)

    //Expectation = No Output Msg sent from processor
    val testOutputVals = testHarness.extractOutputValues()
    testOutputVals.isEmpty should be (true)
  }

  test("When Processor status == sendComplete, But expectedCount < totalProcessedCount, " +
    "processElement2 should Not output Notification msg") {

    val tracker: Tracker = new Tracker()
    val testHarness = ProcessFunctionTestHarnesses
                      .forKeyedCoProcessFunction(tracker, keySelector1, keySelector2, TypeInformation.of(classOf[String]))

    val expectedRecordCount = 50
    //Send in 8 total records (With Only 1 Invalid)
    val validTestNode = createTestNodeWithBatchId(true)
    val invalidTestNode = createTestNodeWithBatchId(false)
    var count = 1
    for (count <- count to 7){
      testHarness.processElement1(validTestNode, count + 2)
    }
    testHarness.processElement1(invalidTestNode, 20)

    val notificationTestMsg = createTestBatchNotificationObject(DefaultTestBatchId,
      expectedRecordCount, BatchNotification.Status.SEND_COMPLETED)
    testHarness.processElement2(new NotificationRecord(null, null, notificationTestMsg), 20)
    //Expectation = No Output Msg sent from processor
    val testOutputVals = testHarness.extractOutputValues()
    testOutputVals.isEmpty should be (true)
  }

  test("After the currentNotification is Serialized/Deserialized, newNotification should be a deep copy from the original one") {
    val tracker: Tracker = new Tracker()
    val expectedRecordCount = 50
    val batchNotification = createTestBatchNotificationObject(DefaultTestBatchId,
      expectedRecordCount, BatchNotification.Status.SEND_COMPLETED)

    val notification = tracker.createNotificationMsg(batchNotification, BatchNotification.Status.SEND_COMPLETED, expectedRecordCount, 0, null)
    validateExpectedNotificationOutput(notification.value, BatchNotification.Status.SEND_COMPLETED, expectedRecordCount, expectedRecordCount, 0, null)
  }

  test("when Batch notification is null, Notification record should be null as well") {
    val tracker: Tracker = new Tracker()
    val expectedRecordCount = 50
    val batchNotification = null

    val notification = tracker.createNotificationMsg(batchNotification, BatchNotification.Status.SEND_COMPLETED, expectedRecordCount, 0, null)
    notification should equal(null)
  }

  test("When Processor status == sendComplete But receive more records than expected, " +
    "processElement1 should send only one Fail notification message") {
    val tracker: Tracker = new Tracker()
    val testHarness = ProcessFunctionTestHarnesses
                      .forKeyedCoProcessFunction(tracker, keySelector1, keySelector2, TypeInformation.of(classOf[String]))

    val expectedRecordCount = 100
    val notificationTestMsg = createTestBatchNotificationObject(DefaultTestBatchId,
                              expectedRecordCount, BatchNotification.Status.SEND_COMPLETED)
    testHarness.processElement2(new NotificationRecord(null, null, notificationTestMsg), 20)

    //Sending 102 records (With Only 2 Invalid)
    val validTestNode = createTestNodeWithBatchId(true)
    val invalidTestNode = createTestNodeWithBatchId(false)
    val count = 1
    for (count <- count to expectedRecordCount){
      testHarness.processElement1(validTestNode, count + 2)
    }
    // Two extra records
    testHarness.processElement1(invalidTestNode, 15)
    testHarness.processElement1(invalidTestNode, 15)

    //trigger processing time timer by advancing the processing time of the operator directly
    testHarness.setProcessingTime(timeout + 500)

    val testOutputVals = testHarness.extractOutputValues()
    // the expectations would be getting only one FAILED notification message
    // when receiving more records than expected BEFORE the scheduled timeout reaches the specified time
    testOutputVals should have size 1
    val outputMsg = testOutputVals.get(0).value
    validateExpectedNotificationOutput(outputMsg, BatchNotification.Status.FAILED, expectedRecordCount, expectedRecordCount+1, 1,
      s"Failing batch: ${DefaultTestBatchId}, received too many records, actualRecordCount: ${expectedRecordCount+1} > expectedRecordCount: ${expectedRecordCount}")
  }

  test("When Processor status == sendComplete But receive more records than expected, " +
    "processElement2 should send Fail notification message") {
    val tracker: Tracker = new Tracker()
    val testHarness = ProcessFunctionTestHarnesses
                      .forKeyedCoProcessFunction(tracker, keySelector1, keySelector2, TypeInformation.of(classOf[String]))

    val expectedRecordCount = 15

    //Sending 16 records (With Only 1 Invalid)
    val validTestNode = createTestNodeWithBatchId(true)
    val invalidTestNode = createTestNodeWithBatchId(false)
    val count = 1
    for (count <- count to expectedRecordCount){
      testHarness.processElement1(validTestNode, count + 2)
    }
    testHarness.processElement1(invalidTestNode, count + 2)

    val notificationTestMsg = createTestBatchNotificationObject(DefaultTestBatchId,
      expectedRecordCount, BatchNotification.Status.SEND_COMPLETED)
    testHarness.processElement2(new NotificationRecord(null, null, notificationTestMsg), 20)

    val testOutputVals = testHarness.extractOutputValues()
    testOutputVals should have size 1
    val outputMsg = testOutputVals.get(0).value
    validateExpectedNotificationOutput(outputMsg, BatchNotification.Status.FAILED, expectedRecordCount, expectedRecordCount+1, 1,
    s"Failing batch: ${DefaultTestBatchId}, received too many records, actualRecordCount: ${expectedRecordCount+1} > expectedRecordCount: ${expectedRecordCount}")
  }

  test("when sending a notification message with BatchNotification.Status -> SEND_COMPLETED" +
    " to the processElement2, test should run without any NPE") {
    val tracker: Tracker = new Tracker()
    val testHarness = ProcessFunctionTestHarnesses
                      .forKeyedCoProcessFunction(tracker, keySelector1, keySelector2, TypeInformation.of(classOf[String]))

    val expectedRecordCount = 2

    val notificationTestMsg = createTestBatchNotificationObject(DefaultTestBatchId,
      expectedRecordCount, BatchNotification.Status.STARTED)
    val notificationTestMsg2 = createTestBatchNotificationObject(DefaultTestBatchId,
      expectedRecordCount, BatchNotification.Status.SEND_COMPLETED)

    testHarness.processElement2(new NotificationRecord(null, null, notificationTestMsg), 1)
    testHarness.processElement2(new NotificationRecord(null, null, notificationTestMsg2), 1)
    val testOutputVals = testHarness.extractOutputValues()
    testOutputVals.isEmpty should be (true)
  }

  test("When sending messages with 2 different batchIds, then SEND_COMPLETED msg for one of them" +
    "with the expected record count, Only the correct batch is completed when processing SEND_COMPLETED notifications") {

    val tracker: Tracker = new Tracker()
    val testHarness = ProcessFunctionTestHarnesses
                      .forKeyedCoProcessFunction(tracker, keySelector1, keySelector2, TypeInformation.of(classOf[String]))

    val secondBatch = "secondBatch"
    val expectedRecordCount = 10
    val notificationTestMsg1 = createTestBatchNotificationObject(DefaultTestBatchId,
      expectedRecordCount, BatchNotification.Status.STARTED)
    val notificationTestMsg2 = createTestBatchNotificationObject(secondBatch,
      expectedRecordCount, BatchNotification.Status.STARTED)

    testHarness.processElement2(new NotificationRecord(null, null, notificationTestMsg1), 20)
    testHarness.processElement2(new NotificationRecord(null, null, notificationTestMsg2), 20)

    //shipping 10 valid records for 2 batches, 20 in all
    val validTestNode = createTestNodeWithBatchId(true)
    val validTestNode2ndBatch = createTestNodeWithBatchId(isValid = true, secondBatch)

    val count = 1
    for (count <- count until (expectedRecordCount*10) by 10){
      testHarness.processElement1(validTestNode, count + 2)
      testHarness.processElement1(validTestNode2ndBatch, count + 5)
    }

    val notificationTestMsg3 = createTestBatchNotificationObject(DefaultTestBatchId,
      expectedRecordCount, BatchNotification.Status.SEND_COMPLETED)

    testHarness.processElement2(new NotificationRecord(null, null, notificationTestMsg3), 20)

    //trigger processing time timer by advancing the processing time of the operator directly
    testHarness.setProcessingTime(timeout + 500)

    val testOutputVals = testHarness.extractOutputValues()
    testOutputVals should have size 1
    val outputMsg = testOutputVals.get(0).value
    outputMsg.getId should equal (DefaultTestBatchId)
  }

  test("When Processor has status sendComplete And expectedCount == totalProcessedCount," +
    "Should wait a configured amount of time before send BatchComplete notification message") {

    val tracker: Tracker = new Tracker()
    val testHarness = ProcessFunctionTestHarnesses
                      .forKeyedCoProcessFunction(tracker, keySelector1, keySelector2, TypeInformation.of(classOf[String]))

    val expectedRecordCount = 100
    val notificationTestMsg = createTestBatchNotificationObject(DefaultTestBatchId,
                              expectedRecordCount, BatchNotification.Status.SEND_COMPLETED)
    testHarness.processElement2(new NotificationRecord(null, null, notificationTestMsg), 20)

    //98 records isValid = true; 2 isValid = false
    val validTestNode = createTestNodeWithBatchId(true)
    val invalidTestNode = createTestNodeWithBatchId(false)
    val count = 1
    for (count <- count to expectedRecordCount - 2) {
      testHarness.processElement1(validTestNode, count +3)
    }
    testHarness.processElement1(invalidTestNode, count + 4)
    testHarness.processElement1(invalidTestNode, count + 5)

    // verify that No Output values have been sent yet
    // when expected Record Count is equal to totalProcessedCount (happy path)
    // and the scheduled timeout hasn't reached the specified time
    var testOutputVals = testHarness.extractOutputValues()
    testOutputVals.isEmpty should be (true)

    //trigger processing time timer by advancing the processing time of the operator directly
    testHarness.setProcessingTime(timeout + 500)

    testOutputVals = testHarness.extractOutputValues()
    testOutputVals should have size (1)
    val outputMsg = testOutputVals.get(0).value
    validateExpectedNotificationOutput(outputMsg, BatchNotification.Status.COMPLETED, expectedRecordCount, expectedRecordCount, 2, null)
    outputMsg.getDataType should equal(DefaultDataType)
  }

  test("It should fail the batch when receiving more records than expected within the timeout") {
    val tracker: Tracker = new Tracker()
    val testHarness = ProcessFunctionTestHarnesses
                      .forKeyedCoProcessFunction(tracker, keySelector1, keySelector2, TypeInformation.of(classOf[String]))

    val expectedRecordCount = 100
    val notificationTestMsg = createTestBatchNotificationObject(DefaultTestBatchId,
                              expectedRecordCount, BatchNotification.Status.SEND_COMPLETED)
    testHarness.processElement2(new NotificationRecord(null, null, notificationTestMsg), 20)

    //Sending 101 records (1 invalid)
    val validTestNode = createTestNodeWithBatchId(true)
    val invalidTestNode = createTestNodeWithBatchId(false)
    val count = 1
    for (count <- count to expectedRecordCount){
      testHarness.processElement1(validTestNode, count + 2)
    }

    // extra record BEFORE the timeout is completed
    testHarness.processElement1(invalidTestNode, count + 15)

    // verify that output values have been already sent
    // when receiving extra records before the scheduled timeout reaches the specified time
    var testOutputVals = testHarness.extractOutputValues()
    testOutputVals should have size (1)

    //trigger processing time timer by advancing the processing time of the operator directly
    testHarness.setProcessingTime(timeout + 500)

    testOutputVals = testHarness.extractOutputValues()
    testOutputVals should have size (1)

    val outputMsg = testOutputVals.get(0).value
    validateExpectedNotificationOutput(outputMsg, BatchNotification.Status.FAILED, expectedRecordCount, expectedRecordCount+1, 1,
      s"Failing batch: ${DefaultTestBatchId}, received too many records, actualRecordCount: ${expectedRecordCount+1} > expectedRecordCount: ${expectedRecordCount}")
  }

  test("It should complete the batch when receiving more records than expected outside the timeout") {
    val tracker: Tracker = new Tracker()
    val testHarness = ProcessFunctionTestHarnesses
                      .forKeyedCoProcessFunction(tracker, keySelector1, keySelector2, TypeInformation.of(classOf[String]))

    val expectedRecordCount = 100
    val notificationTestMsg = createTestBatchNotificationObject(DefaultTestBatchId,
                              expectedRecordCount, BatchNotification.Status.SEND_COMPLETED)
    testHarness.processElement2(new NotificationRecord(null, null, notificationTestMsg), 20)

    //Sending 101 records (1 invalid)
    val validTestNode = createTestNodeWithBatchId(true)
    val invalidTestNode = createTestNodeWithBatchId(false)
    val count = 1
    for (count <- count to expectedRecordCount){
      testHarness.processElement1(validTestNode, count + 2)
    }

    // verify that No Output values have been sent yet
    // when expected Record Count is equal to totalProcessedCount
    // and the scheduled timeout hasn't reached the specified time
    var testOutputVals = testHarness.extractOutputValues()
    testOutputVals.isEmpty should be (true)

    //trigger processing time timer by advancing the processing time of the operator directly
    testHarness.setProcessingTime(timeout + 500)

    // any extra record should be ignored AFTER the timeout is completed
    testHarness.processElement1(invalidTestNode, count + 15)

    testOutputVals = testHarness.extractOutputValues()
    testOutputVals should have size 1

    val outputMsg = testOutputVals.get(0).value
    validateExpectedNotificationOutput(outputMsg, BatchNotification.Status.COMPLETED, expectedRecordCount, expectedRecordCount, 0, null)
  }

  test("When Processor has status sendComplete And expectedCount == totalProcessedCount, " +
    "processElement2 Should wait a configured amount of time before send BatchComplete notification message") {

    val tracker: Tracker = new Tracker()
    val testHarness = ProcessFunctionTestHarnesses
      .forKeyedCoProcessFunction(tracker, keySelector1, keySelector2, TypeInformation.of(classOf[String]))

    //Add the values to Valid + invalid Counts
    val expectedRecordCount = 100
    val validTestNode = createTestNodeWithBatchId(true)
    val invalidTestNode = createTestNodeWithBatchId(false)
    val count = 1
    for (count <- count to expectedRecordCount - 2){
      testHarness.processElement1(validTestNode, count + 2)
    }
    testHarness.processElement1(invalidTestNode, 15)
    testHarness.processElement1(invalidTestNode, 18)

    val notificationTestMsg = createTestBatchNotificationObject(DefaultTestBatchId,
      expectedRecordCount, BatchNotification.Status.SEND_COMPLETED)
    testHarness.processElement2(new NotificationRecord(null, null, notificationTestMsg), 20)

    // verify that No Output values have been sent yet
    // when expected Record Count is equal to totalProcessedCount (happy path)
    // and the scheduled timeout hasn't reached the specified time
    var testOutputVals = testHarness.extractOutputValues()
    testOutputVals.isEmpty should be (true)

    //trigger processing time timer by advancing the processing time of the operator directly
    testHarness.setProcessingTime(timeout + 500)

    testOutputVals = testHarness.extractOutputValues()
    testOutputVals should have size 1
    val outputMsg = testOutputVals.get(0).value
    validateExpectedNotificationOutput(outputMsg, BatchNotification.Status.COMPLETED, expectedRecordCount, expectedRecordCount, 2, null)
    outputMsg.getDataType should equal(DefaultDataType)
  }

  test("processElement2 should fail the batch when receiving more records than expected within the timeout") {

    val tracker: Tracker = new Tracker()
    val testHarness = ProcessFunctionTestHarnesses
      .forKeyedCoProcessFunction(tracker, keySelector1, keySelector2, TypeInformation.of(classOf[String]))

    //Sending 102 records (2 invalids)
    val expectedRecordCount = 100
    val validTestNode = createTestNodeWithBatchId(true)
    val invalidTestNode = createTestNodeWithBatchId(false)
    val count = 1
    for (count <- count to expectedRecordCount ){
      testHarness.processElement1(validTestNode, count + 2)
    }

    // two extra records BEFORE the timeout is completed
    testHarness.processElement1(invalidTestNode, 15)
    testHarness.processElement1(invalidTestNode, 18)

    val notificationTestMsg = createTestBatchNotificationObject(DefaultTestBatchId,
      expectedRecordCount, BatchNotification.Status.SEND_COMPLETED)
    testHarness.processElement2(new NotificationRecord(null, null, notificationTestMsg), 20)

    // verify that output values have been already sent
    // when receiving extra records before the scheduled timeout reaches the specified time
    var testOutputVals = testHarness.extractOutputValues()
    testOutputVals should have size (1)

    //trigger processing time timer by advancing the processing time of the operator directly
    testHarness.setProcessingTime(timeout + 500)

    testOutputVals = testHarness.extractOutputValues()
    testOutputVals should have size 1
    val outputMsg = testOutputVals.get(0).value
    validateExpectedNotificationOutput(outputMsg, BatchNotification.Status.FAILED, expectedRecordCount, expectedRecordCount+2, 2,
      s"Failing batch: ${DefaultTestBatchId}, received too many records, actualRecordCount: ${expectedRecordCount+2} > expectedRecordCount: ${expectedRecordCount}")
  }

  test("processElement2 should complete the batch when receiving more records than expected outside the timeout") {

    val tracker: Tracker = new Tracker()
    val testHarness = ProcessFunctionTestHarnesses
      .forKeyedCoProcessFunction(tracker, keySelector1, keySelector2, TypeInformation.of(classOf[String]))

    //Sending 102 records (2 invalids)
    val expectedRecordCount = 100
    val validTestNode = createTestNodeWithBatchId(true)
    val invalidTestNode = createTestNodeWithBatchId(false)
    val count = 1
    for (count <- count to expectedRecordCount ){
      testHarness.processElement1(validTestNode, count + 2)
    }

    val notificationTestMsg = createTestBatchNotificationObject(DefaultTestBatchId,
      expectedRecordCount, BatchNotification.Status.SEND_COMPLETED)
    testHarness.processElement2(new NotificationRecord(null, null, notificationTestMsg), 20)

    // verify that No Output values have been sent yet
    // when expected Record Count is equal to totalProcessedCount
    // and the scheduled timeout hasn't reached the specified time
    var testOutputVals = testHarness.extractOutputValues()
    testOutputVals.isEmpty should be (true)

    //trigger processing time timer by advancing the processing time of the operator directly
    testHarness.setProcessingTime(timeout + 500)

    // any extra record should be ignored AFTER the timeout is completed
    testHarness.processElement1(invalidTestNode, 15)
    testHarness.processElement1(invalidTestNode, 18)

    testOutputVals = testHarness.extractOutputValues()
    testOutputVals should have size 1

    val outputMsg = testOutputVals.get(0).value
    validateExpectedNotificationOutput(outputMsg, BatchNotification.Status.COMPLETED, expectedRecordCount, expectedRecordCount, 0, null)
    outputMsg.getDataType should equal(DefaultDataType)
  }

  def createTestNodeWithBatchId(isValid: Boolean): ObjectNode = {
    val factory:JsonNodeFactory = JsonNodeFactory.instance
    val root:ObjectNode = factory.objectNode()
    root.put("batchId", DefaultTestBatchId)
    root.put("isValid", isValid)
    root
  }

  def createTestNodeWithBatchId(isValid: Boolean, batchId: String): ObjectNode = {
    val factory:JsonNodeFactory = JsonNodeFactory.instance
    val root:ObjectNode = factory.objectNode()
    root.put("batchId", batchId)
    root.put("isValid", isValid)
    root
  }

  def createTestBatchNotificationObject(batchId: String, expectedRecCount: Int,
                                        status: BatchNotification.Status): BatchNotification = {
    return new BatchNotification()
      .withId(batchId)
      .withStatus(status)
      .withDataType(DefaultDataType)
      .withStartDate(OffsetDateTime.parse(DefaultStartDate))
      .withEndDate(OffsetDateTime.parse(DefaultEndDate))
      .withExpectedRecordCount(expectedRecCount)
      .withName(DefaultBatchName)
      .withTopic(DefaultTopic)
      .withInvalidThreshold(5)
  }

  def validateExpectedNotificationOutput(notification: BatchNotification,
                                         expectedStatus: BatchNotification.Status,
                                         expectedRecordCount: Int,
                                         actualRecordCount: Int,
                                         invalidRecordCount: Int,
                                         failureMessage: String): Unit = {
    notification.getId should equal(DefaultTestBatchId)
    notification.getExpectedRecordCount() should equal(expectedRecordCount)
    notification.getActualRecordCount() should equal(actualRecordCount)
    notification.getInvalidRecordCount() should equal(invalidRecordCount)
    notification.getStatus should equal(expectedStatus)
    notification.getTopic should equal(DefaultTopic)
    notification.getName should equal(DefaultBatchName)
    notification.getStartDate should equal(OffsetDateTime.parse(DefaultStartDate))
    notification.getEndDate should equal(OffsetDateTime.parse(DefaultEndDate))
    notification.getFailureMessage should equal(failureMessage)
  }
}
