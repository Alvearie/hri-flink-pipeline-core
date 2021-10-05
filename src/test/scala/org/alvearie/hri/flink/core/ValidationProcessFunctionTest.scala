/**
 * (C) Copyright IBM Corp. 2021
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package org.alvearie.hri.flink.core

import java.nio.charset.StandardCharsets
import java.time.temporal.ChronoUnit
import java.time.{OffsetDateTime, ZoneOffset}
import java.util
import java.util.concurrent.TimeUnit

import com.fasterxml.jackson.databind.node.ObjectNode
import org.alvearie.hri.api.{BatchNotification, InvalidRecord, MgmtClient, RequestException}
import org.alvearie.hri.flink.core.BaseValidationJob.getStateTtlConfig
import org.alvearie.hri.flink.core.serialization.{HriRecord, NotificationRecord}
import org.apache.flink.api.common.state.MapStateDescriptor
import org.apache.flink.api.common.typeinfo.{BasicTypeInfo, TypeInformation}
import org.apache.flink.streaming.api.scala.OutputTag
import org.apache.flink.streaming.util.{BroadcastOperatorTestHarness, ProcessFunctionTestHarnesses}
import org.apache.flink.util.FlinkException
import org.apache.http.HttpStatus
import org.apache.kafka.common.header.internals.RecordHeaders
import org.mockito.invocation.InvocationOnMock
import org.mockito.scalatest.MockitoSugar
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers._

import scala.collection.JavaConverters._
import scala.concurrent.duration.Duration
import scala.util.{Failure, Success}

class ValidationProcessFunctionTest extends AnyFunSuite with MockitoSugar {

  // these help Scala infer types
  implicit val hriRecordTypeInfo: TypeInformation[HriRecord] = TypeInformation.of(classOf[HriRecord])
  implicit val invalidRecordTypeInfo: TypeInformation[InvalidRecord] = TypeInformation.of(classOf[InvalidRecord])
  implicit val objectNodeTypeInfo: TypeInformation[ObjectNode] = TypeInformation.of(classOf[ObjectNode])

  val invalidOutputTag = OutputTag[InvalidRecord]("invalid")
  val countsOutputTag = OutputTag[ObjectNode]("counts")
  val DefaultTestBatchId = "338"
  val DefaultTestMsgBody = "msgBody"
  val DefaultTestInvalidMsgBody = "invalidMsgBody"
  val DefaultTestRecordCount = 4508
  private val DefaultTopic = "ingest.t1.data-int1.in"
  private val DefaultPartition = 1
  private val DefaultOffset = 1234L
  val tenantId = "myTenant"

  private val NotificationDescriptor = new MapStateDescriptor[String, BatchNotification](
    "NotificationsBroadcastState",
    BasicTypeInfo.STRING_TYPE_INFO,
    TypeInformation.of(classOf[BatchNotification])
  )

  NotificationDescriptor.enableTimeToLive(getStateTtlConfig)

  test("processElement should call getBatch, retry call HRI mgmtClient and return batchNotification") {
    val mockClient = mock[MgmtClient]

    val batchNotification = createTestBatchNotification(DefaultTestBatchId, BatchNotification.Status.SEND_COMPLETED, DefaultTestRecordCount)
    var count = 0
    when(mockClient.getBatchId(tenantId, DefaultTestBatchId))
      .thenAnswer( (_: InvocationOnMock) => {
        count += 1
        if (count < 5)
          Failure(new RequestException("Connection timeout", HttpStatus.SC_INTERNAL_SERVER_ERROR))
        else
          Success(batchNotification)
      })

    val validator = new TestValidationProcessFunction(
      NotificationDescriptor,
      invalidOutputTag,
      countsOutputTag,
      _ => (true,null),
      tenantId,
      mockClient
    )

    validator.initialBackOff = Duration.apply(1, TimeUnit.SECONDS)
    validator.maxBackOff = Duration.apply(3, TimeUnit.SECONDS)
    validator.maxRetry = java.time.Duration.ofSeconds(30)

    val testHarness: BroadcastOperatorTestHarness[HriRecord, NotificationRecord, HriRecord] =
      ProcessFunctionTestHarnesses.forBroadcastProcessFunction(validator, NotificationDescriptor)

    val testInput:HriRecord = createTesthriRecordInput()
    testHarness.processElement(testInput, 1)

    verify(mockClient, times(5)).getBatchId(tenantId, DefaultTestBatchId)
    val testOutput = testHarness.extractOutputValues()
    ensureActualOutputMatchesDefaultExpected(testOutput)
  }

  test("processElement should call getBatch but timeout on retry") {
    val mockClient = mock[MgmtClient]

    when(mockClient.getBatchId(tenantId, DefaultTestBatchId))thenReturn(Failure(new RequestException("Internal server error", HttpStatus.SC_INTERNAL_SERVER_ERROR)))

    val validator = new TestValidationProcessFunction(
      NotificationDescriptor,
      invalidOutputTag,
      countsOutputTag,
      _ => (true,null),
      tenantId,
      mockClient
    )

    validator.initialBackOff = Duration.apply(11, TimeUnit.MILLISECONDS)
    validator.maxBackOff = Duration.apply(11, TimeUnit.MILLISECONDS)
    validator.maxRetry = java.time.Duration.ofMillis(10)

    val testHarness: BroadcastOperatorTestHarness[HriRecord, NotificationRecord, HriRecord] =
      ProcessFunctionTestHarnesses.forBroadcastProcessFunction(validator, NotificationDescriptor)

    val testInput:HriRecord = createTesthriRecordInput()

    val thrown = intercept[FlinkException] {
      testHarness.processElement(testInput, 3)
    }
    if (thrown == null) {fail("expected exception not thrown")}
    thrown.getMessage should startWith("Reached max HRI Management API retry timeout of")
  }

  test("processElement should ship records to invalid output when the batch Id is not in the broadcast state and HRI mgmtClient responds with 'Not Found'") {
    val mockClient = mock[MgmtClient]

    val validator = new TestValidationProcessFunction(
      NotificationDescriptor,
      invalidOutputTag,
      countsOutputTag,
      _ => (true,null),
      tenantId,
      mockClient
    )

    when(mockClient.getBatchId(tenantId, DefaultTestBatchId)) thenReturn(Failure(new RequestException("request failure", HttpStatus.SC_NOT_FOUND)))

    val testHarness: BroadcastOperatorTestHarness[HriRecord, NotificationRecord, HriRecord] =
      ProcessFunctionTestHarnesses.forBroadcastProcessFunction(validator, NotificationDescriptor)

    val testInput:HriRecord = createTesthriRecordInput()
    testHarness.processElement(testInput, 3)

    val invalidOut = testHarness.getSideOutput(invalidOutputTag)
    invalidOut should have size 1

    val invalidRecord = invalidOut.element().getValue
    invalidRecord.getBatchId should be (validator.extractBatchId(testInput.headers))
    invalidRecord.getTopic should equal(testInput.topic)
    invalidRecord.getFailure should equal(validator.Unknown_Batch_ID_Err_Msg)

    verify(mockClient, times(1)).getBatchId(tenantId, DefaultTestBatchId)
  }

  test("processElement should call getBatch and unexpected error should be thrown from mgmtApi") {
    val mockClient = mock[MgmtClient]

    val validator = new TestValidationProcessFunction(
      NotificationDescriptor,
      invalidOutputTag,
      countsOutputTag,
      _ => (true,null),
      tenantId,
      mockClient
    )

    when(mockClient.getBatchId(tenantId, DefaultTestBatchId)) thenReturn(Failure(new Exception("unexpected error")))

    val testHarness: BroadcastOperatorTestHarness[HriRecord, NotificationRecord, HriRecord] =
      ProcessFunctionTestHarnesses.forBroadcastProcessFunction(validator, NotificationDescriptor)

    val testInput:HriRecord = createTesthriRecordInput()

    val thrown = intercept[FlinkException] {
      testHarness.processElement(testInput, 3)
    }
    if (thrown == null) {fail("expected exception not thrown")}
    thrown.getMessage should include("unexpected error")
  }

  test("It should send valid input JSON to output topic") {
    val validator = new ValidationProcessFunction(NotificationDescriptor, invalidOutputTag, countsOutputTag, _ => (true,null))
    val testHarness: BroadcastOperatorTestHarness[HriRecord, NotificationRecord, HriRecord] =
      ProcessFunctionTestHarnesses.forBroadcastProcessFunction(validator, NotificationDescriptor)

    val testBroadcastInput:NotificationRecord = createTestBroadcastInputNode(DefaultTestBatchId,
      BatchNotification.Status.SEND_COMPLETED, DefaultTestRecordCount)
    testHarness.processBroadcastElement(testBroadcastInput, 1)

    val testInput:HriRecord = createTesthriRecordInput()
    testHarness.processElement(testInput, 1)

    val testOutput = testHarness.extractOutputValues()
    ensureActualOutputMatchesDefaultExpected(testOutput)
  }

  test("It should return NoOp upon status Terminated") {
    val validator = new ValidationProcessFunction(NotificationDescriptor, invalidOutputTag, countsOutputTag, _ => (true,null))
    val testHarness: BroadcastOperatorTestHarness[HriRecord, NotificationRecord, HriRecord] =
      ProcessFunctionTestHarnesses.forBroadcastProcessFunction(validator, NotificationDescriptor)

    val testBroadcastInput:NotificationRecord = createTestBroadcastInputNode(DefaultTestBatchId,
      BatchNotification.Status.TERMINATED, DefaultTestRecordCount)
    testHarness.processBroadcastElement(testBroadcastInput, 1)
    val testInput:HriRecord = createTesthriRecordInput()
    testHarness.processElement(testInput, 20)
    val testOutput = testHarness.extractOutputValues()
    testOutput should have size 0

    val state = testHarness.getBroadcastState(NotificationDescriptor)
    val batchInfo = state.get(DefaultTestBatchId)
    batchInfo.getId should equal(DefaultTestBatchId)
    batchInfo.getExpectedRecordCount should equal(DefaultTestRecordCount)
  }

  test("For Bad record, should update isValid side output context value") {
    val validator = new ValidationProcessFunction(NotificationDescriptor, invalidOutputTag, countsOutputTag, _ => (true,null))
    val testHarness: BroadcastOperatorTestHarness[HriRecord, NotificationRecord, HriRecord] =
      ProcessFunctionTestHarnesses.forBroadcastProcessFunction(validator, NotificationDescriptor)

    val testInput:HriRecord = createInvalidTestInput()
    testHarness.processElement(testInput, 3)

    val invalidOut = testHarness.getSideOutput(invalidOutputTag)
    invalidOut should have size 1

    val invalidRecord = invalidOut.element().getValue
    invalidRecord.getBatchId should be (null)
    invalidRecord.getTopic should equal(testInput.topic)
    invalidRecord.getFailure should equal(validator.Missing_Batch_ID_Err_Msg)

    info("end invalid inout test")
  }

  test("For valid input, should update batch trackers") {
    val validator = new ValidationProcessFunction(NotificationDescriptor, invalidOutputTag, countsOutputTag, _ => (true,null))
    val testHarness: BroadcastOperatorTestHarness[HriRecord, NotificationRecord, HriRecord] =
      ProcessFunctionTestHarnesses.forBroadcastProcessFunction(validator, NotificationDescriptor)
    val testBroadcastInput:NotificationRecord = createTestBroadcastInputNode(DefaultTestBatchId,
      BatchNotification.Status.SEND_COMPLETED, DefaultTestRecordCount)
    testHarness.processBroadcastElement(testBroadcastInput, 1)

    val testInput:HriRecord = createTesthriRecordInput()
    testHarness.processElement(testInput, 3)
    val countsSideOutput = testHarness.getSideOutput(countsOutputTag)
    val outputVal = countsSideOutput.element().getValue()
    outputVal.get("batchId").asText() should equal(DefaultTestBatchId)
    outputVal.get("isValid").asBoolean() should be(true)
  }

  test("For Invalid input, should update batch trackers and send to invalid topic") {
    val errorMsg = "error message"
    val validator = new ValidationProcessFunction(NotificationDescriptor, invalidOutputTag, countsOutputTag, _ => (false,errorMsg))
    val testHarness: BroadcastOperatorTestHarness[HriRecord, NotificationRecord, HriRecord] =
      ProcessFunctionTestHarnesses.forBroadcastProcessFunction(validator, NotificationDescriptor)
    val testBroadcastInput:NotificationRecord = createTestBroadcastInputNode(DefaultTestBatchId,
      BatchNotification.Status.SEND_COMPLETED, DefaultTestRecordCount)
    testHarness.processBroadcastElement(testBroadcastInput, 1)

    val testInput:HriRecord = createTesthriRecordInput()
    testHarness.processElement(testInput, 3)

    val invalidOut = testHarness.getSideOutput(invalidOutputTag)
    invalidOut should have size 1

    val invalidRecord = invalidOut.element().getValue
    invalidRecord.getBatchId should be (validator.extractBatchId(testInput.headers))
    invalidRecord.getTopic should equal(testInput.topic)
    invalidRecord.getFailure should equal(errorMsg)

    val countsSideOutput = testHarness.getSideOutput(countsOutputTag)
    val outputVal = countsSideOutput.element().getValue()
    outputVal.get("batchId").asText() should equal(DefaultTestBatchId)
    outputVal.get("isValid").asBoolean() should be(false)
  }

  test("For unknown batchId, should send record to invalid topic") {
    val validator = new ValidationProcessFunction(NotificationDescriptor, invalidOutputTag, countsOutputTag, _ => (true,null))
    val testHarness: BroadcastOperatorTestHarness[HriRecord, NotificationRecord, HriRecord] =
      ProcessFunctionTestHarnesses.forBroadcastProcessFunction(validator, NotificationDescriptor)

    val emptyHeaders = new RecordHeaders()
    val testInput:HriRecord = new HriRecord(emptyHeaders, null, DefaultTestMsgBody.getBytes(StandardCharsets.UTF_8), DefaultTopic, DefaultPartition, DefaultOffset)
    testHarness.processElement(testInput, 3)

    val invalidOut = testHarness.getSideOutput(invalidOutputTag)
    invalidOut should have size 1

    val invalidRecord = invalidOut.element().getValue
    invalidRecord.getBatchId should be (null)
    invalidRecord.getTopic should equal(testInput.topic)
    invalidRecord.getFailure should equal(validator.Missing_Batch_ID_Err_Msg)
  }

  test("when batchId is already completed, should send record to invalid topic") {
    val alreadyCompletedErrorMsg = "Bad Message - batchId is already completed"
    val validator = new ValidationProcessFunction(NotificationDescriptor, invalidOutputTag, countsOutputTag, _ => (false,alreadyCompletedErrorMsg))
    val testHarness: BroadcastOperatorTestHarness[HriRecord, NotificationRecord, HriRecord] =
      ProcessFunctionTestHarnesses.forBroadcastProcessFunction(validator, NotificationDescriptor)
    val testBroadcastInput:NotificationRecord = createTestBroadcastInputNode(DefaultTestBatchId,
      BatchNotification.Status.COMPLETED, DefaultTestRecordCount)
    testHarness.processBroadcastElement(testBroadcastInput, 1)

    val testInput:HriRecord = createTesthriRecordInput()
    testHarness.processElement(testInput, 3)

    val invalidOut = testHarness.getSideOutput(invalidOutputTag)
    invalidOut should have size 1

    val invalidRecord = invalidOut.element().getValue
    invalidRecord.getBatchId should be (validator.extractBatchId(testInput.headers))
    invalidRecord.getTopic should equal(testInput.topic)
    invalidRecord.getFailure should equal(alreadyCompletedErrorMsg)
    invalidRecord.getPartition should equal(testInput.partition)
    invalidRecord.getOffset should equal(testInput.offset)
  }

  test("getBatch should throw exception when invalid request code 400 is returned") {
    val mockClient = mock[MgmtClient]

    when(mockClient.getBatchId(tenantId, DefaultTestBatchId))thenReturn(Failure(new RequestException("Invalid request", HttpStatus.SC_BAD_REQUEST)))

    val validator = new TestValidationProcessFunction(
      NotificationDescriptor,
      invalidOutputTag,
      countsOutputTag,
      _ => (true,null),
      tenantId,
      mockClient
    )

    validator.initialBackOff = Duration.apply(11, TimeUnit.MILLISECONDS)
    validator.maxBackOff = Duration.apply(11, TimeUnit.MILLISECONDS)
    validator.maxRetry = java.time.Duration.ofMillis(10)

    val testHarness: BroadcastOperatorTestHarness[HriRecord, NotificationRecord, HriRecord] =
      ProcessFunctionTestHarnesses.forBroadcastProcessFunction(validator, NotificationDescriptor)

    val testInput:HriRecord = createTesthriRecordInput()

    val thrown = intercept[FlinkException] {
      testHarness.processElement(testInput, 3)
    }
    if (thrown == null) {fail("expected exception not thrown")}
    thrown.getMessage should equal("Call to HRI Management API failed: Invalid request.")
  }

  test("getBatch should throw exception when unauthorized code 401 is returned") {
    val mockClient = mock[MgmtClient]

    when(mockClient.getBatchId(tenantId, DefaultTestBatchId))thenReturn(Failure(new RequestException("Unauthorized", HttpStatus.SC_UNAUTHORIZED)))

    val validator = new TestValidationProcessFunction(
      NotificationDescriptor,
      invalidOutputTag,
      countsOutputTag,
      _ => (true,null),
      tenantId,
      mockClient
    )

    validator.initialBackOff = Duration.apply(11, TimeUnit.MILLISECONDS)
    validator.maxBackOff = Duration.apply(11, TimeUnit.MILLISECONDS)
    validator.maxRetry = java.time.Duration.ofMillis(10)

    val testHarness: BroadcastOperatorTestHarness[HriRecord, NotificationRecord, HriRecord] =
      ProcessFunctionTestHarnesses.forBroadcastProcessFunction(validator, NotificationDescriptor)

    val testInput:HriRecord = createTesthriRecordInput()

    val thrown = intercept[FlinkException] {
      testHarness.processElement(testInput, 3)
    }
    if (thrown == null) {fail("expected exception not thrown")}
    thrown.getMessage should equal("Call to HRI Management API failed: Unauthorized.")
  }


  def createTestBroadcastInputNode(batchId: String, status: BatchNotification.Status, recordCount: Int): NotificationRecord = {
    val notification = createTestBatchNotification(batchId, status, recordCount)
    new NotificationRecord(null, null, notification)
  }

  def createTestBatchNotification(batchId: String, status: BatchNotification.Status, recordCount: Int): BatchNotification = {
    val notification = new BatchNotification()
      .withId(batchId)
      .withName("name-134340")
      .withTopic("ingest.1.in")
      .withDataType("claims")
      .withStatus(status)
      .withStartDate(OffsetDateTime.now(ZoneOffset.UTC).truncatedTo(ChronoUnit.SECONDS))
      .withEndDate(OffsetDateTime.now(ZoneOffset.UTC).truncatedTo(ChronoUnit.SECONDS))
      .withExpectedRecordCount(recordCount)
      .withInvalidThreshold(10)
    notification
  }

  def createTesthriRecordInput(): HriRecord = {
    val headers = new RecordHeaders()
    headers.add("batchId", DefaultTestBatchId.getBytes(StandardCharsets.UTF_8))

    new HriRecord(headers, null, DefaultTestMsgBody.getBytes(StandardCharsets.UTF_8), DefaultTopic, DefaultPartition, DefaultOffset)
  }

  def createInvalidTestInput(): HriRecord = {
    new HriRecord(new RecordHeaders(), null, DefaultTestInvalidMsgBody.getBytes(StandardCharsets.UTF_8), DefaultTopic, DefaultPartition, DefaultOffset)
  }

  def ensureActualOutputMatchesDefaultExpected(testOutput: util.List[HriRecord]): Unit = {
    testOutput should have size 1
    val record = testOutput.get(0)

    val actualBatchId = record.headers.asScala.find(_.key() == "batchId")
      .map(h => new String(h.value(), StandardCharsets.UTF_8))
      .getOrElse(null)
    actualBatchId should equal(DefaultTestBatchId)

    val actualMsgBody = new String(record.value, StandardCharsets.UTF_8)
    actualMsgBody should equal(DefaultTestMsgBody)

    val actualTopic = record.topic;
    actualTopic should equal(DefaultTopic)

    val actualPartition = record.partition
    actualPartition should equal(DefaultPartition)

    val actualOffset = record.offset
    actualOffset should equal(DefaultOffset)
  }

}
