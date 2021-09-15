/**
 * (C) Copyright IBM Corp. 2021
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package org.alvearie.hri.flink.core

import java.time.OffsetDateTime
import java.util.concurrent.TimeUnit

import org.alvearie.hri.api.BatchNotification.Status
import org.alvearie.hri.api.{BatchNotification, MgmtClient, RequestException}
import org.alvearie.hri.flink.core.serialization.NotificationRecord
import org.apache.flink.util.FlinkException
import org.apache.http.HttpStatus
import org.mockito.invocation.InvocationOnMock
import org.mockito.scalatest.MockitoSugar
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers._

import scala.concurrent.duration.Duration
import scala.util.{Failure, Success}

class TestMgmtApiSink(override val tenantId: String, mockMgmtClient: MgmtClient) extends MgmtApiSink(tenantId, "url", "clientId", "clientSecret", "audience", "https://oauthdomain.com/hri") {
  override def createMgmtClient(): MgmtClient = mockMgmtClient
}

class MgmtApiSinkTest extends AnyFunSuite with MockitoSugar{
  val tenantId = "mytenant"
  val batchId = "mybatch"
  val validCount = 10
  val invalidCount = 5
  val failureMessage = "Error message"

  test("it should call processingComplete for 'completed' Batch") {
    val mockClient = mock[MgmtClient]

    when(mockClient.processingComplete(tenantId, batchId, validCount, invalidCount)) thenReturn Success(Unit)
    val sink = new TestMgmtApiSink(tenantId, mockClient)

    sink.invoke(new NotificationRecord(null, null,
      createTestBatchNotification(Status.COMPLETED, validCount, invalidCount, null)), null)

    verify(mockClient).processingComplete(tenantId, batchId, validCount, invalidCount)
  }

  test("it should call fail for 'failed' Batch") {
    val mockClient = mock[MgmtClient]

    when(mockClient.failBatch(tenantId, batchId, validCount, invalidCount, failureMessage)) thenReturn Success(Unit)
    val sink = new TestMgmtApiSink(tenantId, mockClient)

    sink.invoke(new NotificationRecord(null, null,
      createTestBatchNotification(Status.FAILED, validCount, invalidCount, failureMessage)), null)

    verify(mockClient).failBatch(tenantId, batchId, validCount, invalidCount, failureMessage)
  }

  test("it should retry on a HRI MgmtApi failure") {
    val mockClient = mock[MgmtClient]

    var count = 0
    when(mockClient.processingComplete(tenantId, batchId, validCount, invalidCount))
      .thenAnswer( (_: InvocationOnMock) => {
        count += 1
        if (count < 5)
          Failure(new RequestException("Connection timeout", HttpStatus.SC_INTERNAL_SERVER_ERROR)) // code 500
        else
          Success(Unit)
      })

    val sink = new TestMgmtApiSink(tenantId, mockClient)
    sink.initialBackOff = Duration.apply(1, TimeUnit.MILLISECONDS)

    sink.invoke(new NotificationRecord(null, null,
      createTestBatchNotification(Status.COMPLETED, validCount, invalidCount, null)), null)

    verify(mockClient, times(5)).processingComplete(tenantId, batchId, validCount, invalidCount)
  }

  test("it should fail when it reaches the maxRetry") {
    val mockClient = mock[MgmtClient]

    var count = 0
    when(mockClient.processingComplete(tenantId, batchId, validCount, invalidCount))
      .thenReturn(Failure(new RequestException("Connection timeout", HttpStatus.SC_INTERNAL_SERVER_ERROR))) // code 500

    val sink = new TestMgmtApiSink(tenantId, mockClient)
    sink.initialBackOff = Duration.apply(1, TimeUnit.MILLISECONDS)
    sink.maxRetry = java.time.Duration.ofMillis(8)

    val caught = intercept[FlinkException] {
      sink.invoke(new NotificationRecord(null, null,
        createTestBatchNotification(Status.COMPLETED, validCount, invalidCount, null)), null)
    }

    val exMsg = caught.getMessage
    exMsg should startWith("Reached max HRI Management API retry timeout")
  }

  test("it should throw an Exception for an unexpected Batch status") {
    val mockClient = mock[MgmtClient]

    val sink = new TestMgmtApiSink(tenantId, mockClient)

    val caught = intercept[FlinkException] {
      sink.invoke(new NotificationRecord(null, null,
        createTestBatchNotification(Status.STARTED, validCount, invalidCount, failureMessage)), null)
    }
    val exMsg = caught.getMessage
    exMsg should startWith("Received unexpected status")
    exMsg should include (Status.STARTED.toString)
  }

  test("it should continue when error code 409 is returned for a conflict") {
    val mockClient = mock[MgmtClient]

    when(mockClient.processingComplete(tenantId, batchId, validCount, invalidCount))
      .thenReturn(Failure(new RequestException("Connection timeout", HttpStatus.SC_CONFLICT))) // code 409

    val sink = new TestMgmtApiSink(tenantId, mockClient)
    sink.initialBackOff = Duration.apply(1, TimeUnit.MILLISECONDS)
    sink.maxRetry = java.time.Duration.ofMillis(8)

    sink.invoke(new NotificationRecord(null, null,
      createTestBatchNotification(Status.COMPLETED, validCount, invalidCount, null)), null)

    verify(mockClient).processingComplete(tenantId, batchId, validCount, invalidCount)
  }

  test("it should throw exception when error code 400 is returned") {
    val mockClient = mock[MgmtClient]

    when(mockClient.processingComplete(tenantId, batchId, validCount, invalidCount))
      .thenReturn(Failure(new RequestException("Bad request", HttpStatus.SC_BAD_REQUEST))) // code 400

    val sink = new TestMgmtApiSink(tenantId, mockClient)
    sink.initialBackOff = Duration.apply(1, TimeUnit.MILLISECONDS)
    sink.maxRetry = java.time.Duration.ofMillis(8)

    val caught = intercept[FlinkException] {
      sink.invoke(new NotificationRecord(null, null,
        createTestBatchNotification(Status.COMPLETED, validCount, invalidCount, null)), null)
    }

    val exMsg = caught.getMessage
    exMsg should equal("Call to HRI Management API failed: Bad request.")
  }

  test("it should throw exception when error code 401 is returned") {
    val mockClient = mock[MgmtClient]

    when(mockClient.processingComplete(tenantId, batchId, validCount, invalidCount))
      .thenReturn(Failure(new RequestException("Unauthorized", HttpStatus.SC_UNAUTHORIZED))) // code 401

    val sink = new TestMgmtApiSink(tenantId, mockClient)
    sink.initialBackOff = Duration.apply(1, TimeUnit.MILLISECONDS)
    sink.maxRetry = java.time.Duration.ofMillis(8)

    val caught = intercept[FlinkException] {
      sink.invoke(new NotificationRecord(null, null,
        createTestBatchNotification(Status.COMPLETED, validCount, invalidCount, null)), null)
    }

    val exMsg = caught.getMessage
    exMsg should equal("Call to HRI Management API failed: Unauthorized.")
  }

  def createTestBatchNotification(status: BatchNotification.Status, actualRecordCount: Int, invalidRecordCount: Int, failMsg: String): BatchNotification = {
    new BatchNotification()
      .withId(batchId)
      .withName("myName")
      .withTopic("ingest.mytenant.da1.in")
      .withDataType("myDataType")
      .withStartDate(OffsetDateTime.parse("2020-05-05T21:02:23Z"))
      .withStatus(status)
      .withExpectedRecordCount(actualRecordCount)
      .withActualRecordCount(actualRecordCount)
      .withInvalidThreshold(5)
      .withInvalidRecordCount(invalidRecordCount)
      .withFailureMessage(failMsg)
  }
}
