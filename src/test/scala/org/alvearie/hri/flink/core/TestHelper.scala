/**
 * (C) Copyright IBM Corp. 2021
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package org.alvearie.hri.flink.core

import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets
import java.time.OffsetDateTime

import org.alvearie.hri.api.BatchNotification
import org.alvearie.hri.flink.core.jobtest.sources.{TestBatchNotification, TestRecordHeaders}
import org.alvearie.hri.flink.core.serialization.{HriRecord, NotificationRecord}
import org.apache.kafka.common.header.internals.RecordHeaders

object TestHelper {

  val DelayHeaderKey = "RecordDelayInMillis"
  private val DefaultBatchName = "Some-Test-Batch05-中文"
  private val DefaultDataType = "claims-中文"
  val DefaultTopic = "ingest.t1.data-int1.in"
  private val DefaultStartDate = "2020-12-08T03:02:23Z"
  private val DefaultEndDate = "2020-12-11T16:02:44Z"
  val DefaultTestBatchId = "batch-42"
  private val DefaultPartition = 1
  private val DefaultOffset = 1234L
  val PassThruHeaderVal = "justPassinThru"
  val ValidJsonOne = """{"resourceType":"Claim","id":"100150","text":{"div":"<div xmlns=\"http://www.w3.org/1999/xhtml\">A human-readable rendering of the MOnkee-Porcupine Health Claim</div>"},"insurance":[{"identifier":{"system":"http://PorcupineSys.com/claim","value":"19006"},"coverage":{"reference":"CoverageBlarg/9876B1"}}]}"""
  val ValidJsonTwo = """{"name":"Porcpine","age":26,"status":"snorkel-中文"}"""
  val InvalidHriBody1 = "Not-validJSON"
  val InvalidHriBody2 = "Luna-is-Not-Valid-JSON"
  val InvalidHriBody3 = "Porcuspino-Invalid"
  val InvalidHriBody4 = "Monkey-Invalid"
  val InvalidHriBody5 = "Tiger-Invalid-JSON"

  def createTestNotification(headers: RecordHeaders, key: Array[Byte],
                             batchId: String, expectedRecCount: Int,
                             status: BatchNotification.Status,
                             invalidThreshold: Int = 5): NotificationRecord = {

    val batchNotification = createTestBatchNotification(batchId, expectedRecCount,
      status, invalidThreshold, DefaultBatchName)
    new NotificationRecord(headers, key, batchNotification)
  }

  def createTestBatchNotification(batchId: String, expectedRecCount: Int,
                                        status: BatchNotification.Status,
                                        invalidThreshold: Int,
                                        batchName: String): TestBatchNotification = {
    return new TestBatchNotification()
      .withId(batchId)
      .withName(batchName)
      .withStatus(status)
      .withDataType(DefaultDataType)
      .withStartDate(OffsetDateTime.parse(DefaultStartDate))
      .withEndDate(OffsetDateTime.parse(DefaultEndDate))
      .withExpectedRecordCount(expectedRecCount)
      .withTopic(DefaultTopic)
      .withInvalidThreshold(invalidThreshold).asInstanceOf[TestBatchNotification]
  }

  def getDefaultTestHeaders() : TestRecordHeaders = {
    val headers = new TestRecordHeaders()
    headers.add("batchId", DefaultTestBatchId.getBytes(StandardCharsets.UTF_8))
    headers.add("passThru", PassThruHeaderVal.getBytes(StandardCharsets.UTF_8))
    headers
  }

  def getTestHeadersWithDelayHeader(delayTime:Long) : TestRecordHeaders = {
    val headers = getDefaultTestHeaders()
    headers.add(DelayHeaderKey, convertLongToByteArray(delayTime))
    headers
  }

  def createOneValidHriRecord(headers: TestRecordHeaders, key: Array[Byte], jsonStr: String): HriRecord = {
    val newHriRecBody = jsonStr
    new HriRecord(headers, key, newHriRecBody.getBytes(StandardCharsets.UTF_8) , DefaultTopic, DefaultPartition, DefaultOffset)
  }

  def convertLongToByteArray(longNum:Long): Array[Byte] = {
    ByteBuffer.allocate(8).putLong(longNum).array()
  }

  def convertByteArrayToLong(longBytes: Array[Byte]): Long = {
    val buffer = ByteBuffer.wrap(longBytes)
    buffer.getLong
  }

  // This sets an OS environment variable using reflection, which isn't normally allowed
  def setEnv(name: String, value: String): Unit = {
    val env = System.getenv
    val field = env.getClass.getDeclaredField("m")
    field.setAccessible(true)
    field.get(env).asInstanceOf[java.util.Map[String,String]].put(name, value)
  }

  // This removes an OS environment variable using reflection, which isn't normally allowed
  def removeEnv(name: String): Unit = {
    val env = System.getenv
    val field = env.getClass.getDeclaredField("m")
    field.setAccessible(true)
    field.get(env).asInstanceOf[java.util.Map[String,String]].remove(name)
  }

}
