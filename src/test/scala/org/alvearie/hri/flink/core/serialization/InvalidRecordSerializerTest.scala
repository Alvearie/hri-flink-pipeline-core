/**
 * (C) Copyright IBM Corp. 2021
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package org.alvearie.hri.flink.core.serialization

import java.io.StringWriter
import java.nio.charset.StandardCharsets

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.alvearie.hri.api.InvalidRecord
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.header.Headers
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers._

import scala.collection.JavaConverters._

class InvalidRecordSerializerTest extends AnyFunSuite {

  @transient private lazy val mapper = new ObjectMapper().registerModule(DefaultScalaModule)

  private val TopicName = "topicName"
  private val FakeFailure = "oopsie"
  private val FakeBatchId = "testBatchId"
  private val DefaultPartition = 1
  private val DefaultOffset = (1234L).toInt
  private val ExpectedKey = null

  test("All fields should be preserved") {
    val serializer = new InvalidRecordSerializer(TopicName)
    val invalidRecord = new InvalidRecord()
      .withFailure(FakeFailure)
      .withBatchId(FakeBatchId)
      .withOffset(DefaultOffset)
      .withPartition(DefaultPartition)

    val record = serializer.serialize(invalidRecord, ConsumerRecord.NO_TIMESTAMP)
    val actualHeaders = record.headers()
    val actualKey = record.key()
    val actualValue = record.value()
    val actualBatchId = getBatchIdFromHeaders(actualHeaders)

    val expectedValue = getExpectedValue(invalidRecord)

    actualBatchId should equal(FakeBatchId)
    actualKey should be(ExpectedKey)
    actualValue should equal(expectedValue)
  }

  test("Null failure should be preserved") {
    val serializer = new InvalidRecordSerializer(TopicName)
    val invalidRecord = new InvalidRecord()
      .withFailure(null)
      .withBatchId(FakeBatchId)
      .withOffset(DefaultOffset)
      .withPartition(DefaultPartition)

    val record = serializer.serialize(invalidRecord, ConsumerRecord.NO_TIMESTAMP)
    val actualHeaders = record.headers()
    val actualKey = record.key()
    val actualValue = record.value()
    val actualBatchId = getBatchIdFromHeaders(actualHeaders)

    val expectedValue = getExpectedValue(invalidRecord)

    actualBatchId should equal(FakeBatchId)
    actualKey should be(ExpectedKey)
    actualValue should equal(expectedValue)
  }

  test("Null value should be preserved") {
    val serializer = new InvalidRecordSerializer(TopicName)
    val invalidRecord = new InvalidRecord()
      .withFailure(null)
      .withBatchId(null)
      .withOffset(null)
      .withPartition(null)

    val record = serializer.serialize(invalidRecord, ConsumerRecord.NO_TIMESTAMP)
    val actualHeaders = record.headers()
    val actualKey = record.key()
    val actualValue = record.value()

    val expectedValue = getExpectedValue(invalidRecord)

    actualHeaders.toArray shouldBe empty
    actualKey should be(ExpectedKey)
    actualValue should equal(expectedValue)
  }

  def getBatchIdFromHeaders(headers: Headers): String = {
    headers.asScala.find(_.key() == "batchId")
      .map(h => new String(h.value(), StandardCharsets.UTF_8))
      .getOrElse("none")
  }

  /**
   * The serialized 'value' of the ProducerRecord should be the InvalidRecord input params transformed into a JSON
   * string.
   * @param invalidRecord
   * @return
   */
  def getExpectedValue(invalidRecord: InvalidRecord): Array[Byte] = {
    val jsonOutput = new StringWriter()
    mapper.writeValue(jsonOutput, invalidRecord)
    jsonOutput.toString.getBytes(StandardCharsets.UTF_8)
  }
}
