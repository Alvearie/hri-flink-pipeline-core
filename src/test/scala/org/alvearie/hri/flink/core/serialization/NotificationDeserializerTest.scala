/**
 * (C) Copyright IBM Corp. 2021
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package org.alvearie.hri.flink.core.serialization

import java.nio.charset.StandardCharsets
import java.time.temporal.ChronoUnit
import java.time.{OffsetDateTime, ZoneOffset}

import com.fasterxml.jackson.databind.DeserializationFeature
import com.fasterxml.jackson.databind.json.JsonMapper
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule
import org.alvearie.hri.api.BatchNotification
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.header.Headers
import org.apache.kafka.common.header.internals.RecordHeaders
import org.apache.kafka.common.record.TimestampType
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers._

import scala.collection.JavaConverters._

class NotificationDeserializerTest extends AnyFunSuite {

    private val mapper = JsonMapper.builder()
      .addModule(new JavaTimeModule())
      .disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES)
      .build()

    // header values
    private val HeaderField = "headerField"
    private val ExpectedHeaderValue = "headerValue"

    // key
    private val ExpectedKey = "key"

    // metadata values
    private val Partition = 3
    private val TopicName = "topicName"
    private val Offset = 1234L

    private val FakeHeaders = createHeaders()
    private val FakeKey = ExpectedKey.getBytes(StandardCharsets.UTF_8)
    private val ExpectedNotification = createNotification()
    private val FakeNotification = mapper.writeValueAsBytes(ExpectedNotification)


    test("All fields should be preserved") {
        val deserializer = new NotificationDeserializer()
        val consumerRecord = createConsumerRecord(FakeHeaders, FakeKey, FakeNotification)

        val record = deserializer.deserialize(consumerRecord)
        val actualKey = new String(record.key, StandardCharsets.UTF_8)
        val actualNotification = record.value

        val actualHeaderVal = record.headers.asScala.find(_.key() == HeaderField)
            .map(h => new String(h.value(), StandardCharsets.UTF_8))
            .getOrElse("empty")

        actualHeaderVal should equal(ExpectedHeaderValue)
        actualKey should equal(ExpectedKey)
        actualNotification should equal(ExpectedNotification)
    }

    test("Null headers should be preserved") {
        val deserializer = new NotificationDeserializer()
        val consumerRecord = createConsumerRecord(null, FakeKey, FakeNotification)

        val record = deserializer.deserialize(consumerRecord)
        val actualHeaders = record.headers
        val actualKey = new String(record.key, StandardCharsets.UTF_8)
        val actualNotification = record.value

        actualHeaders should be(null)
        actualKey should equal(ExpectedKey)
        actualNotification should equal(ExpectedNotification)
    }

    test("Null key should be preserved") {
        val deserializer = new NotificationDeserializer()
        val consumerRecord = createConsumerRecord(null, null, FakeNotification)

        val record = deserializer.deserialize(consumerRecord)
        val actualHeaders = record.headers
        val actualKey = record.key
        val actualNotification = record.value

        actualHeaders should be(null)
        actualKey should be(null)
        actualNotification should equal(ExpectedNotification)
    }

    test("Null body should be preserved") {
        val deserializer = new NotificationDeserializer()
        val consumerRecord = createConsumerRecord(null, null, null)

        val record = deserializer.deserialize(consumerRecord)
        val actualHeaders = record.headers
        val actualKey = record.key
        val actualNotification = record.value

        actualHeaders should be(null)
        actualKey should be(null)
        actualNotification should equal(null)
    }

    test("UTF8 Characters should be preserved") {
        val utf8Chars = "中文"
        val utf8Headers = new RecordHeaders()
        utf8Headers.add(HeaderField, (ExpectedHeaderValue+utf8Chars).getBytes(StandardCharsets.UTF_8))
        val utf8Key = (ExpectedKey+utf8Chars).getBytes(StandardCharsets.UTF_8)
        val utf8Notification = createNotification()
          .withName( "name"+utf8Chars)
          .withDataType("dataType"+utf8Chars)

        val deserializer = new NotificationDeserializer()
        val consumerRecord = createConsumerRecord(utf8Headers, utf8Key, mapper.writeValueAsBytes(utf8Notification))

        val record = deserializer.deserialize(consumerRecord)
        val actualKey = new String(record.key, StandardCharsets.UTF_8)
        val actualNotification = record.value

        val actualHeaderVal = record.headers.asScala.find(_.key() == HeaderField)
          .map(h => new String(h.value(), StandardCharsets.UTF_8))
          .getOrElse("empty")

        actualHeaderVal should equal(ExpectedHeaderValue+utf8Chars)
        actualKey should equal(ExpectedKey+utf8Chars)
        actualNotification should equal(utf8Notification)
    }

    def createHeaders(): Headers = {
        val headers = new RecordHeaders()
        headers.add(HeaderField, ExpectedHeaderValue.getBytes(StandardCharsets.UTF_8))
        headers
    }


    def createConsumerRecord(headers: Headers, key: Array[Byte], value: Array[Byte]): ConsumerRecord[Array[Byte], Array[Byte]] = {
        new ConsumerRecord[Array[Byte], Array[Byte]](
            TopicName,
            Partition,
            Offset,
            ConsumerRecord.NO_TIMESTAMP,
            TimestampType.NO_TIMESTAMP_TYPE,
            ConsumerRecord.NULL_CHECKSUM.toLong,
            ConsumerRecord.NULL_SIZE,
            ConsumerRecord.NULL_SIZE,
            key,
            value,
            headers
        )
    }

    def createNotification(): BatchNotification = {
         new BatchNotification()
          .withId("id")
          .withName( "name")
          .withTopic("topic")
          .withDataType("dataType")
          .withStatus(BatchNotification.Status.COMPLETED)
          .withStartDate(OffsetDateTime.now(ZoneOffset.UTC).truncatedTo(ChronoUnit.SECONDS))
          .withEndDate(OffsetDateTime.now(ZoneOffset.UTC).truncatedTo(ChronoUnit.SECONDS))
          .withExpectedRecordCount(1)
          .withActualRecordCount(1)
          .withInvalidRecordCount(0)
          .withInvalidThreshold(10)
          .withFailureMessage("")
    }

}