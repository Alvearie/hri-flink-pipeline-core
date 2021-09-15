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
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers._

import scala.collection.JavaConverters._

class NotificationSerializerTest extends AnyFunSuite {

    private val mapper = JsonMapper.builder()
      .addModule(new JavaTimeModule())
      .disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES)
      .build()

    // header values
    private val HeaderField = "headerField"
    private val ExpectedHeaderValue = "headerValue"

    // key value
    private val ExpectedKey = "key"

    private val TopicName = "topicName"
    private val FakeHeaders = createHeaders()
    private val FakeKey = ExpectedKey.getBytes(StandardCharsets.UTF_8)
    private val ExpectedNotification = createBatchNotification()

    test("All fields should be preserved") {
        val serializer = new NotificationSerializer(TopicName)
        val notificationRecord = new NotificationRecord(FakeHeaders, FakeKey, ExpectedNotification)

        val record = serializer.serialize(notificationRecord, ConsumerRecord.NO_TIMESTAMP)
        val actualHeaders = record.headers()
        val actualKey = new String(record.key(), StandardCharsets.UTF_8)
        val actualNotification = mapper.readValue(record.value(), classOf[BatchNotification])

        val actualHeaderVal = actualHeaders.asScala.find(_.key() == HeaderField)
            .map(h => new String(h.value(), StandardCharsets.UTF_8))
            .getOrElse("none")

        actualHeaderVal should equal(ExpectedHeaderValue)
        actualKey should equal(ExpectedKey)
        actualNotification should equal(ExpectedNotification)
    }

    test("Null headers should be empty") {
        val serializer = new NotificationSerializer(TopicName)
        val notificationRecord = new NotificationRecord(null, FakeKey, ExpectedNotification)

        val record = serializer.serialize(notificationRecord, ConsumerRecord.NO_TIMESTAMP)
        val actualHeaders = record.headers()
        val actualKey = new String(record.key(), StandardCharsets.UTF_8)
        val actualNotification = mapper.readValue(record.value(), classOf[BatchNotification])

        actualHeaders.toArray shouldBe empty
        actualKey should equal(ExpectedKey)
        actualNotification should equal(ExpectedNotification)
    }

    test("Null key should be preserved") {
        val serializer = new NotificationSerializer(TopicName)
        val notificationRecord = new NotificationRecord(null, null, ExpectedNotification)

        val record = serializer.serialize(notificationRecord, ConsumerRecord.NO_TIMESTAMP)
        val actualHeaders = record.headers()
        val actualKey = record.key()
        val actualNotification = mapper.readValue(record.value(), classOf[BatchNotification])

        actualHeaders.toArray shouldBe empty
        actualKey should be(null)
        actualNotification should equal(ExpectedNotification)
    }

    test("Null body should be preserved") {
        val serializer = new NotificationSerializer(TopicName)
        val notificationRecord = new NotificationRecord(null, null, null)

        val record = serializer.serialize(notificationRecord, ConsumerRecord.NO_TIMESTAMP)
        val actualHeaders = record.headers()
        val actualKey = record.key()
        val actualBody = record.value()

        actualHeaders.toArray shouldBe empty
        actualKey should be(null)
        actualBody should be(null)
    }

    test("UTF8 Characters should be preserved") {
        val utf8Chars = "中文"
        val utf8Headers = new RecordHeaders()
        utf8Headers.add(HeaderField, (ExpectedHeaderValue+utf8Chars).getBytes(StandardCharsets.UTF_8))
        val utf8Key = (ExpectedKey+utf8Chars).getBytes(StandardCharsets.UTF_8)
        val utf8Notification = createBatchNotification()
          .withName( "name"+utf8Chars)
          .withDataType("dataType"+utf8Chars)

        val serializer = new NotificationSerializer(TopicName)
        val notificationRecord = new NotificationRecord(utf8Headers, utf8Key, utf8Notification)

        val record = serializer.serialize(notificationRecord, ConsumerRecord.NO_TIMESTAMP)
        val actualHeaders = record.headers()
        val actualKey = new String(record.key(), StandardCharsets.UTF_8)
        val actualNotification = mapper.readValue(record.value(), classOf[BatchNotification])

        val actualHeaderVal = actualHeaders.asScala.find(_.key() == HeaderField)
          .map(h => new String(h.value(), StandardCharsets.UTF_8))
          .getOrElse("none")

        actualHeaderVal should equal(ExpectedHeaderValue+utf8Chars)
        actualKey should equal(ExpectedKey+utf8Chars)
        actualNotification should equal(utf8Notification)
    }

    def createHeaders(): Headers = {
        val headers = new RecordHeaders()
        headers.add(HeaderField, ExpectedHeaderValue.getBytes(StandardCharsets.UTF_8))
        headers
    }

    def createBatchNotification(): BatchNotification = {
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