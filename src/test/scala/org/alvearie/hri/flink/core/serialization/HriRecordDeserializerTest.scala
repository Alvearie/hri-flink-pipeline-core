/**
 * (C) Copyright IBM Corp. 2021
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package org.alvearie.hri.flink.core.serialization

import java.nio.charset.StandardCharsets

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.ScalaObjectMapper
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.header.Headers
import org.apache.kafka.common.header.internals.RecordHeaders
import org.apache.kafka.common.record.TimestampType
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers._

import scala.collection.JavaConverters._

class HriRecordDeserializerTest extends AnyFunSuite {

    private val mapper = new ObjectMapper with ScalaObjectMapper

    // header values
    private val HeaderField = "headerField"
    private val ExpectedHeaderValue = "headerValue"

    // key and body values
    private val ExpectedKey = "key"
    private val ExpectedBody = "body"

    // metadata values
    private val Partition = 3
    private val TopicName = "topicName"
    private val Offset = 1234L

    private val FakeHeaders = createHeaders()
    private val FakeKey = ExpectedKey.getBytes(StandardCharsets.UTF_8)
    private val FakeBody = ExpectedBody.getBytes(StandardCharsets.UTF_8)

    test("All fields should be preserved") {
        val deserializer = new HriRecordDeserializer
        val consumerRecord = createConsumerRecord(FakeHeaders, FakeKey, FakeBody)

        val record = deserializer.deserialize(consumerRecord)
        val actualHeaders = record.headers
        val actualKey = new String(record.key, StandardCharsets.UTF_8)
        val actualBody = new String(record.value, StandardCharsets.UTF_8)

        val actualHeaderVal = actualHeaders.asScala.find(_.key() == HeaderField)
            .map(h => new String(h.value(), StandardCharsets.UTF_8))
            .getOrElse("empty")

        actualHeaderVal should equal(ExpectedHeaderValue)
        actualKey should equal(ExpectedKey)
        actualBody should equal(ExpectedBody)
    }

    test("Null headers should be preserved") {
        val deserializer = new HriRecordDeserializer
        val consumerRecord = createConsumerRecordWithoutHeaders(FakeKey, FakeBody)

        val record = deserializer.deserialize(consumerRecord)
        val actualHeaders = record.headers
        val actualKey = new String(record.key, StandardCharsets.UTF_8)
        val actualBody = new String(record.value, StandardCharsets.UTF_8)
        val actualTopic = record.topic
        val actualPartition = record.partition
        val actualOffset = record.offset

        actualHeaders.toArray shouldBe empty
        actualKey should equal(ExpectedKey)
        actualBody should equal(ExpectedBody)
        actualTopic should equal(TopicName)
        actualPartition should equal(Partition)
        actualOffset should equal(Offset)
    }

    test("Null key should be preserved") {
        val deserializer = new HriRecordDeserializer
        val consumerRecord = createConsumerRecordWithoutHeaders(null, FakeBody)

        val record = deserializer.deserialize(consumerRecord)
        val actualHeaders = record.headers
        val actualKey = record.key
        val actualBody = new String(record.value, StandardCharsets.UTF_8)
        val actualTopic = record.topic
        val actualPartition = record.partition
        val actualOffset = record.offset

        actualHeaders.toArray shouldBe empty
        actualKey should be(null)
        actualBody should equal(ExpectedBody)
        actualTopic should equal(TopicName)
        actualPartition should equal(Partition)
        actualOffset should equal(Offset)
    }

    test("Null body should be preserved") {
        val deserializer = new HriRecordDeserializer
        val consumerRecord = createConsumerRecordWithoutHeaders(null, null)

        val record = deserializer.deserialize(consumerRecord)
        val actualHeaders = record.headers
        val actualKey = record.key
        val actualBody = record.value
        val actualTopic = record.topic
        val actualPartition = record.partition
        val actualOffset = record.offset

        actualHeaders.toArray shouldBe empty
        actualKey should be(null)
        actualBody should be(null)
        actualTopic should equal(TopicName)
        actualPartition should equal(Partition)
        actualOffset should equal(Offset)
    }

    // It's not possible to test if the Header Keys can include UTF8 characters, because they are just strings.
    // The Kafka documentation does not specify the encoding https://kafka.apache.org/documentation/#recordheader.
    // I did look through the Kafka client Java library, and it does use UTF8 string encoding when writing them.
    // But not sure if other implementations will.

    test("UTF8 Characters should be preserved") {
        val utf8Chars = "中文"
        val utf8Headers = new RecordHeaders()
        utf8Headers.add(HeaderField, (ExpectedHeaderValue+utf8Chars).getBytes(StandardCharsets.UTF_8))
        val utf8Key = (ExpectedKey+utf8Chars).getBytes(StandardCharsets.UTF_8)
        val utf8Body = (ExpectedBody+utf8Chars).getBytes(StandardCharsets.UTF_8)


        val deserializer = new HriRecordDeserializer
        val consumerRecord = createConsumerRecord(utf8Headers, utf8Key, utf8Body)

        val record = deserializer.deserialize(consumerRecord)
        val actualHeaders = record.headers
        val actualKey = new String(record.key, StandardCharsets.UTF_8)
        val actualBody = new String(record.value, StandardCharsets.UTF_8)

        val actualHeaderVal = actualHeaders.asScala.find(_.key() == HeaderField)
          .map(h => new String(h.value(), StandardCharsets.UTF_8))
          .getOrElse("empty")

        actualHeaderVal should equal(ExpectedHeaderValue+utf8Chars)
        actualKey should equal(ExpectedKey+utf8Chars)
        actualBody should equal(ExpectedBody+utf8Chars)
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

    def createConsumerRecordWithoutHeaders(key: Array[Byte], value: Array[Byte]): ConsumerRecord[Array[Byte], Array[Byte]] = {
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
        )
    }
}