/**
 * (C) Copyright IBM Corp. 2021
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package org.alvearie.hri.flink.core.serialization

import java.nio.charset.StandardCharsets

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.header.Headers
import org.apache.kafka.common.header.internals.RecordHeaders
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers._

import scala.collection.JavaConverters._

class HriRecordSerializerTest extends AnyFunSuite {

    // header values
    private val HeaderField = "headerField"
    private val ExpectedHeaderValue = "headerValue"

    // key and body values
    private val ExpectedKey = "key"
    private val ExpectedBody = "body"

    private val TopicName = "topicName"
    private val FakeHeaders = createHeaders()
    private val FakeKey = ExpectedKey.getBytes(StandardCharsets.UTF_8)
    private val FakeBody = ExpectedBody.getBytes(StandardCharsets.UTF_8)
    private val FakePartition = 1
    private val FakeOffset = 1234L

    test("All fields should be preserved") {
        val serializer = new HriRecordSerializer(TopicName)
        val hriRecord = new HriRecord(FakeHeaders, FakeKey, FakeBody, TopicName, FakePartition, FakeOffset)

        val record = serializer.serialize(hriRecord, ConsumerRecord.NO_TIMESTAMP)
        val actualHeaders = record.headers()
        val actualKey = new String(record.key(), StandardCharsets.UTF_8)
        val actualBody = new String(record.value(), StandardCharsets.UTF_8)

        val actualHeaderVal = actualHeaders.asScala.find(_.key() == HeaderField)
            .map(h => new String(h.value(), StandardCharsets.UTF_8))
            .getOrElse("none")

        actualHeaderVal should equal(ExpectedHeaderValue)
        actualKey should equal(ExpectedKey)
        actualBody should equal(ExpectedBody)
    }

    test("Null headers should be empty") {
        val serializer = new HriRecordSerializer(TopicName)
        val hriRecord = new HriRecord(null, FakeKey, FakeBody, TopicName, FakePartition, FakeOffset)

        val record = serializer.serialize(hriRecord, ConsumerRecord.NO_TIMESTAMP)
        val actualHeaders = record.headers()
        val actualKey = new String(record.key(), StandardCharsets.UTF_8)
        val actualBody = new String(record.value(), StandardCharsets.UTF_8)

        actualHeaders.toArray shouldBe empty
        actualKey should equal(ExpectedKey)
        actualBody should equal(ExpectedBody)
    }

    test("Null key should be preserved") {
        val serializer = new HriRecordSerializer(TopicName)
        val hriRecord = new HriRecord(null, null, FakeBody, TopicName, FakePartition, FakeOffset)

        val record = serializer.serialize(hriRecord, ConsumerRecord.NO_TIMESTAMP)
        val actualHeaders = record.headers()
        val actualKey = record.key()
        val actualBody = new String(record.value(), StandardCharsets.UTF_8)

        actualHeaders.toArray shouldBe empty
        actualKey should be(null)
        actualBody should equal(ExpectedBody)
    }

    test("Null body should be preserved") {
        val serializer = new HriRecordSerializer(TopicName)
        val hriRecord = new HriRecord(null, null, null, TopicName, FakePartition, FakeOffset)

        val record = serializer.serialize(hriRecord, ConsumerRecord.NO_TIMESTAMP)
        val actualHeaders = record.headers()
        val actualKey = record.key()
        val actualBody = record.value()

        actualHeaders.toArray shouldBe empty
        actualKey should be(null)
        actualBody should be(null)
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

        val serializer = new HriRecordSerializer(TopicName)
        val hriRecord = new HriRecord(utf8Headers, utf8Key, utf8Body, TopicName, FakePartition, FakeOffset)

        val record = serializer.serialize(hriRecord, ConsumerRecord.NO_TIMESTAMP)
        val actualHeaders = record.headers()
        val actualKey = new String(record.key(), StandardCharsets.UTF_8)
        val actualBody = new String(record.value(), StandardCharsets.UTF_8)

        val actualHeaderVal = actualHeaders.asScala.find(_.key() == HeaderField)
          .map(h => new String(h.value(), StandardCharsets.UTF_8))
          .getOrElse("none")

        actualHeaderVal should equal(ExpectedHeaderValue+utf8Chars)
        actualKey should equal(ExpectedKey+utf8Chars)
        actualBody should equal(ExpectedBody+utf8Chars)
    }

    def createHeaders(): Headers = {
        val headers = new RecordHeaders()
        headers.add(HeaderField, ExpectedHeaderValue.getBytes(StandardCharsets.UTF_8))
        headers
    }

}