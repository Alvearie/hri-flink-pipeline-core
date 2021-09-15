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
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.header.internals.RecordHeaders

import scala.tools.jline_embedded.internal.Nullable

class InvalidRecordSerializer (val topic: String) extends KafkaSerializationSchema[InvalidRecord] {

  @transient private lazy val mapper = new ObjectMapper().registerModule(DefaultScalaModule)

  def serialize(element: InvalidRecord, @Nullable timestamp: java.lang.Long): ProducerRecord[Array[Byte], Array[Byte]] = {
    // The ProducerRecord return value is created from the InvalidRecord input param transformed into a JSON String,
    // with the added Header (batchId).
    val jsonOutput = new StringWriter()
    mapper.writeValue(jsonOutput, element)
    val elementValue = jsonOutput.toString.getBytes(StandardCharsets.UTF_8)

    val elementKey = null
    val elementHeaders = new RecordHeaders()
    val elementBatchId = Option(element.getBatchId)
    if (elementBatchId.isDefined) {
      elementHeaders.add("batchId", elementBatchId.get.getBytes(StandardCharsets.UTF_8))
    }

    // Note:  The partition must be null and NOT record.getPartition.  The record's partition is the source partition,
    // and not necessarily the same partition of the topic we will be writing to.
    new ProducerRecord[Array[Byte], Array[Byte]](topic, null, elementKey, elementValue, elementHeaders)
  }

}
