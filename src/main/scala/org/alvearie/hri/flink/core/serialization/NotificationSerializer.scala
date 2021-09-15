/**
 * (C) Copyright IBM Corp. 2021
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package org.alvearie.hri.flink.core.serialization

import com.fasterxml.jackson.databind.DeserializationFeature
import com.fasterxml.jackson.databind.json.JsonMapper
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema
import org.apache.kafka.clients.producer.ProducerRecord
import org.slf4j.LoggerFactory

import scala.tools.jline_embedded.internal.Nullable

/**
 * An implementation of a KafkaSerializationSchema, which encodes the Value as a BatchNotification.
 * It is expected that this class will be used by HRI Flink processors to handle "notification" records. These
 * records are produced by the HRI Mgmt-API in a known format and Flink processors must have access to the individual
 * field values within the message.
 *
 * @param topic Kafka topic to be inserted in serialized ProducerRecords
 */
class NotificationSerializer(val topic: String) extends KafkaSerializationSchema[NotificationRecord] {

    @transient private lazy val mapper = JsonMapper.builder()
        .addModule(new JavaTimeModule())
        .disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES)
        .build()

    @transient private val log = LoggerFactory.getLogger(this.getClass)

    override def serialize(element: NotificationRecord, @Nullable timestamp: java.lang.Long): ProducerRecord[Array[Byte], Array[Byte]] = {
        val value = if (element.value == null) null else mapper.writeValueAsBytes(element.value)
        new ProducerRecord[Array[Byte], Array[Byte]](topic, null, element.key, value, element.headers)
    }

}
