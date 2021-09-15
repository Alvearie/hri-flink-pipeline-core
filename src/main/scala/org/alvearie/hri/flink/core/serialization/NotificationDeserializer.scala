/**
 * (C) Copyright IBM Corp. 2021
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package org.alvearie.hri.flink.core.serialization

import com.fasterxml.jackson.databind.DeserializationFeature
import com.fasterxml.jackson.databind.json.JsonMapper
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule
import org.alvearie.hri.api.BatchNotification
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.slf4j.LoggerFactory

/**
 * An implementation of a KafkaDeserializationSchema, which parses the Value to a BatchNotification.
 * It is expected that this class will be used by HRI Flink processors to handle "notification" records. These
 * records are produced by the HRI Mgmt-API in a known format and Flink processors must have access to the individual
 * field values within the message.
 */
class NotificationDeserializer() extends KafkaDeserializationSchema[NotificationRecord] {

   @transient private lazy val mapper = JsonMapper.builder()
       .addModule(new JavaTimeModule())
       .disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES)
       .build()

    @transient private val log = LoggerFactory.getLogger(this.getClass)

    override def deserialize(record: ConsumerRecord[Array[Byte], Array[Byte]]): NotificationRecord = {
        val value = if (record.value == null) null else mapper.readValue(record.value, classOf[BatchNotification])

        new NotificationRecord(record.headers(), record.key(), value)
    }

    override def isEndOfStream(nextElement: NotificationRecord): Boolean = false

    override def getProducedType: TypeInformation[NotificationRecord] = TypeInformation.of(classOf[NotificationRecord])

}
