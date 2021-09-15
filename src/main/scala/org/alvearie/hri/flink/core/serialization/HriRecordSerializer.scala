/**
 * (C) Copyright IBM Corp. 2021
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package org.alvearie.hri.flink.core.serialization

import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema
import org.apache.kafka.clients.producer.ProducerRecord

import scala.tools.jline_embedded.internal.Nullable

/**
 * An implementation of a KafkaSerializationSchema, which simply passes through keys and values as byte arrays.
 * It is expected that this class will be used by HRI Flink processors to handle "data" records. These records
 * are produced by Data Integrators and the HRI does not impose any restrictions on what type of data they contain,
 * so the Flink processors do not attempt to parse them.
 *
 * @param topic Kafka topic to be inserted in serialized ProducerRecords
 */
class HriRecordSerializer(val topic: String) extends KafkaSerializationSchema[HriRecord] {

    override def serialize(element: HriRecord, @Nullable timestamp: java.lang.Long): ProducerRecord[Array[Byte], Array[Byte]] = {
        new ProducerRecord[Array[Byte], Array[Byte]](topic, null, element.key, element.value, element.headers)
    }

}
