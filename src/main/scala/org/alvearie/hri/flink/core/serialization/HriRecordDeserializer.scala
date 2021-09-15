/**
 * (C) Copyright IBM Corp. 2021
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package org.alvearie.hri.flink.core.serialization

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema
import org.apache.kafka.clients.consumer.ConsumerRecord

/**
 * An implementation of a KafkaDeserializationSchema, which simply passes through keys and values as byte arrays.
 * It is expected that this class will be used by HRI Flink processors to handle "data" records. These records
 * are produced by Data Integrators and the HRI does not impose any restrictions on what type of data they contain,
 * so the Flink processors do not attempt to parse them.
 */
class HriRecordDeserializer() extends KafkaDeserializationSchema[HriRecord] {

    override def deserialize(record: ConsumerRecord[Array[Byte], Array[Byte]]): HriRecord = {
        new HriRecord(record.headers(), record.key(), record.value(), record.topic(), record.partition(), record.offset())
    }

    override def isEndOfStream(nextElement: HriRecord): Boolean = false

    override def getProducedType: TypeInformation[HriRecord] = TypeInformation.of(classOf[HriRecord])

}
