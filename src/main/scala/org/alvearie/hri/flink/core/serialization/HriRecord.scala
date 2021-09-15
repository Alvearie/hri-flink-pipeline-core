/**
 * (C) Copyright IBM Corp. 2021
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package org.alvearie.hri.flink.core.serialization

import org.apache.kafka.common.header.Headers

class HriRecord(var headers: Headers, var key: Array[Byte], var value: Array[Byte],
                var topic: String, var partition: java.lang.Integer, var offset: java.lang.Long) extends Serializable {

    def this() {
        this(null, null, null, null, null, null)
    }

}
