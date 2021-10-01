/**
 * (C) Copyright IBM Corp. 2021
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package org.alvearie.hri.flink.core.serialization

import java.io.{ObjectInputStream, ObjectOutputStream}

import org.alvearie.hri.api.BatchNotification
import org.apache.kafka.common.header.Headers

class NotificationRecord(var headers: Headers, val key: Array[Byte], val value: BatchNotification) extends Serializable{

    def this() {
        this(null, null, null)
    }

}
