/**
 * (C) Copyright IBM Corp. 2021
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package org.alvearie.hri.flink.core.jobtest.sources

import java.io.{ObjectInputStream, ObjectOutputStream}

import org.apache.kafka.common.header.internals.RecordHeader
import org.apache.kafka.common.header.internals.RecordHeaders

class TestRecordHeaders extends RecordHeaders with Serializable {

  private val serialVersionUID = - 1749935200821233226L

  private def writeObject(oos: ObjectOutputStream): Unit = {
    oos.defaultWriteObject()
    val headersArr = this.toArray
    oos.writeInt(headersArr.size)
    headersArr.foreach(header => {
      val hdrValAsString = (header.value().map(_.toChar).mkString)
      oos.writeUTF(header.key())
      oos.writeUTF(hdrValAsString)
    })
  }

  private def readObject(ois: ObjectInputStream): Unit = {
    ois.defaultReadObject()
    val headersArrSize = ois.readInt()
    for (i <- 0 to headersArrSize - 1) {
      val hKey = ois.readUTF()
      val hValue = ois.readUTF()
      val readHeader = new RecordHeader(hKey, hValue.getBytes)
      this.add(readHeader)
    }
  }

}
