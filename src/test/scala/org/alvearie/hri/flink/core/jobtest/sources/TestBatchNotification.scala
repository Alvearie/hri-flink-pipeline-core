/**
 * (C) Copyright IBM Corp. 2021
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package org.alvearie.hri.flink.core.jobtest.sources

import java.io.{ObjectInputStream, ObjectOutputStream}

import com.fasterxml.jackson.databind.DeserializationFeature
import com.fasterxml.jackson.databind.json.JsonMapper
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule
import org.alvearie.hri.api.BatchNotification
import org.apache.commons.io.IOUtils

//@SerialVersionUID(- 2004152013543193491L)
class TestBatchNotification extends BatchNotification with Serializable {
  private val serialVersionUID = - 2004152013543193491L

  @transient private lazy val mapper = JsonMapper.builder()
    .addModule(new JavaTimeModule())
    .disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES)
    .build()

  private def writeObject(oos: ObjectOutputStream): Unit = {
    oos.defaultWriteObject()
    oos.write(mapper.writeValueAsBytes(this))
  }

  private def readObject(ois: ObjectInputStream): Unit = {
    ois.defaultReadObject()
    val bytes = IOUtils.toByteArray(ois)
    val deserializedObj = mapper.readValue(bytes, classOf[TestBatchNotification])
    this.withId(deserializedObj.getId())
    this.withName(deserializedObj.getName())
    this.withTopic(deserializedObj.getTopic())
    this.withDataType(deserializedObj.getDataType())
    this.withStatus(deserializedObj.getStatus())
    this.withExpectedRecordCount(deserializedObj.getExpectedRecordCount())
    this.withActualRecordCount(deserializedObj.getActualRecordCount())
    this.withInvalidRecordCount(deserializedObj.getInvalidRecordCount())
    this.withStartDate(deserializedObj.getStartDate())
    this.withEndDate(deserializedObj.getEndDate())
    this.withMetadata(deserializedObj.getMetadata())
    this.withInvalidThreshold(deserializedObj.getInvalidThreshold())
    this.withFailureMessage(deserializedObj.getFailureMessage())
  }

}
