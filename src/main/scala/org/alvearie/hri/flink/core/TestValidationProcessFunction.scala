/**
 * (C) Copyright IBM Corp. 2021
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package org.alvearie.hri.flink.core

import com.fasterxml.jackson.databind.node.ObjectNode
import org.alvearie.hri.api.{BatchLookup, BatchNotification, InvalidRecord, MgmtClient}
import org.alvearie.hri.flink.core.serialization.HriRecord
import org.apache.flink.api.common.state.MapStateDescriptor
import org.apache.flink.streaming.api.scala.OutputTag

class TestValidationProcessFunction(notificationDescriptor: MapStateDescriptor[String, BatchNotification],
                                    invalidOutputTag: OutputTag[InvalidRecord],
                                    countsOutputTag: OutputTag[ObjectNode],
                                    validator: Validator,
                                    tenantId: String,
                                    mockMgmtClient: BatchLookup
                                   )
  extends ValidationProcessFunction(
    notificationDescriptor,
    invalidOutputTag,
    countsOutputTag,
    validator,
    tenantId,
    "url",
    "clientId",
    "clientSecret",
    "audience",
    "https://oauthdomain.com/hri") {
  override def createMgmtClient(): BatchLookup = mockMgmtClient
}
