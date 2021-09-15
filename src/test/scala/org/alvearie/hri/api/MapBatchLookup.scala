/**
 * (C) Copyright IBM Corp. 2021
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package org.alvearie.hri.api

import scala.util.{Failure, Success, Try}
import org.apache.http.HttpStatus
import org.slf4j.LoggerFactory

class MapBatchLookup(batchNotificationSeq: Seq[BatchNotification]) extends BatchLookup with Serializable {

  private val batchNotificationMap: Map[String, BatchNotification] = Map(batchNotificationSeq map { a => a.getId -> a }: _*)

  private val log = LoggerFactory.getLogger(this.getClass)

  override def getBatchId(tenantId: String, batchId: String): Try[BatchNotification] = {
    batchNotificationMap.get(batchId) match {
      case Some(b) =>
        log.info("found batch " + batchId)
        Success(b)
      case None =>
        log.info("did not find batch " + batchId)
        Failure(new RequestException("not found", HttpStatus.SC_NOT_FOUND))
    }

  }
}
