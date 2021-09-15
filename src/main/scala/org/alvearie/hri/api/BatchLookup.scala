/**
 * (C) Copyright IBM Corp. 2021
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package org.alvearie.hri.api

import scala.util.Try

trait BatchLookup {
  def getBatchId(tenantId: String, batchId: String): Try[BatchNotification]
}
