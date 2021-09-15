/**
 * (C) Copyright IBM Corp. 2021
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package org.alvearie.hri.flink.core

import org.alvearie.hri.flink.core.serialization.HriRecord

import scala.runtime.RichBoolean

trait Validator extends Serializable {
  def isValid(record: HriRecord) : (RichBoolean, String)
}
