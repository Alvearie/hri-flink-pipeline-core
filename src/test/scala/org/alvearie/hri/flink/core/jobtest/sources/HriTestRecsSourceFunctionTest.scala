/**
 * (C) Copyright IBM Corp. 2021
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package org.alvearie.hri.flink.core.jobtest.sources

import org.alvearie.hri.flink.core.TestHelper
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers._

class HriTestRecsSourceFunctionTest extends AnyFunSuite {

  test("it should get a positive RecordDelay value when Delay Header is present") {
    val srcFunction = new HriTestRecsSourceFunction()
    val testHeaders = TestHelper.getTestHeadersWithDelayHeader(250)
    srcFunction.getRecordDelay(testHeaders) should equal (250)
  }

  test("it should get a RecordDelay value = Zero when Delay Header is Not present") {
    val srcFunction = new HriTestRecsSourceFunction()
    val testHeaders = TestHelper.getDefaultTestHeaders()
    srcFunction.getRecordDelay(testHeaders) should equal (0)
  }

  test("it should use a initial (Before All Records) Delay when passed into constructor") {
    val startDelayTime = 100L
    val testHeaders = TestHelper.getDefaultTestHeaders()
    val srcFunction = new HriTestRecsSourceFunction(null, startDelayTime)
    srcFunction.getInitialDelay() should equal (startDelayTime)
  }
}
