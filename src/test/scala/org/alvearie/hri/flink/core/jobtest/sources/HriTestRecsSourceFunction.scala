/**
 * (C) Copyright IBM Corp. 2021
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package org.alvearie.hri.flink.core.jobtest.sources

import org.alvearie.hri.flink.core.TestHelper
import org.alvearie.hri.flink.core.serialization.HriRecord
import org.apache.flink.streaming.api.functions.source.{RichParallelSourceFunction, SourceFunction}
import org.apache.kafka.common.header.Headers

class HriTestRecsSourceFunction() extends RichParallelSourceFunction[HriRecord] {

  @volatile private var isRunning = true
  private var hriRecordsOpt: Option[Seq[HriRecord]] = None
  private val delayKey = TestHelper.DelayHeaderKey
  //The initDelayInMillis is a one-time Delay before ALL HRI Record messages are sent
  private var initDelayInMillis: Long = 0
  //The endingDelay is a one-time Delay after ALL HRI Record messages are sent
  private var endingDelay: Long = 1000

  def this(hriRecsOpt: Option[Seq[HriRecord]]) {
    this()
    this.hriRecordsOpt = hriRecsOpt
  }

  def this(hriRecsOpt: Option[Seq[HriRecord]] = None, initDelay: Long = 0, endingDelay: Long = 1000) {
    this()
    this.hriRecordsOpt = hriRecsOpt
    this.initDelayInMillis = initDelay
    this.endingDelay = endingDelay
  }

  override def run(ctx: SourceFunction.SourceContext[HriRecord]): Unit = {
    val startTime = System.currentTimeMillis()

    val hriRecs = hriRecordsOpt match {
      case Some(rec) => rec
      case None      => {
        println("No HRI Recs To Test With - Pass In HRI_Records in Constructor!")
        return
      }
    }

    if (initDelayInMillis > 0) {
      println("Initial Delay of Processing HRI Records -> " + initDelayInMillis + " millis")
      Thread.sleep(initDelayInMillis)
    }

    val numHriRecs = hriRecs.size
    println(s"Starting Hri Test Recs Sources Processing of " +
      s"${numHriRecs} recs at startTime: $startTime")
    val itr = hriRecs.iterator
    while(isRunning && itr.hasNext) {
      val hriRecord = itr.next()
      val delayPerRecMillis:Long = getRecordDelay(hriRecord.headers)
      if (delayPerRecMillis > 0) {
        Thread.sleep(delayPerRecMillis)
      }
      println("send In HRI test Rec with key: " + hriRecord.key.toString)
      ctx.collect(hriRecord)
    }

    val endTime = System.currentTimeMillis()
    println(s"Took ${endTime - startTime} msecs for ${numHriRecs} values")
    //this sleep is added to trigger onTimer callbacks in downstream processors like Tracker.scala
    Thread.sleep(endingDelay)
  }

  override def cancel(): Unit = {
    isRunning = false
  }

  def getRecordDelay(headers: Headers): Long = {
    var delay = 0L
    val delayHeader = headers.lastHeader(delayKey)
    if(delayHeader != null) {
      val headerBytes = delayHeader.value()
      delay = TestHelper.convertByteArrayToLong(headerBytes)
      println("hri_Rec_Delay: " + delay)
    }
    delay
  }

  def getInitialDelay(): Long = {
    initDelayInMillis
  }
}
