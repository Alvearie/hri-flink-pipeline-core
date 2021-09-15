/**
 * (C) Copyright IBM Corp. 2021
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package org.alvearie.hri.flink.core.jobtest.sources

import org.alvearie.hri.api.BatchNotification
import org.alvearie.hri.flink.core.serialization.NotificationRecord
import org.apache.flink.streaming.api.functions.source.{RichParallelSourceFunction, SourceFunction}


class NotificationSourceFunction() extends RichParallelSourceFunction[NotificationRecord] {

  @volatile private var isRunning = true
  private var notificationsOpt: Option[Seq[NotificationRecord]] = None
  //NOTE: DelayPerRECORDMillis Applies to Notification messages that do NOT have BatchNotification.Status.STARTED;
  //In other words, there will never be a delay per record of the BatchNotification.Status.STARTED notification
  private var delayPerRecMillis: Long = 0
  //initDelayInMillis is a one-time Initial Delay before ALL Notification messages are sent
  private var initDelayInMillis: Long = 0

  def this(notificationRecOpt: Option[Seq[NotificationRecord]] = None, delayPerRec: Long = 0, initDelay: Long = 0) {
    this()
    this.notificationsOpt = notificationRecOpt
    this.delayPerRecMillis = delayPerRec
    this.initDelayInMillis = initDelay
  }

  override def run(ctx: SourceFunction.SourceContext[NotificationRecord]): Unit = {
    val startTime = System.currentTimeMillis()

    val notificationRecs = notificationsOpt match {
      case Some(rec) => rec
      case None      => {
        println("No Notification Recs To Test With - Pass In NotificationRecords in Constructor! ")
        return
      }
    }

    val numRecs = notificationRecs.size
    println(s"Start Notification Source Processing of " +
      s"${numRecs} possible recs at startTime: $startTime")
    val itr = notificationRecs.iterator

    val numSubtasks = getRuntimeContext().getNumberOfParallelSubtasks()
    println("numSubtasks: " + numSubtasks)

    val subTaskIdx = getRuntimeContext().getIndexOfThisSubtask
    val taskInfo = "subtaskIdx: " + subTaskIdx
    // If > 1 task Slot in TestJob, Only the 1st Subtask sends the Notification Record msges
    var totalRecordsSent = 0
    if (numSubtasks == 1 || (numSubtasks > 1 && subTaskIdx == 0)) {
      totalRecordsSent = sendNotificationRecords(ctx, itr, taskInfo)
    }

    val endTime = System.currentTimeMillis()
    println(s"Notification-Run Complete (task: ${taskInfo}) : Took ${endTime - startTime} msecs for ${totalRecordsSent} values")
  }

  private def sendNotificationRecords(ctx: SourceFunction.SourceContext[NotificationRecord],
                                      itr: Iterator[NotificationRecord], taskInfo: String): Int = {
    var totalRecordsSent = 0
    while (isRunning && itr.hasNext) {
      val notificationRecord = itr.next()
      if (delayPerRecMillis > 0 && isDelayProcessingThisRec(notificationRecord)) {
        Thread.sleep(delayPerRecMillis)
      }
      println(taskInfo + ": send In test Notification Rec with Status: " + notificationRecord.value.getStatus)
      ctx.collect(notificationRecord)
      totalRecordsSent = totalRecordsSent + 1
    }
    totalRecordsSent
  }

  override def cancel(): Unit = {
    isRunning = false
  }

  private def isDelayProcessingThisRec(notificationRecord: NotificationRecord): Boolean = {
    var delayProcessing = false
    if (notificationRecord.value.getStatus != BatchNotification.Status.STARTED) {
      //TODO: per record Delay logic -> check notificationRec headers for header isDelay == true
      delayProcessing = true
    }
    delayProcessing
  }


}
