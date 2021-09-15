/**
 * (C) Copyright IBM Corp. 2021
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package org.alvearie.hri.flink.core

import com.fasterxml.jackson.databind.DeserializationFeature
import com.fasterxml.jackson.databind.json.JsonMapper
import com.fasterxml.jackson.databind.node.ObjectNode
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule
import org.alvearie.hri.api.BatchNotification
import org.alvearie.hri.flink.core.BaseValidationJob.getStateTtlConfig
import org.alvearie.hri.flink.core.serialization.NotificationRecord
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction
import org.apache.flink.util.Collector
import org.slf4j.LoggerFactory

/**
 * the data type stored in the state
 * @param valid the number of valid records
 * @param invalid the number of invalid records
 * @param isFinished flag to indicate either the batch has been failed|completed or not
 * @param lastModified timestamp of the last record stored
 * @param batchNotification the batch notification
 */
case class BatchState(valid: Int, invalid: Int, isFinished: Boolean, lastModified: Long, batchNotification: BatchNotification)

class Tracker(timeout:Long = 300000)
  extends KeyedCoProcessFunction[String, ObjectNode, NotificationRecord, NotificationRecord] {

  @transient private lazy val mapper = JsonMapper.builder()
    .addModule(new JavaTimeModule())
    .disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES)
    .build()

  private val log = LoggerFactory.getLogger(this.getClass)

  private val batchDescriptor = new ValueStateDescriptor[BatchState](
    "batchStatus", TypeInformation.of(classOf[BatchState])
  )
  batchDescriptor.enableTimeToLive(getStateTtlConfig)

  @throws[Exception]
  override def processElement1(value: ObjectNode,
                               ctx: KeyedCoProcessFunction[String, ObjectNode, NotificationRecord, NotificationRecord]#Context,
                               out: Collector[NotificationRecord]): Unit = {

    val batchStatus: ValueState[BatchState] = getRuntimeContext.getState(batchDescriptor)
    val batchId = value.get("batchId").asText("")
    val isValid = value.get("isValid").asBoolean()

    val batchState = batchStatus.value() match {
      case null =>
        BatchState(if (isValid) 1 else 0, if (!isValid)  1 else 0, isFinished = false, ctx.timerService().currentProcessingTime(), null)
      case BatchState(valid, invalid, isFinished, _, batchNotification) =>
        BatchState(if (isValid) valid + 1 else valid, if (!isValid) invalid + 1 else invalid, isFinished, ctx.timerService().currentProcessingTime(), batchNotification)
    }

    batchStatus.update(batchState)

    log.debug("Batch: {}, validCount: {}, invalidCount: {}, isFinished: {}, lastModified: {}", batchId, batchStatus.value().valid.toString,
               batchStatus.value().invalid.toString, batchStatus.value().isFinished.toString, batchStatus.value().lastModified.toString)

    batchStatusCheck(batchStatus, ctx, out)
  }

  @throws[Exception]
  override def processElement2(record: NotificationRecord,
                               ctx: KeyedCoProcessFunction[String, ObjectNode, NotificationRecord, NotificationRecord]#Context,
                               out: Collector[NotificationRecord]): Unit = {

    val batchStatus: ValueState[BatchState] = getRuntimeContext.getState(batchDescriptor)
    val batchNotification = record.value
    log.info("Adding notificationRec for batchId: {} with status: {}", batchNotification.getId:Any, batchNotification.getStatus.toString:Any)

    val batchState = batchStatus.value() match {
      case null =>
        BatchState(0, 0, isFinished = false, ctx.timerService().currentProcessingTime(), batchNotification)
      case BatchState(valid, invalid, isFinished, _, _) =>
        BatchState(valid, invalid, isFinished, ctx.timerService().currentProcessingTime(), batchNotification)
    }

    batchStatus.update(batchState)

    batchStatusCheck(batchStatus, ctx, out)
  }

  // this method is called when clock time reaches the specified time
  override def onTimer( timestamp: Long,
                        ctx: KeyedCoProcessFunction[String, ObjectNode, NotificationRecord, NotificationRecord]#OnTimerContext,
                        out: Collector[NotificationRecord]): Unit = {

    val batchStatus: ValueState[BatchState] = getRuntimeContext.getState(batchDescriptor)
    val notification = batchStatus.value().batchNotification
    log.info("Timer registered at: {}", timestamp)
    if (isBatchComplete(notification, batchStatus)) {
      outputNotification(BatchNotification.Status.COMPLETED, batchStatus, out, msg = null)
    }
  }

  def batchStatusCheck(batchStatus: ValueState[BatchState],
                      ctx: KeyedCoProcessFunction[String, ObjectNode, NotificationRecord, NotificationRecord]#Context,
                      out: Collector[NotificationRecord]): Unit = {

    val notification = batchStatus.value().batchNotification
    if (notification != null) {
      log.debug("Notification Message -> checking if batch {} is failed|completed; status: {}", notification.getId:Any, notification.getStatus:Any)
      if (tooManyInvalidRecords(notification, batchStatus)) {
        val msg = s"Failing Batch: ${notification.getId}, too many invalid records invalidCount: ${batchStatus.value().invalid} == invalidThreshold: ${notification.getInvalidThreshold}"
        log.info(msg)
        outputNotification(BatchNotification.Status.FAILED, batchStatus, out, msg)
      } else if (isBatchComplete(notification, batchStatus)) {
          // schedule the next timer timeout from the current processing time
          ctx.timerService.registerProcessingTimeTimer(batchStatus.value().lastModified + timeout)
          log.info("Scheduling Timer for Batch: {}, at: {}, actualRecordCount: {} == expectedRecordCount: {}",
              notification.getId, timeout.toString, (batchStatus.value().valid + batchStatus.value().invalid).toString, notification.getExpectedRecordCount.toString )
      } else if (moreRecordsThanExpected(notification, batchStatus)) {
        val msg = s"Failing batch: ${notification.getId}, received too many records, actualRecordCount: ${batchStatus.value().valid + batchStatus.value().invalid} > expectedRecordCount: ${notification.getExpectedRecordCount}"
        log.warn(msg)
        outputNotification(BatchNotification.Status.FAILED, batchStatus, out, msg)
      }
    }
  }

  def isBatchComplete(currentNotification: BatchNotification, state: ValueState[BatchState]): Boolean = {
    val status = currentNotification.getStatus

    if (status == BatchNotification.Status.SEND_COMPLETED && !state.value().isFinished) {
      val expectedCount = currentNotification.getExpectedRecordCount

      return state.value().valid + state.value().invalid == expectedCount
    }

    false
  }

  def moreRecordsThanExpected(currentNotification: BatchNotification, state: ValueState[BatchState]): Boolean = {
    val status = currentNotification.getStatus

    if (status == BatchNotification.Status.SEND_COMPLETED && !state.value().isFinished) {
      val expectedCount = currentNotification.getExpectedRecordCount

      return state.value().valid + state.value().invalid > expectedCount
    }

    false
  }

  def tooManyInvalidRecords(currentNotification: BatchNotification, state: ValueState[BatchState]): Boolean = {
    val status = currentNotification.getStatus

    if (currentNotification.getInvalidThreshold > -1 && status != BatchNotification.Status.TERMINATED && !state.value().isFinished) {

      return state.value().invalid == currentNotification.getInvalidThreshold
    }

    false
  }

  def createNotificationMsg(currentNotification: BatchNotification,
                            status: BatchNotification.Status,
                            validRecordCount: Int,
                            invalidRecordCount: Int,
                            failureMessage: String): NotificationRecord = {
    if (currentNotification != null) {
      val newNotification = mapper.readValue(mapper.writeValueAsString(currentNotification), classOf[BatchNotification])

      newNotification
        .withStatus(status)
        .withActualRecordCount(validRecordCount + invalidRecordCount)
        .withInvalidRecordCount(invalidRecordCount)

      if (failureMessage != null) {
        newNotification.withFailureMessage(failureMessage)
      }
      new NotificationRecord(null, null, newNotification)
    } else {
      null
    }
  }

  // This prevents more than one failed|completed message to be output, which causes an error when calling the HRI Management API
  def outputNotification(status: BatchNotification.Status, state: ValueState[BatchState], out: Collector[NotificationRecord], msg: String): Unit ={
    if (!state.value().isFinished){
      val outNotification = createNotificationMsg(state.value().batchNotification, status, state.value().valid, state.value().invalid, msg)
      val batchState = BatchState(state.value().valid, state.value().invalid, isFinished = true, state.value().lastModified, state.value().batchNotification)
      state.update(batchState)
      out.collect(outNotification)
    }
  }

}
