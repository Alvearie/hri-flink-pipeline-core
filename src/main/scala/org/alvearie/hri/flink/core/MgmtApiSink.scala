/**
 * (C) Copyright IBM Corp. 2021
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package org.alvearie.hri.flink.core

import java.time.Instant
import java.util.concurrent.TimeUnit

import org.alvearie.hri.api.BatchNotification.Status
import org.alvearie.hri.api.{MgmtClient, RequestException}
import org.alvearie.hri.flink.core.serialization.NotificationRecord
import org.apache.flink.streaming.api.functions.sink.SinkFunction
import org.apache.flink.streaming.api.functions.sink.SinkFunction.Context
import org.apache.flink.util.FlinkException
import org.slf4j.LoggerFactory

import scala.concurrent.duration.Duration
import scala.util.{Failure, Success, Try}

class MgmtApiSink(val tenantId: String, val mgmtApiUrl: String, val mgmtClientId: String, val mgmtClientSecret: String, val mgmtClientAudience: String, val oauthServiceBaseUrl: String) extends SinkFunction[NotificationRecord]{
  private val log = LoggerFactory.getLogger(this.getClass)
  var initialBackOff = Duration.apply(1, TimeUnit.SECONDS)
  var maxBackOff = Duration.apply(5, TimeUnit.MINUTES)
  var maxRetry = java.time.Duration.ofHours(24)

  // Trying to make the client serializable was difficult,
  // so instead generate it on demand
  @transient lazy val mgmtClient: MgmtClient = createMgmtClient()

  // This enables overriding for testing
  def createMgmtClient(): MgmtClient = {
    try {
      new MgmtClient(mgmtApiUrl, mgmtClientId, mgmtClientSecret, mgmtClientAudience, oauthServiceBaseUrl)
    } catch {
      case ex: Throwable =>
        throw new FlinkException(ex) // can't recover from this
    }
  }

  override def invoke(record: NotificationRecord, context: Context[_]): Unit = {
    val batch = record.value
    log.info("Received Batch {} with status of {}", batch.getId: Any, batch.getStatus: Any)
    batch.getStatus match {
      case Status.COMPLETED => retry(mgmtClient.processingComplete(tenantId, batch.getId, batch.getActualRecordCount, batch.getInvalidRecordCount))
      case Status.FAILED => retry(mgmtClient.failBatch(tenantId, batch.getId, batch.getActualRecordCount, batch.getInvalidRecordCount, batch.getFailureMessage))
      case _ =>
        val msg = s"Received unexpected status: '${batch.getStatus}' for batch: '${batch.getId}'"
        log.error(msg)
        throw new FlinkException(msg) // can't recover from this because the data is bad
    }
  }

  // retry until 'maxRetry' with exponential backoff
  def retry(fn: => Try[Unit]): Unit = {
    var backOff = initialBackOff
    val start = Instant.now
    while(true) {
      fn match {
        case Success(_) => return
        case Failure(ex) =>
          if (ex.isInstanceOf[RequestException]) {
            val code = ex.asInstanceOf[RequestException].statusCode
            if (code == 409) {
              // In the case of a 409 (conflict), the batch somehow already got to the desired state. There's
              // no point continuing to retry or throwing an exception, we'll just return.
              log.warn(s"Call to HRI Management API did not change the batch state because the batch is already in the desired state: ${ex.getMessage:Any}")
              return
            }
            else if (code >= 400 && code < 500) {
              // For any other 400 code, there is something wrong with the request (e.g. invalid credentials, invalid request)
              // and there's no point continuing to retry. Throw an exception.
              val msg = s"Call to HRI Management API failed: ${ex.getMessage:Any}."
              log.error(msg)
              throw new FlinkException(msg)
            }
            // Otherwise we'll retry
          }
          log.error("Call to HRI Management API failed: {}. Backing off {} milliseconds", ex.getMessage:Any, backOff:Any)
          if(java.time.Duration.between(start, Instant.now).compareTo(maxRetry) > 0 ){
            val msg = s"Reached max HRI Management API retry timeout of ${maxRetry.getSeconds/3600} hours"
            log.error(msg)
            throw new FlinkException(msg)
          } else {
            Thread.sleep(backOff.toMillis)
            backOff = maxBackOff.min(backOff*2)
          }
      }
    }
  }

}
