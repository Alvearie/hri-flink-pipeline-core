/**
 * (C) Copyright IBM Corp. 2021
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package org.alvearie.hri.flink.core

import java.nio.charset.StandardCharsets
import java.time.Instant
import java.util.concurrent.TimeUnit
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.node.ObjectNode
import com.fasterxml.jackson.module.scala.{DefaultScalaModule, ScalaObjectMapper}
import org.alvearie.hri.api.{BatchLookup, BatchNotification, InvalidRecord, MgmtClient, RequestException}
import org.alvearie.hri.flink.core.serialization.{HriRecord, NotificationRecord}
import org.apache.flink.api.common.state.MapStateDescriptor
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction
import org.apache.flink.util.{Collector, FlinkException, OutputTag}
import org.apache.http.HttpStatus
import org.apache.kafka.common.header.Headers
import org.slf4j.LoggerFactory

import java.time
import scala.util.{Failure, Success, Try}
import scala.collection.JavaConverters._
import scala.concurrent.duration.{Duration, FiniteDuration}

class ValidationProcessFunction(
                                 notificationDescriptor: MapStateDescriptor[String, BatchNotification],
                                 invalidOutputTag: OutputTag[InvalidRecord],
                                 countsOutputTag: OutputTag[ObjectNode],
                                 validator: Validator,
                                 tenantId: String,
                                 mgmtApiUrl: String,
                                 mgmtClientId: String,
                                 mgmtClientSecret: String,
                                 mgmtClientAudience: String,
                                 oauthServiceBaseUrl: String
) extends BroadcastProcessFunction[HriRecord, NotificationRecord, HriRecord] {

    val Error_Header_Key = "errorMessage"
    val Missing_Batch_ID_Err_Msg = "Bad Message - No header or batchId node"
    val Unknown_Batch_ID_Err_Msg = "Bad Message - Unknown batchId"
    val Already_Completed_Batch_ID_Err_Msg = "Bad Message - batchId is already completed"
    private val log = LoggerFactory.getLogger(this.getClass)
    @transient lazy val mapper = new ObjectMapper with ScalaObjectMapper
    mapper.registerModule(DefaultScalaModule)
    private var useMgmtApi = true

    // Trying to make the client serializable was difficult,
    // so instead generate it on demand
    @transient lazy val mgmtClient: BatchLookup = createMgmtClient()

      var initialBackOff: FiniteDuration = Duration.apply(1, TimeUnit.SECONDS)
      var maxBackOff: FiniteDuration = Duration.apply(5, TimeUnit.MINUTES)
      var maxRetry: time.Duration = java.time.Duration.ofHours(24)

    def getUseMgmtApi(): Boolean = {useMgmtApi}

    // This enables overriding for testing
    def createMgmtClient(): BatchLookup = new MgmtClient(mgmtApiUrl, mgmtClientId, mgmtClientSecret, mgmtClientAudience, oauthServiceBaseUrl)

    /**
     * constructor for testing purposes without a MgmtApiClient
     */
    def this(notificationDescriptor: MapStateDescriptor[String, BatchNotification],
             invalidOutputTag: OutputTag[InvalidRecord],
             countsOutputTag: OutputTag[ObjectNode],
             validator: Validator) {
        this(notificationDescriptor, invalidOutputTag, countsOutputTag, validator, null, null, null, null, null, null)
        useMgmtApi = false
    }

    @throws[Exception]
    def processElement(
                        record: HriRecord,
                        ctx: BroadcastProcessFunction[HriRecord, NotificationRecord, HriRecord]#ReadOnlyContext,
                        out: Collector[HriRecord]
    ): Unit = {

        val batchId = extractBatchId(record.headers)

        if (batchId == null) {
            //TODO: will need to handle pure streaming data that doesn't belong to a batch in the future
            log.error(Missing_Batch_ID_Err_Msg)

            val invalidRecord = new InvalidRecord
            invalidRecord.withFailure(Missing_Batch_ID_Err_Msg)
                .withTopic(record.topic)
                .withPartition(record.partition)
                .withOffset(record.offset.toInt)

            ctx.output(invalidOutputTag, invalidRecord)
            return
        }

        val batch = getBatch(ctx, batchId)

        if (batch == null) {
            // unknown batchId
            log.error(Unknown_Batch_ID_Err_Msg)

            val invalidRecord = new InvalidRecord
            invalidRecord.withFailure(Unknown_Batch_ID_Err_Msg)
              .withTopic(record.topic)
              .withPartition(record.partition)
              .withOffset(record.offset.toInt)
              .withBatchId(batchId)

            ctx.output(invalidOutputTag, invalidRecord)
            return
        } else {
            val status = batch.getStatus
            if (status == BatchNotification.Status.TERMINATED || status == BatchNotification.Status.FAILED) {
                val ignoreMsg = "Ignoring record from " + status + " batch: " + batchId
                log.info(ignoreMsg)
                return
            } else if (status == BatchNotification.Status.COMPLETED){
                // batchId is already completed
                log.error(Already_Completed_Batch_ID_Err_Msg)

                val invalidRecord = new InvalidRecord
                invalidRecord.withFailure(Already_Completed_Batch_ID_Err_Msg)
                  .withTopic(record.topic)
                  .withPartition(record.partition)
                  .withOffset(record.offset.toInt)
                  .withBatchId(batchId)

                ctx.output(invalidOutputTag, invalidRecord)
                return
            }
        }

        val (isValid, errMsg) = validator.isValid(record)

        if (isValid.self) {
            // send record to output topic
            out.collect(record)
        } else {
            val invalidRecord = new InvalidRecord
            invalidRecord.withFailure(errMsg)
              .withTopic(record.topic)
              .withPartition(record.partition)
              .withOffset(record.offset.toInt)
              .withBatchId(batchId)

            // send record to invalid topic With Error Msg in Header
            ctx.output(invalidOutputTag, invalidRecord)
        }

        if (batchId != null) {
            // update batch progress tracker
            val node = mapper.createObjectNode()
                .put("batchId", batchId)
                .put("isValid", isValid.self)
            ctx.output(countsOutputTag, node)
        }
    }

    private def getBatch(ctx: BroadcastProcessFunction[HriRecord, NotificationRecord, HriRecord]#ReadOnlyContext,
                 batchId: String): BatchNotification = {
        if (ctx.getBroadcastState(notificationDescriptor).contains(batchId)) {
            return ctx.getBroadcastState(notificationDescriptor).get(batchId)
        } else if (useMgmtApi) {
            return retry(mgmtClient.getBatchId(tenantId, batchId))
        }
        return null
    }

    private def retry(fn: => Try[BatchNotification]): BatchNotification = {
        var backOff = initialBackOff
        val start = Instant.now
        while(true) {
          fn match {
            case Success(b) => return b
            case Failure(ex: RequestException) =>
                val code = ex.statusCode
                if (code == HttpStatus.SC_NOT_FOUND) {
                    // Code 404 means the batch id or tenant id was not found. We don't want to fail outright because this
                    // data is provided externally, so it's likely a mistake by a data integrator. However, there also isn't
                    // a benefit to continuing to retry.
                    log.warn(s"Call to HRI Management API failed, the batch or tenant ID was not found: ${ex.getMessage:Any}")
                    return null
                }
                else if (code >= 400 && code < 500) {
                    // For any other 400 code, there is something wrong with the request (e.g. invalid credentials, invalid request)
                    // and there's no point continuing to retry. Throw an exception.
                    val msg = s"Call to HRI Management API failed: ${ex.getMessage:Any}."
                    log.error(msg)
                    throw new FlinkException(msg)
                }
                // Otherwise we'll retry
                else {
                    log.error("Call to HRI Management API failed: {}. Backing off {} milliseconds", ex.getMessage: Any, backOff: Any)

                    if (java.time.Duration.between(start, Instant.now).compareTo(maxRetry) > 0) {
                        val msg = s"Reached max HRI Management API retry timeout of ${maxRetry.getSeconds / 3600} hours"
                        log.error(msg)
                        throw new FlinkException(msg)
                    } else {
                        Thread.sleep(backOff.toMillis)
                        backOff = maxBackOff.min(backOff * 2)
                    }
                }
            case Failure(e) =>
                log.error("unexpected exception from mgmtClient", e)
                throw new FlinkException(e)
          }
        }
        return null //unreachable code
    }

    @throws[Exception]
    def processBroadcastElement(
                                 record: NotificationRecord,
                                 ctx: BroadcastProcessFunction[HriRecord, NotificationRecord, HriRecord]#Context,
                                 out: Collector[HriRecord]
    ): Unit = {
        val batchCountObj = record.value.getId
        log.info("Adding notificationRec for batchId: " + batchCountObj +
          " with status: " + record.value.getStatus + " to broadcastState")
        ctx.getBroadcastState(notificationDescriptor).put(batchCountObj, record.value)
    }

    def extractBatchId(headers: Headers): String = {
        if (headers != null) {
            return headers.asScala.find(_.key() == "batchId")
              .map(h => new String(h.value(), StandardCharsets.UTF_8)).orNull
        }
        null
    }

}
