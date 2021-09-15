/**
 * (C) Copyright IBM Corp. 2021
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package org.alvearie.hri.flink.core.jobtest

import java.nio.charset.StandardCharsets
import java.util.concurrent.Callable

import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import org.alvearie.hri.api.{BatchLookup, InvalidRecord}
import org.alvearie.hri.flink.core.serialization.{HriRecord, NotificationRecord}
import org.alvearie.hri.flink.core.{BaseValidationJob, Validator}
import org.apache.commons.lang3.StringUtils
import org.apache.commons.lang3.exception.ExceptionUtils
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction
import picocli.CommandLine

import scala.runtime.RichBoolean
import scala.util.{Failure, Success, Try}

@CommandLine.Command(
  name = "validateTest",
  synopsisSubcommandLabel = "COMMAND",
  mixinStandardHelpOptions = true,
  version = Array("1.0"),
  description = Array("HRI JSON-only TEST validation job")
)
class JsonValidationJob extends Callable[Integer] with Serializable {
  @CommandLine.Option(names = Array("-b", "--brokers"), split = ",", description = Array("Comma-separated list of Event Streams (Kafka) brokers"), required = true)
  private var brokers: Array[String] = null

  @CommandLine.Option(names = Array("-p", "--password"), description = Array("IBM Cloud Event Streams password"), hidden = true, required = false)
  private var password: String = null

  @CommandLine.Option(names = Array("-i", "--input"), description = Array("IBM Cloud Event Streams (Kafka) input topic"), required = true)
  private var inputTopic: String = null

  @CommandLine.Option(names = Array("-d", "--batch-delay"), defaultValue = "300000", description = Array("Amount of time to wait in milliseconds for extra records before completing a batch. Default ${DEFAULT-VALUE}"))
  private var batchCompletionDelay: Long = _

  override def call(): Integer = {
    val baseValidationJob = new BaseValidationJob(inputTopic, brokers, password,
      new TestValidator(), batchCompletionDelay)

    baseValidationJob.startJob("JSON Schema Validation")

    return 0
  }

  def runJob(notificationSrc: RichParallelSourceFunction[NotificationRecord],
             dataRecsSrc: RichParallelSourceFunction[HriRecord],
             validRecsSink: RichSinkFunction[HriRecord],
             invalidRecsSink: RichSinkFunction[InvalidRecord],
             recCountSink: RichSinkFunction[NotificationRecord],
             mgmtClient: BatchLookup): Unit = {

    val baseValidationJob = new BaseValidationJob(new TestValidator(),
      notificationSrc, dataRecsSrc, validRecsSink, invalidRecsSink, recCountSink, mgmtClient, batchCompletionDelay)

    baseValidationJob.startJob("Test JSON Schema Validation With Test Src/Sinks")
  }
}

object JsonValidationJob {

  def main(args: Array[String]): Unit = {
    val command = new CommandLine(new JsonValidationJob()).execute(args:_*)
    System.exit(command)
  }
}

class TestValidator() extends Validator with Serializable {
  override def isValid(record: HriRecord): (RichBoolean, String) = {
    val recordBody = new String(record.value, StandardCharsets.UTF_8)
    val valResult = Try(getJsonNodeFromStringContent(recordBody))
    val errMsg: String = valResult match {
      case Success(node) => StringUtils.EMPTY
      case Failure(ex) => ExceptionUtils.getStackTrace(ex)
    }

    (StringUtils.isEmpty(errMsg), errMsg)
  }

  def getJsonNodeFromStringContent(content: String): JsonNode = {
    if (StringUtils.isEmpty(content)) {
      throw new Exception("Cannot get Json for an Empty/null String")
    }
    val mapper = new ObjectMapper()
    mapper.readTree(content)
  }
}