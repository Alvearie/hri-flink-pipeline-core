/**
 * (C) Copyright IBM Corp. 2021
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package org.alvearie.hri.api

import org.apache.http.client.entity.UrlEncodedFormEntity
import org.apache.http.client.methods.{CloseableHttpResponse, HttpGet, HttpPost, HttpPut, HttpRequestBase}
import org.apache.http.entity.{ContentType, StringEntity}
import org.apache.http.impl.client.{CloseableHttpClient, HttpClients}
import org.apache.http.message.BasicNameValuePair
import org.apache.http.{HttpEntity, HttpStatus, NameValuePair}
import org.apache.http.util.EntityUtils
import com.fasterxml.jackson.databind.{DeserializationFeature, JsonNode, ObjectMapper}
import com.fasterxml.jackson.databind.json.JsonMapper
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.alvearie.hri.flink.core.serialization.NotificationDeserializer
import org.apache.http.ssl.SSLContexts

import java.util.Base64
import java.util.ArrayList
import org.slf4j.LoggerFactory

import java.io.{File, FileNotFoundException}
import scala.util.{Failure, Success, Try}

class MgmtClient(val baseUri: String, val clientId: String, val clientSecret: String, val audience: String, val oauthServiceBaseUrl: String,
                 val httpClient: CloseableHttpClient = MgmtClient.createHttpClient()) extends Serializable with BatchLookup {
  private val log = LoggerFactory.getLogger(this.getClass)
  log.info("Creating HRI MgmtClient for {}", baseUri)

  @transient lazy private val mapper = new ObjectMapper().registerModule(DefaultScalaModule)

  @transient private lazy val jsonMapper = JsonMapper.builder()
    .addModule(new JavaTimeModule())
    .disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES)
    .build()

  val notificationDeserializer: NotificationDeserializer = new NotificationDeserializer

  def processingComplete(tenantId: String, batchId: String, actualCount: Int, invalidCount: Int): Try[Unit] = {
    log.info("processingComplete for tenant: {}, batch: {}, actualCount: {}, invalidCount {}",
      tenantId, batchId, actualCount.toString, invalidCount.toString)
    val body = Map(
      MgmtClient.actualRecordCountField -> actualCount,
      MgmtClient.invalidRecordCountField -> invalidCount,
    )

    val httpPut = new HttpPut(s"$baseUri/tenants/$tenantId/batches/$batchId/action/processingComplete")
    val accessTokenTry = getOauthAccessToken(tenantId)
    if (accessTokenTry.isFailure) {
      return Failure(accessTokenTry.failed.get)
    }

    httpPut.addHeader(MgmtClient.oauthHeaderField, "Bearer " + accessTokenTry.get)
    val entity = new StringEntity(mapper.writeValueAsString(body), ContentType.APPLICATION_JSON)
    httpPut.setEntity(entity)

    executeRequest(httpPut, _ => Unit)
  }

  def failBatch(tenantId: String, batchId: String, actualCount: Int, invalidCount: Int, failureMessage: String): Try[Unit] = {
    log.info("Failure for tenant: {}, batch: {}, actualCount: {}, invalidCount: {}, msg: {}",
      tenantId, batchId, actualCount.toString, invalidCount.toString, failureMessage)
    val body = Map(
      MgmtClient.actualRecordCountField -> actualCount,
      MgmtClient.invalidRecordCountField -> invalidCount,
      MgmtClient.failureMessageField -> failureMessage,
    )

    val httpPut = new HttpPut(s"$baseUri/tenants/$tenantId/batches/$batchId/action/fail")
    val accessTokenTry = getOauthAccessToken(tenantId)
    if (accessTokenTry.isFailure) {
      return Failure(accessTokenTry.failed.get)
    }

    httpPut.addHeader(MgmtClient.oauthHeaderField, "Bearer " + accessTokenTry.get)
    val entity = new StringEntity(mapper.writeValueAsString(body), ContentType.APPLICATION_JSON)
    httpPut.setEntity(entity)

    executeRequest(httpPut, _ => Unit)
  }

  def getBatchId(tenantId: String, batchId: String): Try[BatchNotification] = {
    val httpGet = new HttpGet(s"$baseUri/tenants/$tenantId/batches/$batchId")
    val accessTokenTry = getOauthAccessToken(tenantId)
    if (accessTokenTry.isFailure) {
      return Failure(accessTokenTry.failed.get)
    }

    httpGet.addHeader(MgmtClient.oauthHeaderField, "Bearer " + accessTokenTry.get)

    executeRequest(httpGet, entity => jsonMapper.readValue(entity.getContent, classOf[BatchNotification]))
  }

  private def getOauthAccessToken(tenantId: String): Try[String] = {

    val tokenRequestUrl = s"$oauthServiceBaseUrl/token"
    val tokenRequest = new HttpPost(tokenRequestUrl)

    // Convert clientId and clientSecret to base64, and add headers
    val credentials = new String(Base64.getEncoder.encode(s"$clientId:$clientSecret".getBytes))
    tokenRequest.addHeader(MgmtClient.oauthHeaderField, s"Basic $credentials")
    tokenRequest.addHeader(MgmtClient.contentTypeHeaderField, MgmtClient.contentTypeHeaderValue)

    // Add grant_type and scope parameters
    val nameValuePairs = new ArrayList[NameValuePair](3)
    nameValuePairs.add(new BasicNameValuePair(MgmtClient.grantTypeField, MgmtClient.grantTypeValue))
    nameValuePairs.add(new BasicNameValuePair(MgmtClient.scopeField, s"${MgmtClient.hriInternalScope} ${MgmtClient.hriConsumerScope} tenant_$tenantId"))
    nameValuePairs.add(new BasicNameValuePair(MgmtClient.audienceField, audience))
    tokenRequest.setEntity(new UrlEncodedFormEntity(nameValuePairs))

    // Execute the request, and extract the access_token from the response
    executeRequest(tokenRequest, entity => {
      val responseBody = EntityUtils.toString(entity)
      val node : JsonNode = jsonMapper.readTree(responseBody)
      node.get(MgmtClient.accessTokenField).asText
    }) match {
      case Success(accessToken) => Success(accessToken)
      case Failure(ex) => Failure(ex)
    }
  }

  def executeRequest[A](request: HttpRequestBase, entityMapper: HttpEntity => A): Try[A] = {
    var response: CloseableHttpResponse = null
    try {
      response = httpClient.execute(request)
      response.getStatusLine.getStatusCode match {
        case HttpStatus.SC_OK =>
          log.info("HRI MgmtApi action call successful")
          Success(entityMapper(response.getEntity))
        case status =>
          val msg = status.intValue + ": " + EntityUtils.toString(response.getEntity)
          log.info("HRI MgmtApi action call failed: {}", msg)
          Failure(new RequestException(msg, status))
      }
    } catch {
      case ex: Throwable => Failure(ex)
    } finally {
      if (response != null) response.close()
    }
  }
}

class RequestException(errMsg: String = null, val statusCode: Integer) extends Exception(errMsg)


object MgmtClient {
  val actualRecordCountField = "actualRecordCount"
  val invalidRecordCountField = "invalidRecordCount"
  val failureMessageField = "failureMessage"
  val oauthHeaderField = "Authorization"
  val contentTypeHeaderField = "Content-Type"
  val contentTypeHeaderValue = "application/x-www-form-urlencoded"
  val grantTypeField = "grant_type"
  val grantTypeValue = "client_credentials"
  val scopeField = "scope"
  val hriInternalScope = "hri_internal"
  val hriConsumerScope = "hri_consumer"
  val accessTokenField = "access_token"
  val audienceField = "audience"

  val trustStoreEnv = "HRI_TRUSTSTORE"
  val trustStorePasswordEnv = "HRI_TRUSTSTORE_PASSWORD"

  /**
   * If 'HRI_TRUSTSTORE' and 'HRI_TRUSTSTORE_PASSWORD' are set, constructs an Http client using the specified trust
   * store. If not set, creates a default Http client.
   * If unable to load the trust store or create the client, an Exception is thrown.
   * @return a Http client
   */
  def createHttpClient() : CloseableHttpClient = {
    val log = LoggerFactory.getLogger(this.getClass)
    val trustStorePath = System.getenv(trustStoreEnv)
    val password = System.getenv(trustStorePasswordEnv)

    if( trustStorePath == null || trustStorePath.isEmpty ) {
      log.info("HRI_TRUSTSTORE is not set, so creating default Http client")
      return HttpClients.createDefault()
    } else if ( password == null || password.isEmpty ) {
      val msg = trustStoreEnv + " is set, but " + trustStorePasswordEnv + " is not. Both must be empty or set."
      log.error(msg)
      throw new IllegalArgumentException(msg)
    }
    log.info("Creating Http client with trust store {}", trustStorePath)

    val trustStoreFile = new File(trustStorePath);
    if (!trustStoreFile.exists() || !trustStoreFile.isFile) {
      val msg = "Not found or not a file: " + trustStoreFile.getPath
      log.error(msg);
      throw new FileNotFoundException(msg);
    }

    val sslContext = SSLContexts.custom
      .loadTrustMaterial(trustStoreFile, password.toCharArray)
      .build

    return HttpClients.custom
      .setSSLContext(sslContext)
      .build
  }
}
