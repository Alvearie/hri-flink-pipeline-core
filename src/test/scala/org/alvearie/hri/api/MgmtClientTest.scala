/**
 * (C) Copyright IBM Corp. 2021
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package org.alvearie.hri.api

import java.time.temporal.ChronoUnit
import java.time.{OffsetDateTime, ZoneOffset}
import java.util.Base64

import org.apache.http.{Header, HttpStatus, HttpVersion, ProtocolVersion}
import org.apache.http.message.BasicHttpResponse
import org.apache.http.client.methods.{CloseableHttpResponse, HttpPost, HttpPut, HttpUriRequest}
import org.apache.http.entity.{ContentType, StringEntity}
import org.apache.http.impl.client.CloseableHttpClient
import org.apache.http.util.EntityUtils
import com.fasterxml.jackson.databind.DeserializationFeature
import com.fasterxml.jackson.databind.json.JsonMapper
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule
import org.mockito.ArgumentMatcher
import org.mockito.scalatest.MockitoSugar
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers._

import scala.util.{Failure, Success}

class MgmtClientTest extends AnyFunSuite with MockitoSugar{

  private val baseUrl = "https://mydomain.com/hri"
  private val tenantId = "myTenant"
  private val batchId = "myBatch"
  private val oauthServiceBaseUrl = "https://oauthdomain.com/hri"
  private val clientId = "myClientId"
  private val clientSecret = "myClientSecret"
  private val credentials = new String(Base64.getEncoder.encode(s"$clientId:$clientSecret".getBytes))
  private val accessToken = "ABCDE"
  private val accessTokenField = "access_token"
  private val authorizationHeaderName = "Authorization"
  private val audience = "myAudience"
  private val expectedTokenRequestParams = Array("grant_type=client_credentials", "scope=", s"${MgmtClient.hriInternalScope}", s"${MgmtClient.hriConsumerScope}", s"tenant_$tenantId", s"audience=$audience")

  class RequestMatcherPut(uri: String, bodyElements: Seq[String]) extends ArgumentMatcher[HttpUriRequest] {
    override def matches(request: HttpUriRequest): Boolean = {
      if (request.getMethod != "PUT") return false
      val putRequest = request.asInstanceOf[HttpPut]
      val body = EntityUtils.toString(putRequest.getEntity)
      bodyElements.foreach( element => if (!body.contains(element)) return false)
      val requestHeaders = request.getAllHeaders
      request.getURI.toString() == uri &&
        requestHeaders.size == 1 &&
        requestHeaders.head.getName() == authorizationHeaderName &&
        requestHeaders.head.getValue() == s"Bearer $accessToken"
    }
  }

  class RequestMatcherGet(uri: String) extends ArgumentMatcher[HttpUriRequest] {
    override def matches(request: HttpUriRequest): Boolean = {
      val requestHeaders = request.getAllHeaders
      request.getMethod == "GET" &&
        request.getURI.toString() == uri &&
        requestHeaders.size == 1 &&
        requestHeaders.head.getName() == authorizationHeaderName &&
        requestHeaders.head.getValue() == s"Bearer $accessToken"
    }
  }

  class RequestMatcherPost(uri: String) extends ArgumentMatcher[HttpUriRequest] {
    override def matches(request: HttpUriRequest): Boolean = {
      if (request.getMethod != "POST") return false

      // Verify Content-Type and Authorization headers both present
      val requestHeaders = request.getAllHeaders
      var authHeaderFound = false
      var contentTypeHeaderFound = false
      if (requestHeaders.size == 2) {
        for (i <- 0 to 1) {
          val header : Header = requestHeaders(i)
          if (header.getName == authorizationHeaderName && header.getValue == s"Basic $credentials") {
            authHeaderFound = true
          } else if (header.getName == "Content-Type" && header.getValue == "application/x-www-form-urlencoded") {
            contentTypeHeaderFound = true
          }
        }
      }
      val correctHeaders = authHeaderFound && contentTypeHeaderFound

      // Make sure the body matches
      val postRequest = request.asInstanceOf[HttpPost]
      val entity = EntityUtils.toString(postRequest.getEntity())
      expectedTokenRequestParams.foreach( element => if (!entity.contains(element)) return false)

      request.getURI.toString() == uri &&
        correctHeaders
    }
  }

  private val mapper = JsonMapper.builder()
    .addModule(new JavaTimeModule())
    .disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES)
    .build()

  test("it should return a BatchNotification without exceptions on successful getBatchId call") {
    val mockClient = mock[CloseableHttpClient]

    val getMatcher = new RequestMatcherGet(
      s"$baseUrl/tenants/$tenantId/batches/$batchId",
    )

    val postMatcher = new RequestMatcherPost(
      s"$oauthServiceBaseUrl/token"
    )

    val batch = new BatchNotification()
      .withId(batchId)
      .withName("testBatch")
      .withTopic("testTopic")
      .withDataType("testType")
      .withStartDate(OffsetDateTime.now(ZoneOffset.UTC).truncatedTo(ChronoUnit.SECONDS))
      .withStatus(BatchNotification.Status.STARTED)
      .withInvalidThreshold(5)

    val httpResponse = new FakeHttpResponse(HttpVersion.HTTP_1_1, HttpStatus.SC_OK, "OK")
    httpResponse.setEntity(new StringEntity(mapper.writeValueAsString(batch), ContentType.APPLICATION_JSON))
    doReturn(httpResponse).when(mockClient).execute(argThat(getMatcher))

    val httpTokenResponse = new FakeHttpResponse(HttpVersion.HTTP_1_1, HttpStatus.SC_OK, "OK")
    httpTokenResponse.setEntity(new StringEntity(s""" {"$accessTokenField": "$accessToken"} """))
    doReturn(httpTokenResponse).when(mockClient).execute(argThat(postMatcher))

    val mgmtClient = new MgmtClient(baseUrl, clientId, clientSecret, audience, oauthServiceBaseUrl, mockClient)
    mgmtClient.getBatchId(tenantId, batchId) match {
      case Success(batchNotification) => batchNotification should equal(batch)
      case Failure(ex) => fail(ex)
    }
  }

  test("it should return a BatchNotification with UTF8 Characters on successful getBatchId call") {
    val mockClient = mock[CloseableHttpClient]

    val getMatcher = new RequestMatcherGet(
      s"$baseUrl/tenants/$tenantId/batches/$batchId",
    )

    val postMatcher = new RequestMatcherPost(
      s"$oauthServiceBaseUrl/token"
    )

    val batch = new BatchNotification()
      .withId(batchId)
      .withName("testBatch中文")
      .withTopic("testTopic")
      .withDataType("testType中文")
      .withStartDate(OffsetDateTime.now(ZoneOffset.UTC).truncatedTo(ChronoUnit.SECONDS))
      .withStatus(BatchNotification.Status.STARTED)
      .withInvalidThreshold(5)

    val httpResponse = new FakeHttpResponse(HttpVersion.HTTP_1_1, HttpStatus.SC_OK, "OK")
    httpResponse.setEntity(new StringEntity(mapper.writeValueAsString(batch), ContentType.APPLICATION_JSON))
    doReturn(httpResponse).when(mockClient).execute(argThat(getMatcher))

    val httpTokenResponse = new FakeHttpResponse(HttpVersion.HTTP_1_1, HttpStatus.SC_OK, "OK")
    httpTokenResponse.setEntity(new StringEntity(s""" {"$accessTokenField": "$accessToken"} """))
    doReturn(httpTokenResponse).when(mockClient).execute(argThat(postMatcher))

    val mgmtClient = new MgmtClient(baseUrl, clientId, clientSecret, audience, oauthServiceBaseUrl, mockClient)
    mgmtClient.getBatchId(tenantId, batchId) match {
      case Success(batchNotification) => batchNotification should equal(batch)
      case Failure(ex) => fail(ex)
    }
  }

  test("it should throw RequestException when getBatchId responds with non 200") {
    val mockClient = mock[CloseableHttpClient]

    val getMatcher = new RequestMatcherGet(
      s"$baseUrl/tenants/$tenantId/batches/$batchId",
    )

    val postMatcher = new RequestMatcherPost(
      s"$oauthServiceBaseUrl/token"
    )

    val httpResponse = new FakeHttpResponse(HttpVersion.HTTP_1_1, HttpStatus.SC_BAD_REQUEST, "BAD REQUEST")
    httpResponse.setEntity(new StringEntity("{\"errorEventId\":\"adf01715c1d24292b01715c1d2429218\",\"errorDescrption\":\"Bad Request\"}"))
    doReturn(httpResponse).when(mockClient).execute(argThat(getMatcher))

    val httpTokenResponse = new FakeHttpResponse(HttpVersion.HTTP_1_1, HttpStatus.SC_OK, "OK")
    httpTokenResponse.setEntity(new StringEntity(s""" {"$accessTokenField": "$accessToken"} """))
    doReturn(httpTokenResponse).when(mockClient).execute(argThat(postMatcher))

    val mgmtClient = new MgmtClient(baseUrl, clientId, clientSecret, audience, oauthServiceBaseUrl, mockClient)
    mgmtClient.getBatchId(tenantId, batchId) match {
      case Success(_) => fail("expected Exception but got success")
      case Failure(ex) => //expected failure
    }
  }

  test("it should throw RequestException when request for access token fails in getBatchId") {
    val mockClient = mock[CloseableHttpClient]

    val postMatcher = new RequestMatcherPost(
      s"$oauthServiceBaseUrl/token"
    )

    val httpTokenResponse = new FakeHttpResponse(HttpVersion.HTTP_1_1, HttpStatus.SC_BAD_REQUEST, "BAD REQUEST")
    httpTokenResponse.setEntity(new StringEntity("{\"errorEventId\":\"adf01715c1d24292b01715c1d2429218\",\"errorDescrption\":\"Bad Request\"}"))
    doReturn(httpTokenResponse).when(mockClient).execute(argThat(postMatcher))

    val mgmtClient = new MgmtClient(baseUrl, clientId, clientSecret, audience, oauthServiceBaseUrl, mockClient)
    mgmtClient.getBatchId(tenantId, batchId) match {
      case Success(_) => fail("expected Exception but got success")
      case Failure(ex) => //expected failure
    }
  }

  test("it should return without exceptions on successful processingComplete call") {
    val mockClient = mock[CloseableHttpClient]

    val putMatcher = new RequestMatcherPut(
      s"$baseUrl/tenants/$tenantId/batches/$batchId/action/processingComplete",
      Seq[String]("\"actualRecordCount\":15", "\"invalidRecordCount\":5")
    )

    val postMatcher = new RequestMatcherPost(
      s"$oauthServiceBaseUrl/token"
    )

    val httpResponse = new FakeHttpResponse(HttpVersion.HTTP_1_1, HttpStatus.SC_OK, "OK")
    httpResponse.setEntity(new StringEntity(""))
    doReturn(httpResponse).when(mockClient).execute(argThat(putMatcher))

    val httpTokenResponse = new FakeHttpResponse(HttpVersion.HTTP_1_1, HttpStatus.SC_OK, "OK")
    httpTokenResponse.setEntity(new StringEntity(s""" {"$accessTokenField": "$accessToken"} """))
    doReturn(httpTokenResponse).when(mockClient).execute(argThat(postMatcher))

    val mgmtClient = new MgmtClient(baseUrl, clientId, clientSecret, audience, oauthServiceBaseUrl, mockClient)
    mgmtClient.processingComplete(tenantId, batchId, 15, 5) match {
      case Success(_) =>
      case Failure(ex) => fail(ex)
    }
  }

  test("it should throw RequestException when processingComplete responds with non 200") {
    val mockClient = mock[CloseableHttpClient]

    val putMatcher = new RequestMatcherPut(
      s"$baseUrl/tenants/$tenantId/batches/$batchId/action/processingComplete",
      Seq[String]("\"actualRecordCount\":15", "\"invalidRecordCount\":10")
    )

    val postMatcher = new RequestMatcherPost(
      s"$oauthServiceBaseUrl/token"
    )

    val httpResponse = new FakeHttpResponse(HttpVersion.HTTP_1_1, HttpStatus.SC_BAD_REQUEST, "BAD REQUEST")
    httpResponse.setEntity(new StringEntity("{\"errorEventId\":\"adf01715c1d24292b01715c1d2429218\",\"errorDescrption\":\"Bad Request\"}"))
    doReturn(httpResponse).when(mockClient).execute(argThat(putMatcher))

    val httpTokenResponse = new FakeHttpResponse(HttpVersion.HTTP_1_1, HttpStatus.SC_OK, "OK")
    httpTokenResponse.setEntity(new StringEntity(s""" {"$accessTokenField": "$accessToken"} """))
    doReturn(httpTokenResponse).when(mockClient).execute(argThat(postMatcher))

    val mgmtClient = new MgmtClient(baseUrl, clientId, clientSecret, audience, oauthServiceBaseUrl, mockClient)
    mgmtClient.processingComplete(tenantId, batchId, 15, 10) match {
      case Success(_) => fail("expected Exception but got success")
      case Failure(_) => //expected failure
    }

  }

  test("it should throw RequestException when request for access token fails in processingComplete") {
    val mockClient = mock[CloseableHttpClient]

    val postMatcher = new RequestMatcherPost(
      s"$oauthServiceBaseUrl/token"
    )

    val httpTokenResponse = new FakeHttpResponse(HttpVersion.HTTP_1_1, HttpStatus.SC_BAD_REQUEST, "BAD REQUEST")
    httpTokenResponse.setEntity(new StringEntity("{\"errorEventId\":\"adf01715c1d24292b01715c1d2429218\",\"errorDescrption\":\"Bad Request\"}"))
    doReturn(httpTokenResponse).when(mockClient).execute(argThat(postMatcher))

    val mgmtClient = new MgmtClient(baseUrl, clientId, clientSecret, audience, oauthServiceBaseUrl, mockClient)
    mgmtClient.processingComplete(tenantId, batchId, 15, 10) match {
      case Success(_) => fail("expected Exception but got success")
      case Failure(_) => //expected failure
    }

  }

  test("it should return without exceptions on successful fail call") {
    val mockClient = mock[CloseableHttpClient]

    val putMatcher = new RequestMatcherPut(
      s"$baseUrl/tenants/$tenantId/batches/$batchId/action/fail",
      Seq[String]("\"actualRecordCount\":10", "\"invalidRecordCount\":10", "\"failureMessage\":\"Error message\"")
    )

    val postMatcher = new RequestMatcherPost(
      s"$oauthServiceBaseUrl/token"
    )

    val httpResponse = new FakeHttpResponse(HttpVersion.HTTP_1_1, HttpStatus.SC_OK, "OK")
    httpResponse.setEntity(new StringEntity(""))
    doReturn(httpResponse).when(mockClient).execute(argThat(putMatcher))

    val httpTokenResponse = new FakeHttpResponse(HttpVersion.HTTP_1_1, HttpStatus.SC_OK, "OK")
    httpTokenResponse.setEntity(new StringEntity(s""" {"$accessTokenField": "$accessToken"} """))
    doReturn(httpTokenResponse).when(mockClient).execute(argThat(postMatcher))

    val mgmtClient = new MgmtClient(baseUrl, clientId, clientSecret, audience, oauthServiceBaseUrl, mockClient)
    mgmtClient.failBatch(tenantId, batchId, 10, 10, "Error message") match {
      case Success(_) => // success!
      case Failure(ex) => fail(ex)
    }

  }

  test("it should throw RequestException when fail responds with non 200") {
    val mockClient = mock[CloseableHttpClient]

    val putMatcher = new RequestMatcherPut(
      s"$baseUrl/tenants/$tenantId/batches/$batchId/action/fail",
      Seq[String]("\"actualRecordCount\":10", "\"invalidRecordCount\":10", "\"failureMessage\":\"Error message\"")
    )

    val postMatcher = new RequestMatcherPost(
      s"$oauthServiceBaseUrl/token"
    )

    val httpResponse = new FakeHttpResponse(HttpVersion.HTTP_1_1, HttpStatus.SC_BAD_REQUEST, "BAD REQUEST")
    httpResponse.setEntity(new StringEntity("{\"errorEventId\":\"adf01715c1d24292b01715c1d2429218\",\"errorDescrption\":\"Bad Request\"}"))
    doReturn(httpResponse).when(mockClient).execute(argThat(putMatcher))

    val httpTokenResponse = new FakeHttpResponse(HttpVersion.HTTP_1_1, HttpStatus.SC_OK, "OK")
    httpTokenResponse.setEntity(new StringEntity(s""" {"$accessTokenField": "$accessToken"} """))
    doReturn(httpTokenResponse).when(mockClient).execute(argThat(postMatcher))

    val mgmtClient = new MgmtClient(baseUrl, clientId, clientSecret, audience, oauthServiceBaseUrl, mockClient)
    mgmtClient.failBatch(tenantId, batchId, 10, 10, "Error message") match {
      case Success(_) => fail("expected Exception but got success")
      case Failure(_) => //expected failure
    }
  }

  test("it should throw RequestException when request for access token fails in failBatch") {
      val mockClient = mock[CloseableHttpClient]

      val postMatcher = new RequestMatcherPost(
        s"$oauthServiceBaseUrl/token"
      )

      val httpTokenResponse = new FakeHttpResponse(HttpVersion.HTTP_1_1, HttpStatus.SC_BAD_REQUEST, "BAD REQUEST")
      httpTokenResponse.setEntity(new StringEntity("{\"errorEventId\":\"adf01715c1d24292b01715c1d2429218\",\"errorDescrption\":\"Bad Request\"}"))
      doReturn(httpTokenResponse).when(mockClient).execute(argThat(postMatcher))

      val mgmtClient = new MgmtClient(baseUrl, clientId, clientSecret, audience, oauthServiceBaseUrl, mockClient)
      mgmtClient.failBatch(tenantId, batchId, 10, 10, "Error message") match {
        case Success(_) => fail("expected Exception but got success")
        case Failure(_) => //expected failure
      }
  }

}

private class FakeHttpResponse(ver: ProtocolVersion, code: Integer, reason: String) extends BasicHttpResponse(ver, code, reason) with CloseableHttpResponse {

  def this() {
    this(null, null, null)
  }

  override def close(): Unit = {}
}
