/*
 * Copyright 2026 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.servlet.response;

import com.google.gson.JsonParser;
import com.linkedin.cruisecontrol.http.CruiseControlRequestContext;
import com.linkedin.kafka.cruisecontrol.servlet.ServletRequestContext;
import com.linkedin.kafka.cruisecontrol.vertx.VertxRequestContext;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.ext.web.RoutingContext;
import jakarta.servlet.ServletOutputStream;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import java.nio.charset.StandardCharsets;
import org.easymock.Capture;
import org.easymock.EasyMock;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class ResponseUtilsTest {
  private static final String MARKUP = "<script>alert(1)</script>";

  @Test
  public void testErrorResponsesRetainMessagesAndStackTraces() throws Exception {
    for (boolean json : new boolean[]{false, true}) {
      CruiseControlRequestContext request = EasyMock.mock(CruiseControlRequestContext.class);
      Capture<String> body = EasyMock.newCapture();
      request.writeResponseToOutputStream(EasyMock.eq(400), EasyMock.eq(json), EasyMock.eq(true), EasyMock.capture(body));
      EasyMock.replay(request);
      Exception exception = new IllegalArgumentException("Invalid topic pattern", new IllegalStateException("Nested diagnostic"));
      ResponseUtils.writeErrorResponse(request, exception, "Invalid topic pattern", 400, json, true);
      if (json) {
        com.google.gson.JsonObject error = JsonParser.parseString(body.getValue()).getAsJsonObject();
        assertEquals("Invalid topic pattern", error.get("errorMessage").getAsString());
        assertTrue(error.get("stackTrace").getAsString().contains("java.lang.IllegalArgumentException: Invalid topic pattern"));
        assertTrue(error.get("stackTrace").getAsString().contains("Caused by: java.lang.IllegalStateException: Nested diagnostic"));
        assertEquals(1, error.get("version").getAsInt());
      } else {
        assertEquals("Invalid topic pattern", body.getValue());
      }
      EasyMock.verify(request);
    }
  }

  @Test
  public void testServletResponsesPreventHtmlSniffing() throws Exception {
    for (boolean json : new boolean[]{false, true}) {
      HttpServletRequest request = EasyMock.mock(HttpServletRequest.class);
      HttpServletResponse response = EasyMock.mock(HttpServletResponse.class);
      ServletOutputStream output = EasyMock.mock(ServletOutputStream.class);
      EasyMock.expect(request.getSession()).andReturn(null);
      EasyMock.expect(response.getOutputStream()).andReturn(output);
      response.setStatus(404);
      response.setContentType(json ? "application/json" : "text/plain");
      response.setCharacterEncoding("UTF-8");
      response.setHeader("X-Content-Type-Options", "nosniff");
      response.addHeader(EasyMock.eq("Cruise-Control-Version"), EasyMock.anyString());
      response.addHeader(EasyMock.eq("Cruise-Control-Commit_Id"), EasyMock.anyString());
      Capture<String> schema = EasyMock.newCapture();
      if (json) {
        response.addHeader(EasyMock.eq("Cruise-Control-JSON-Schema"), EasyMock.capture(schema));
      }
      String message = json ? "{\"errorMessage\":\"" + MARKUP + "\"}" : MARKUP;
      response.setContentLength(message.length());
      Capture<byte[]> body = EasyMock.newCapture();
      output.write(EasyMock.capture(body));
      output.flush();
      EasyMock.replay(request, response, output);
      new ServletRequestContext(request, response, null).writeResponseToOutputStream(404, json, true, message);
      assertEquals(message, new String(body.getValue(), StandardCharsets.UTF_8));
      if (json) {
        assertEquals("string", JsonParser.parseString(schema.getValue()).getAsJsonObject()
            .getAsJsonObject("properties").getAsJsonObject("errorMessage").get("type").getAsString());
      }
      EasyMock.verify(request, response, output);
    }
  }

  @Test
  public void testVertxResponsesPreventHtmlSniffing() {
    for (boolean json : new boolean[]{false, true}) {
      RoutingContext routingContext = EasyMock.mock(RoutingContext.class);
      HttpServerResponse response = EasyMock.mock(HttpServerResponse.class);
      EasyMock.expect(routingContext.session()).andReturn(null);
      EasyMock.expect(routingContext.response()).andStubReturn(response);
      EasyMock.expect(response.setStatusCode(404)).andReturn(response);
      EasyMock.expect(response.putHeader("Content-Type", json ? "application/json; charset=UTF-8" : "text/plain; charset=UTF-8"))
          .andReturn(response);
      EasyMock.expect(response.putHeader("X-Content-Type-Options", "nosniff")).andReturn(response);
      EasyMock.expect(response.putHeader(EasyMock.eq("Cruise-Control-Version"), EasyMock.anyString())).andReturn(response);
      EasyMock.expect(response.putHeader(EasyMock.eq("Cruise-Control-Commit_Id"), EasyMock.anyString())).andReturn(response);
      Capture<String> schema = EasyMock.newCapture();
      if (json) {
        EasyMock.expect(response.putHeader(EasyMock.eq("Cruise-Control-JSON-Schema"), EasyMock.capture(schema))).andReturn(response);
      }
      String message = json ? "{\"errorMessage\":\"" + MARKUP + "\"}" : MARKUP;
      Capture<String> body = EasyMock.newCapture();
      EasyMock.expect(response.end(EasyMock.capture(body))).andReturn(null);
      EasyMock.replay(routingContext, response);
      new VertxRequestContext(routingContext, null).writeResponseToOutputStream(404, json, true, message);
      assertEquals(message, body.getValue());
      if (json) {
        assertEquals("string", JsonParser.parseString(schema.getValue()).getAsJsonObject()
            .getAsJsonObject("properties").getAsJsonObject("errorMessage").get("type").getAsString());
      }
      EasyMock.verify(routingContext, response);
    }
  }
}
