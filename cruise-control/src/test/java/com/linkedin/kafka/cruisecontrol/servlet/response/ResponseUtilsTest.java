/*
 * Copyright 2026 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.servlet.response;

import io.vertx.core.http.HttpServerResponse;
import io.vertx.ext.web.RoutingContext;
import jakarta.servlet.http.HttpServletResponse;
import org.easymock.EasyMock;
import org.junit.Test;

public class ResponseUtilsTest {
  @Test
  public void testServletResponsesPreventMimeSniffing() {
    for (boolean json : new boolean[]{false, true}) {
      HttpServletResponse response = EasyMock.mock(HttpServletResponse.class);
      response.setStatus(404);
      response.setContentType(json ? "application/json" : "text/plain");
      response.setCharacterEncoding("UTF-8");
      response.setHeader("X-Content-Type-Options", "nosniff");
      EasyMock.replay(response);
      ResponseUtils.setResponseCode(response, 404, json, null);
      EasyMock.verify(response);
    }
  }

  @Test
  public void testVertxResponsesPreventMimeSniffing() {
    RoutingContext context = EasyMock.mock(RoutingContext.class);
    HttpServerResponse response = EasyMock.mock(HttpServerResponse.class);
    EasyMock.expect(context.response()).andStubReturn(response);
    EasyMock.expect(response.setStatusCode(404)).andReturn(response);
    EasyMock.expect(response.putHeader("X-Content-Type-Options", "nosniff")).andReturn(response);
    EasyMock.replay(context, response);
    ResponseUtils.setResponseCode(context, 404, null);
    EasyMock.verify(context, response);
  }
}
