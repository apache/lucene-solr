/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.solr.handler.admin;

import java.io.IOException;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.SolrException;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.junit.BeforeClass;
import org.junit.Test;

public class InfoHandlerTest extends SolrTestCaseJ4 {
  
  @BeforeClass
  public static void beforeClass() throws Exception {
    initCore("solrconfig.xml", "schema.xml");
  }
  
  @Test
  public void testCoreAdminHandler() throws Exception {

    final CoreContainer cores = h.getCoreContainer();
    InfoHandler infoHandler = cores.getInfoHandler();
    SolrQueryResponse rsp = handleRequest(infoHandler, "properties");
    
    assertNotNull(rsp.getValues().get("system.properties"));

    
    rsp = handleRequest(infoHandler, "threads");
    
    assertNotNull(rsp.getValues().get("system"));
    
    rsp = handleRequest(infoHandler, "logging");
    
    assertNotNull(rsp.getValues().get("watcher"));

    SolrException e = expectThrows(SolrException.class, () -> handleRequest(infoHandler, "info"));
    assertEquals(404, e.code());

    e = expectThrows(SolrException.class, () -> handleRequest(infoHandler, ""));
    assertEquals(404, e.code());
  }

  @Test
  public void testOverriddenHandlers() throws Exception {
    final CoreContainer cores = h.getCoreContainer();
    final InfoHandler infoHandler = new InfoHandler(cores);
    infoHandler.init(null);

    CountPropertiesRequestHandler propHandler = new CountPropertiesRequestHandler();
    CountThreadDumpHandler threadHandler = new CountThreadDumpHandler();
    CountLoggingHandler loggingHandler = new CountLoggingHandler(cores);
    CountSystemInfoHandler systemInfoHandler = new CountSystemInfoHandler(cores);

    // set the request handlers
    infoHandler.setPropertiesHandler(propHandler);
    infoHandler.setThreadDumpHandler(threadHandler);
    infoHandler.setLoggingHandler(loggingHandler);
    infoHandler.setSystemInfoHandler(systemInfoHandler);

    // verify that the sets are reflected in the gets
    assertEquals(propHandler, infoHandler.getPropertiesHandler());
    assertEquals(threadHandler, infoHandler.getThreadDumpHandler());
    assertEquals(loggingHandler, infoHandler.getLoggingHandler());
    assertEquals(systemInfoHandler, infoHandler.getSystemInfoHandler());

    // call each handler and verify it was actually called
    handleRequest(infoHandler, "properties");
    handleRequest(infoHandler, "threads");
    handleRequest(infoHandler, "logging");
    handleRequest(infoHandler, "system");

    assertEquals(1, propHandler.getRequestCount());
    assertEquals(1, threadHandler.getRequestCount());
    assertEquals(1, loggingHandler.getRequestCount());
    assertEquals(1, systemInfoHandler.getRequestCount());
  }

  // derived request handlers that count the number of request body counts made
  public static class CountPropertiesRequestHandler extends PropertiesRequestHandler {
    private int requestCount = 0;

    @Override
    public void handleRequestBody(SolrQueryRequest req, SolrQueryResponse rsp)
    throws IOException {
      ++requestCount;
      super.handleRequestBody(req, rsp);
    }

    public int getRequestCount() { return requestCount; }
  }

  public static class CountThreadDumpHandler extends ThreadDumpHandler {
    private int requestCount = 0;

    @Override
    public void handleRequestBody(SolrQueryRequest req, SolrQueryResponse rsp)
    throws IOException {
      ++requestCount;
      super.handleRequestBody(req, rsp);
    }

    public int getRequestCount() { return requestCount; }
  }

  public static class CountLoggingHandler extends LoggingHandler {
    private int requestCount = 0;

    CountLoggingHandler(CoreContainer cores) { super(cores); }

    @Override
    public void handleRequestBody(SolrQueryRequest req, SolrQueryResponse rsp)
    throws Exception {
      ++requestCount;
      super.handleRequestBody(req, rsp);
    }

    public int getRequestCount() { return requestCount; }
  }

  public static class CountSystemInfoHandler extends SystemInfoHandler {
    private int requestCount = 0;

    CountSystemInfoHandler(CoreContainer cores) { super(cores); }

    @Override
    public void handleRequestBody(SolrQueryRequest req, SolrQueryResponse rsp)
    throws Exception {
      ++requestCount;
      super.handleRequestBody(req, rsp);
    }

    public int getRequestCount() { return requestCount; }
  }

  private SolrQueryResponse handleRequest(InfoHandler infoHandler, String path)
      throws Exception {
    SolrQueryResponse rsp = new SolrQueryResponse();
    SolrQueryRequest req = req();
    req.getContext().put("path", path);
    infoHandler.handleRequestBody(req, rsp);
    return rsp;
  }

}
