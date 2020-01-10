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
package org.apache.solr.cloud.autoscaling;

import javax.servlet.ServletException;
import javax.servlet.ServletInputStream;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.cloud.CloudTestUtils.AutoScalingRequest;
import org.apache.solr.cloud.SolrCloudTestCase;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.util.LogLevel;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.handler.AbstractHandler;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 *
 */
@LogLevel("org.apache.solr.cloud.autoscaling=DEBUG")
@SolrTestCaseJ4.SuppressSSL
public class HttpTriggerListenerTest extends SolrCloudTestCase {

  private static CountDownLatch triggerFiredLatch;

  private MockService mockService;

  @BeforeClass
  public static void setupCluster() throws Exception {
    configureCluster(2)
        .addConfig("conf", configset("cloud-minimal"))
        .configure();
  }

  @Before
  public void setupTest() throws Exception {
    mockService = new MockService();
    mockService.start();
    triggerFiredLatch = new CountDownLatch(1);
  }

  @After
  public void teardownTest() throws Exception {
    if (mockService != null) {
      mockService.close();
    }
  }

  @Test
  public void testHttpListenerIntegration() throws Exception {
    CloudSolrClient solrClient = cluster.getSolrClient();
    String setTriggerCommand = "{" +
        "'set-trigger' : {" +
        "'name' : 'node_added_trigger'," +
        "'event' : 'nodeAdded'," +
        "'waitFor' : '0s'," +
        "'enabled' : true," +
        "'actions' : [" +
        "{'name':'test','class':'" + TestDummyAction.class.getName() + "'}" +
        "]" +
        "}}";
    SolrRequest req = AutoScalingRequest.create(SolrRequest.METHOD.POST, setTriggerCommand);
    NamedList<Object> response = solrClient.request(req);
    assertEquals(response.get("result").toString(), "success");

    String setListenerCommand = "{" +
        "'set-listener' : " +
        "{" +
        "'name' : 'foo'," +
        "'trigger' : 'node_added_trigger'," +
        "'stage' : ['STARTED','ABORTED','SUCCEEDED', 'FAILED']," +
        "'beforeAction' : 'test'," +
        "'afterAction' : ['test']," +
        "'class' : '" + HttpTriggerListener.class.getName() + "'," +
        "'url' : '" + mockService.server.getURI().toString() + "/${config.name:invalid}/${config.properties.beforeAction:invalid}/${stage}'," +
        "'payload': 'actionName=${actionName}, source=${event.source}, type=${event.eventType}'," +
        "'header.X-Foo' : '${config.name:invalid}'" +
        "}" +
        "}";
    req = AutoScalingRequest.create(SolrRequest.METHOD.POST, setListenerCommand);
    response = solrClient.request(req);
    assertEquals(response.get("result").toString(), "success");

    assertEquals(mockService.requests.toString(), 0, mockService.requests.size());

    cluster.startJettySolrRunner();
    cluster.waitForAllNodes(30);
    boolean await = triggerFiredLatch.await(20, TimeUnit.SECONDS);
    assertTrue("The trigger did not fire at all", await);

    Thread.sleep(5000);

    assertEquals(mockService.requests.toString(), 4, mockService.requests.size());
    mockService.requests.forEach(s -> assertTrue(s.contains("Content-Type: application/json")));
    mockService.requests.forEach(s -> assertTrue(s.contains("X-Foo: foo")));
    mockService.requests.forEach(s -> assertTrue(s.contains("source=node_added_trigger")));
    mockService.requests.forEach(s -> assertTrue(s.contains("type=NODEADDED")));

    String request = mockService.requests.get(0);
    assertTrue(request, request.startsWith("/foo/test/STARTED"));
    assertTrue(request, request.contains("actionName=,")); // empty actionName

    request = mockService.requests.get(1);
    assertTrue(request, request.startsWith("/foo/test/BEFORE_ACTION"));
    assertTrue(request, request.contains("actionName=test,")); // actionName

    request = mockService.requests.get(2);
    assertTrue(request, request.startsWith("/foo/test/AFTER_ACTION"));
    assertTrue(request, request.contains("actionName=test,")); // actionName

    request = mockService.requests.get(3);
    assertTrue(request, request.startsWith("/foo/test/SUCCEEDED"));
    assertTrue(request, request.contains("actionName=,")); // empty actionName
  }

  public static class TestDummyAction extends TriggerActionBase {

    @Override
    public void process(TriggerEvent event, ActionContext context) {
      triggerFiredLatch.countDown();
    }
  }

  private static class MockService extends Thread {
    public final List<String> requests = new ArrayList<>();
    private Server server;
    
    public void start() {
      server = new Server(new InetSocketAddress("localhost", 0));
      server.setHandler(new AbstractHandler() {
        @Override
        public void handle(String s, Request request, HttpServletRequest httpServletRequest, HttpServletResponse httpServletResponse) throws IOException, ServletException {
          StringBuilder stringBuilder = new StringBuilder();
          stringBuilder.append(httpServletRequest.getRequestURI());
          Enumeration<String> headerNames = httpServletRequest.getHeaderNames();
          while (headerNames.hasMoreElements()) {
            stringBuilder.append('\n');
            String name = headerNames.nextElement();
            stringBuilder.append(name);
            stringBuilder.append(": ");
            stringBuilder.append(httpServletRequest.getHeader(name));
          }
          stringBuilder.append("\n\n");
          ServletInputStream is = request.getInputStream();
          byte[] httpInData = new byte[request.getContentLength()];
          int len = -1;
          while ((len = is.read(httpInData)) != -1) {
            stringBuilder.append(new String(httpInData, 0, len, StandardCharsets.UTF_8));
          }
          requests.add(stringBuilder.toString());
          httpServletResponse.setStatus(HttpServletResponse.SC_OK);
          request.setHandled(true);
        }
      });
      try {
        server.start();
        for (int i = 0; i < 30; i++) {
          Thread.sleep(1000);
          if (server.isRunning()) {
            break;
          }
          if (server.isFailed()) {
            throw new Exception("MockService startup failed - the test will fail...");
          }
        }
      } catch (Exception e) {
        throw new RuntimeException("Exception starting MockService", e);
      }
    }

    void close() throws Exception {
      if (server != null) {
        server.stop();
      }
    }
  }
}
